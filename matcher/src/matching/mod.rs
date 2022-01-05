mod group_iter;
pub mod matched;
pub mod unmatched;

use rlua::Context;
use ubyte::ToByteUnit;
use itertools::Itertools;
use core::charter::Constraint;
use bytes::{BufMut, Bytes, BytesMut};
use std::{cell::Cell, time::{Duration, Instant}, fs::File, path::PathBuf};
use self::{prelude::*, group_iter::GroupIterator, matched::MatchedHandler};
use crate::{error::MatcherError, formatted_duration_rate, model::{grid::Grid, record::Record, schema::GridSchema}, blue, folders, convert, lua, instructions::constraints::passes, utils::{self, CsvWriter}};

// The column position in our index records for the merge_key used to sort index records.
mod prelude {
    pub const COL_FILE_IDX: usize = 0;
    pub const COL_DATA_BYTE: usize = 1;
    pub const COL_DATA_LINE: usize = 2;
    pub const COL_DERIVED_BYTE: usize = 3;
    pub const COL_DERIVED_LINE: usize = 4;
    pub const COL_MERGE_KEY: usize = 5;
}

///
/// Derive a value ('match key') to group this record with others.
///
fn match_key(record: &Record, headers: &[String]) -> Bytes {
    let mut buf = BytesMut::new();
    for header in headers {
        if let Some(bytes) = record.get_as_bytes(header).expect("Failed to read match ley") {
            buf.put(bytes);
        }
    }
    buf.freeze()
}

///
/// Evaluate the constraint rules against the grroup to see if they all pass.
///
fn is_match(
    group: &[&Record],
    constraints: &[Constraint],
    schema: &GridSchema,
    lua_ctx: &Context,
    lua_time: &Cell<Duration>) -> Result<bool, MatcherError> {

    let mut failed = vec!();
    let start = Instant::now();

    for (_index, constraint) in constraints.iter().enumerate() {
        if !passes(constraint, group, schema, lua_ctx)? {
            failed.push(constraint);
        }
    }

    lua_time.replace(lua_time.get() + start.elapsed());

    Ok(failed.is_empty())
}

///
/// Matching brings together sets of records and if they pass the constraint rules defined, are considered a matched
/// group. If they don't pass the constraints, they are considered unmatched data.
///
/// The matched and unmatched handlers will deal with the results of this process.
///
/// The strategy for matching data resolves around grouping the records by one or more columns of data. To avoid resource
/// starvation for large datasets, this is achieved with an external merge sort procedure.
///
/// Data is loaded into a memory buffer and sorted. The sorted chunk is written to disk and another chunk loaded and
/// sorted. This is repeated until all data has been sorted into chunks. Next, a merge sort reads from each chunked
/// file and writes to a single output file.
///
/// Once all data is sorted by the grouping column(s) (also refered to as sort-key and/or merge-key), then it will
/// form contiguous runs of records belonging to the same potential groups. These groups are loaded into memory, one
/// at a time, and the constraint rules are evaludated to determine if the group matches or not.
///
/// Note: Not all the record data is written to the sorted index files, the format looks something along these lines: -
///
///   "<file_idx>","<data-byte-pos>","<data-line-pos>","<derived-byte-pos>","<derived-line-pos>","<merge_key>"\n
///
/// This row is an index pointer to the real csv data and the derived csv data rows for the record. Note: both byte
/// and line positions are required by the csv library to seek a row.
///
pub fn match_groups(
    ctx: &crate::Context,
    group_by: &[String],
    constraints: &[Constraint],
    grid: &Grid,
    matched: &mut MatchedHandler) -> Result<(), MatcherError> {

    // TODO: Remove all unwraps in these fn's.

    if grid.is_empty() {
        return Ok(())
    }

    log::info!("Grouping by {}", group_by.iter().join(", "));

    let lua_time = Cell::new(Duration::from_millis(0));

    // TODO: Shortcut if all data loads into initial buffer.
    // Build index.unsorted.csv. and calculate the approximate length of each index row.
    let (unsorted_path, avg_len) = create_unsorted(ctx, group_by, grid)?;

    // Use a buffer to sort chunks of data and write each sorted chunk to it's own file.
    let file_count = split_and_sort(ctx, &unsorted_path, avg_len)?;

    // Initialise input and output readers/writers - prior to merge sorting.
    let (inputs, output) = initialise_buffers(ctx, file_count);

    // Merge-sort all the chunks into a single index.sorted.csv file.
    merge_sort(inputs, output);

    // Match groups which pass the constriant rules.
    let (group_count, match_count) = eval_contraints(ctx, grid, constraints, matched, &lua_time)?;

    // Delete all index files, index.unsorted.csv, index.sorted.*
    clean_up_indexes(ctx, file_count)?;

    // TODO: grid.len() should be updated to reflect removed records. MAybe in match handler?
    // TODO Debug grid....

    let (duration, rate) = formatted_duration_rate(group_count, lua_time.get());
    log::info!("Matched {} out of {} groups. Constraints took {} ({}/group)",
        blue(&format!("{}", match_count)),
        blue(&format!("{}", group_count)),
        blue(&duration),
        rate);

    Ok(())
}

///
/// Estimate the size of each record index row.
///
fn estimated_index_size(unsorted_path: &PathBuf, grid: &Grid) -> usize {
    let f = File::open(&unsorted_path).unwrap();
    let f_len = f.metadata().unwrap().len();
    let mut avg_len = (f_len as f64 / grid.len() as f64) as usize;   // Average data length.
    avg_len += std::mem::size_of::<csv::ByteRecord>();               // Struct 8B.
    // TODO: Count fields in bounds....
    avg_len += std::mem::size_of::<usize>();                         // Pointer to struct 8B.
    avg_len += std::mem::size_of::<usize>() * 6;                     // 4 fields, 4 pointers (in the bounds sub-struct)
    avg_len
}

///
/// Calculate how many index records form a batch that will fit in the memory bounds.
///
fn batch_size(avg_len: usize, ctx: &crate::Context) -> usize {
    (ctx.charter().memory_limit() as f64 / avg_len as f64) as usize
}

///
/// Create a file index for every record in the grid, along with the merge-key we'll use to sort the records.
///
fn create_unsorted(ctx: &crate::Context, group_by: &[String], grid: &Grid)
    -> Result<(PathBuf, usize), MatcherError> {

    let unsorted_path = folders::matching(ctx).join("index.unsorted.csv");
    let mut unsorted_writer = utils::writer(&unsorted_path);
    let mut buffer = csv::ByteRecord::new();

    let start = Instant::now();

    // Build an index row for each sourced data record. Record the match-key for each indexes record.
    for record in grid.iter(ctx) {
        buffer.push_field(convert::int_to_string(record.file_idx() as i64).as_bytes());
        buffer.push_field(convert::int_to_string(record.data_position().byte() as i64).as_bytes());
        buffer.push_field(convert::int_to_string(record.data_position().line() as i64).as_bytes());
        buffer.push_field(convert::int_to_string(record.derived_position().byte() as i64).as_bytes());
        buffer.push_field(convert::int_to_string(record.derived_position().line() as i64).as_bytes());
        buffer.push_field(&match_key(&record, group_by));
        unsorted_writer.write_byte_record(&buffer)?;
        buffer.clear();
    }

    unsorted_writer.flush()?;

    // TODO: If avg_len is only used in split_and_sort - do this in THAT fn instead.
    // Calculate the average index row length.
    let f = File::open(&unsorted_path).unwrap();
    let f_len = f.metadata().unwrap().len();
    let avg_len = estimated_index_size(&unsorted_path, grid);

    let (duration, _rate) = formatted_duration_rate(grid.len(), start.elapsed());
    log::debug!("Created {path}, {size}, avergage index length {avg_len}, took {duration}",
        path = unsorted_path.file_name().expect("no filename").to_string_lossy(),
        size = f_len.bytes(),
        avg_len = avg_len.bytes(),
        duration = blue(&duration));

    Ok((unsorted_path, avg_len))
}

///
/// Load unsorted indexes into the memory buffer and sort them. Then writer the buffer to a sorted
/// file - repeat until all unsorted indexes have been sorted into a file.
///
fn split_and_sort(ctx: &crate::Context, unsorted_path: &PathBuf, avg_len: usize) -> Result<usize, MatcherError> {

    let batch_size = batch_size(avg_len, ctx);
    let mut file_count = 0; // Number of split files containing the chunked, sorted data.
    let mut reader = utils::index_reader(unsorted_path);
    let mut buffer: Vec<csv::ByteRecord> = Vec::with_capacity(batch_size);

    for result in reader.byte_records() {
        let record = result.unwrap();
        buffer.push(record);

        if buffer.len() == batch_size {
            // Sort by merge key.
            buffer.sort_unstable_by(|r1, r2| r1.get(COL_MERGE_KEY).unwrap().cmp(r2.get(COL_MERGE_KEY).unwrap()) );

            // Increment the count of split sorted files.
            file_count += 1;

            // Write the sorted data to a new split file.
            let mut writer = sorted_writer(ctx, file_count);
            buffer.iter().for_each(|record| writer.write_byte_record(&record).unwrap());

            // Clear the buffer.
            buffer.clear();
        }
    }

    // Sort and write the last batch.
    if !buffer.is_empty() {
        // Sort by merge key.
        buffer.sort_unstable_by(|r1, r2| r1.get(COL_MERGE_KEY).unwrap().cmp(r2.get(COL_MERGE_KEY).unwrap()) );

        // Increment the count of split sorted files.
        file_count += 1;

        // Write the sorted data to a new split file.
        let mut writer = sorted_writer(ctx, file_count);
        buffer.iter().for_each(|record| writer.write_byte_record(&record).unwrap());
    }

    // println!("Memory Bounds {bounds}\nBatch size was {batch}\ncsv::ByteRecord {csv}\ntotal data {data}",
    //     bounds = MEMORY_BOUNDS.bytes(),
    //     batch = batch_size,
    //     csv = std::mem::size_of::<csv::ByteRecord>().bytes(),
    //     data = total_data_size.bytes());

    Ok(file_count)
}

fn sorted_writer(ctx: &crate::Context, file_idx: usize) -> CsvWriter {
    let sorted_path = folders::matching(ctx).join(format!("index.sorted.{}", file_idx));
    utils::writer(&sorted_path)
}


///
/// Initialise out merge sort buffers for reading in files and writing out a sorted file.
///
fn initialise_buffers(ctx: &crate::Context, file_count: usize) -> (Vec<csv::Reader<File>>, csv::Writer<File>) {
    let output = utils::writer(folders::matching(ctx).join("index.sorted.csv"));

    let inputs = (1..=file_count)
        .map(|idx| utils::index_reader(folders::matching(ctx).join(format!("index.sorted.{}", idx))))
        .collect();

    (inputs, output)
}

///
/// Merge-sort all the sorted index.sorted.nnn files into a single index.sorted.csv file.
///
fn merge_sort(mut inputs: Vec<csv::Reader<File>>, mut output: csv::Writer<File>) {

    let mut registers = Vec::with_capacity(inputs.len());
    for idx in 0..inputs.len() {
        registers.push(inputs[idx].next());
    };

    // Loop until all our registers are empty.
    while registers.iter().any(|r| r.is_some()) {
        // K-way sort on the registers.
        let idx = kway_sort(&registers);

        // Write the next record/index to the output file.
        output.write_byte_record(registers[idx].as_ref().unwrap()).unwrap();

        // Increment the register we just sorted.
        registers[idx] = inputs[idx].next();
    }
}

///
/// Return the slice index of the next record in sort order.
///
/// Because the len of registers will be low - we will iterate elements.
///
fn kway_sort(registers: &[Option<csv::ByteRecord>]) -> usize {
    let mut result = 0;
    let mut current = None;

    for idx in 0..registers.len() {
        match &registers[idx] {
            Some(record) => {
                match current {
                    None => {
                        result = idx;
                        current = record.get(COL_MERGE_KEY);
                    },
                    Some(cur) => {
                        if record.get(COL_MERGE_KEY).unwrap() < cur {
                            result = idx;
                            current = record.get(COL_MERGE_KEY);
                        }
                    },
                }
            },
            None => {},
        }
    }

    result
}

///
/// Iterate all of the sorted indexes as groups and evaluate the Lua constraint rules against each group.
/// If the group is a match, pass it to the match handler.
///
fn eval_contraints(
    ctx: &crate::Context,
    grid: &Grid,
    constraints: &[Constraint],
    matched: &mut MatchedHandler,
    lua_time: &Cell<Duration>) -> Result<(usize, usize), MatcherError> {

    let mut group_count = 0;
    let mut match_count = 0;

    log::info!("Evaluating constraints on groups");

    // TODO: Enforce a group size limit.
    // Create a Lua context to evaluate Constraint rules in.
    ctx.lua().context(|lua_ctx| {
        lua::init_context(&lua_ctx)?;
        lua::create_aggregate_fns(&lua_ctx)?;

        // Iterate groups one at a time, loading all the group's records into memory.
        for group in GroupIterator::new(ctx, grid.schema()) {
            let group = group?;
            group_count += 1;

            let records: Vec<&Record> = group.iter().map(|r|r).collect();

            if is_match(&records, constraints, grid.schema(), &lua_ctx, &lua_time)? {
                matched.append_group(&records)?;
                match_count += 1;

            } else if group_count <= 0 /* TODO: Have a ENV to set this limit - default to 0 */{
                log::info!("Unmatched group:-\n{:?}{}",
                    grid.schema().headers(),
                    records.iter().map(|r| format!("\n{:?}", r.as_strings())).collect::<String>());
            }
        }

        Ok(())
    })
    .map_err(|source| MatcherError::MatchGroupError { source })?;

    Ok((group_count, match_count))
}

///
/// Remove sorted and unsorted index files.
///
fn clean_up_indexes(ctx: &crate::Context, file_count: usize) -> Result<(), MatcherError> {
    std::fs::remove_file(folders::matching(ctx).join("index.unsorted.csv"))?;
    std::fs::remove_file(folders::matching(ctx).join("index.sorted.csv"))?;
    for idx in 1..=file_count {
        std::fs::remove_file(folders::matching(ctx).join(format!("index.sorted.{}", idx)))?;
    }
    Ok(())
}


///
/// Reads csv records with a slightly slicker api.
///
trait RecordProvider {
    fn next(&mut self) -> Option<csv::ByteRecord>;
}

impl RecordProvider for csv::Reader<File> {
    fn next(&mut self) -> Option<csv::ByteRecord> {
        let mut record = csv::ByteRecord::new();
        match self.read_byte_record(&mut record) {
            Ok(read) => {
                if read {
                    Some(record)
                } else {
                    None
                }
            },
            Err(err) => {
                log::error!("Unable to read : {}", err);
                None
            },
        }
    }
}
