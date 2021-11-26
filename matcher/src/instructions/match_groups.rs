use uuid::Uuid;
use csv::Writer;
use rlua::Context;
use serde_json::json;
use itertools::Itertools;
use bytes::{BufMut, Bytes, BytesMut};
use std::{cell::Cell, collections::HashMap, fs::File, io::{BufWriter, Write}, time::{Duration, Instant}};
use crate::{charter::{Charter, Constraint, formatted_duration_rate}, error::MatcherError, folders::{self, ToCanoncialString}, grid::Grid, record::Record, schema::{FileSchema, GridSchema}};

///
/// Bring groups of records together using the columns specified.
///
/// If a group of records matches all the constraint rules specified, the group is written to a matched
/// file and any records which fail to be matched are written to un-matched files.
///
pub fn match_groups(group_by: &[String], constraints: &[Constraint], grid: &mut Grid, lua: &rlua::Lua, job_id: Uuid, charter: &Charter)
    -> Result<(), MatcherError> {

    log::info!("Grouping by {:?}", group_by);

    // Create a match file containing job details and giving us a place to append match results.
    let mut matched_file = create_matched_file(job_id, charter, grid)?; // TODO: Consider a struct tracker for consistency.

    // Create unmatched files for each sourced file.
    let mut unmatched = UnmatchedHandler::new(grid)?;

    let mut group_count = 0;
    let mut match_count = 0;
    let lua_time = Cell::new(Duration::from_millis(0));

    // Create a Lua context to evaluate Constraint rules in.
    lua.context(|lua_ctx| {
        // Form groups from the records.
        for (_key, group) in &grid.records().iter()

            // Build a 'group key' from the record using the grouping columns.
            .map(|record| (match_key(record, group_by, grid.schema()), record) )

            // Sort records by the group key to form contiguous runs of records belonging to the same group.
            .sorted_by(|(key1, _record1), (key2, _record2)| Ord::cmp(&key1, &key2))

            // Group records by the group key.
            .group_by(|(key, _record)| key.clone()) {

            // Collect the records in the group.
            let records = group.map(|(_key, record)| record).collect::<Vec<&Box<Record>>>();

            // Test any constraints on the group to see if it's a match.
            if is_match(&records, constraints, grid.schema(), &lua_ctx, &lua_time)? {
                append_group(&records, &mut matched_file, group_count).unwrap();
                match_count += 1; // TODO: Move to handler.

            } else {
                unmatched.append_group(&records, &grid).unwrap(); // TODO: don't unwrap.
            }

            // TODO: If this instruction is not the last one, don't close the job file and dont write unmatched records.

            group_count += 1;
        }

        Ok(())
    })
    .map_err(|source| MatcherError::MatchScriptError { source })?; // TODO: Maybe rename to MatchGroupError?

    // Terminate the matched file to make it's contents valid JSON.
    write!(&mut matched_file, "]\n}}\n]\n").unwrap(); // TODO: Completing a job should also log the unmatched files and counts).
    unmatched.complete_files();

    if charter.debug() {
        debug_grid(&grid);
    }

    let (duration, rate) = formatted_duration_rate(group_count, lua_time.get());
    log::info!("Matched {} out of {} groups. Constraints took {} ({}/group)",
        match_count,
        group_count,
        duration,
        ansi_term::Colour::RGB(70, 130, 180).paint(rate));

    Ok(())
}

///
/// Derive a value ('match key') to group this record with others.
///
fn match_key(record: &Box<Record>, headers: &[String], schema: &GridSchema) -> Bytes {
    let mut buf = BytesMut::new();
    for header in headers {
        if let Some(bytes) = record.get_bytes_copy(header, schema) {
            buf.put(bytes.as_slice());
        }
    }
    buf.freeze()
}

///
/// Evaluate the constraint rules against the grroup to see if they all pass.
///
fn is_match(group: &[&Box<Record>], constraints: &[Constraint], schema: &GridSchema, lua_ctx: &Context, lua_time: &Cell<Duration>)
    -> Result<bool, rlua::Error> {

    let mut failed = vec!();
    let start = Instant::now();

    for constraint in constraints {
        if !constraint.passes(&group, schema, lua_ctx)? {
            failed.push(constraint);
        }
    }

    // let accumulated = lua_time.get() + start.elapsed();
    lua_time.replace(lua_time.get() + start.elapsed());

    Ok(failed.is_empty())
}

///
/// Open a matched output file to write Json groups to. We'll add job details to the top of the file.
///
fn create_matched_file(job_id: Uuid, charter: &Charter, grid: &Grid) -> Result<BufWriter<File>, MatcherError> {

    let path = folders::new_matched_file();
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);

    write!(&mut writer, "[\n")?;

    let job_header = json!(
    {
        "job_id": job_id.to_hyphenated().to_string(),
        "charter_name": charter.name(),
        "charter_version": charter.version(),
        "files": grid.files().iter().map(|f|f.original_filename()).collect::<Vec<&str>>()
    });

    if let Err(source) = serde_json::to_writer_pretty(&mut writer, &job_header) {
        return Err(MatcherError::FailedToWriteJobHeader { job_header: job_header.to_string(), path: path.to_canoncial_string(), source })
    }

    write!(&mut writer, ",\n{{\n  \"groups\": [\n    ")?;

    Ok(writer)
}

///
/// Append the records in this group to the matched group file.
///
/// Each group entry in the file is a 'file coordinate' to the original data. This is in the form: -
/// [[n1,y1], [n2,y2], [n2,y3]]
///
/// When n is a file index in the grid and y is the line number in the file for the record. Line numbers include
/// the header rows (so the first line of data will start at 3).
///
fn append_group(records: &[&Box<Record>], file: &mut BufWriter<File>, group_count: usize) -> Result<(), MatcherError> {
    // Push this file writing into an fn.
    if group_count !=  0 {
        write!(file, ",\n    ").unwrap();
    }

    let json = records.iter().map(|r| json!(vec!(r.file_idx(), r.row()))).collect::<Vec<serde_json::Value>>();
    serde_json::to_writer(file, &json).unwrap(); // TODO: Don't unwrap - throw external.

    Ok(())
}

///
/// Writes all the grid's data to a file at this point
///
fn debug_grid(grid: &Grid) {
    let output_path = folders::debug_path().join(format!("{}output.csv", folders::new_timestamp()));
    log::info!("Creating grid debug file {}...", output_path.to_canoncial_string());

    let mut wtr = csv::WriterBuilder::new().quote_style(csv::QuoteStyle::Always).from_path(&output_path).unwrap();
    wtr.write_record(grid.schema().headers()).unwrap();

    for record in grid.records() {
        let data: Vec<&[u8]> = grid.record_data(record)
            .iter()
            .map(|v| v.unwrap_or(b""))
            .collect();
        wtr.write_byte_record(&data.into()).unwrap();
    }

    wtr.flush().unwrap();
    log::info!("...{} rows written to {}", grid.records().len(), output_path.to_canoncial_string());
}


///
/// Represents an unmatched file potentially being written to as part of the current job.
///
struct UnmatchedFile {
    rows: usize,
    full_filename: String, // 20211126_072400000_invoices.unmatched.csv.
    schema: FileSchema,    // A copy of the original fileschema.
    writer: Writer<File>,
}

///
/// Manages the unmatched files for the current job.
///
struct UnmatchedHandler {
    files: HashMap<String /* 20211126_072400000_invoices.csv. */, UnmatchedFile>,
}

impl UnmatchedHandler {
    ///
    /// Creating a handler will create an unmatched file for each data file loaded into the grid.
    /// The unmatched files will be ready to have unmatched data appended to them. At the end of the job,
    /// if there are any files that didn't have data appended, they are deleted.
    ///
    pub fn new(grid: &Grid) -> Result<Self, MatcherError> {
        let mut files: HashMap<String, UnmatchedFile> = HashMap::new();

        // Create an unmatched file for each original sourced data file (i.e. there may be )
        for file in grid.files() {
            // We're using original_filename here to ensure files like 'x.unmatched.csv' don't create
            // files 'x.unmatched.unmatched.csv'.
            if !files.contains_key(file.original_filename()) {
                // Create an new unmatched file.
                let output_path = folders::new_unmatched_file(file); // $REC_HOME/unmatched/timestamp_invoices.unmatched.csv
                let full_filename = folders::filename(&output_path)?; // timestamp_invoices.unmatched.csv

                let mut writer = csv::WriterBuilder::new().quote_style(csv::QuoteStyle::Always).from_path(&output_path)
                    .map_err(|source| MatcherError::CannotCreateUnmatchedFile{ path: output_path.to_canoncial_string(), source })?;

                // Add the column header and schema rows.
                let schema = grid.schema().file_schemas().get(file.schema())
                    .ok_or(MatcherError::MissingSchemaInGrid{ filename: file.filename().into(), index: file.schema()  })?;

                writer.write_record(schema.columns().iter().map(|c|c.header()).collect::<Vec<&str>>())
                    .map_err(|source| MatcherError::CannotWriteHeaders{ filename: file.filename().into(), source })?;

                writer.write_record(schema.columns().iter().map(|c|c.data_type().to_str()).collect::<Vec<&str>>())
                    .map_err(|source| MatcherError::CannotWriteSchema{ filename: file.filename().into(), source })?;

                files.insert(file.original_filename().into(), UnmatchedFile{ full_filename, rows: 0, writer, schema: schema.clone() });

                log::trace!("Created file {}", output_path.to_canoncial_string());
            }
        }

        Ok(Self { files })
    }

    pub fn append_group(&mut self, records: &[&Box<Record>], grid: &Grid) -> Result<(), MatcherError> {
        for record in records {
            // Get the unmatched-file for this record.
            println!("R FI: {}, G F len: {}", record.file_idx(), grid.files().len());
            let filename = grid.files().get(record.file_idx()).unwrap().filename(); // TODO: Dont unwrap.
            let mut unmatched = self.files.get_mut(filename)
                .ok_or(MatcherError::UnmatchedFileNotInHandler { filename: filename.to_string() })?;

            // Track how many records are written to each unmatched file.
            unmatched.rows += 1;

            // Copy the record and truncate any projected or merged fields from it so we only write the
            // original record to disc.
            let mut copy = record.inner().clone();
            copy.truncate(unmatched.schema.columns().len());
            unmatched.writer.write_byte_record(&copy)
                .map_err(|source| MatcherError::CannotWriteUnmatchedRecord { filename: unmatched.full_filename.clone(), row: record.row(), source })?;
        }

        Ok(())
    }

    pub fn complete_files(&mut self) /* -> Result<(), MatcherError> */ {
        // Delete any unmatched files we didn't write records to.
        for (_filename, unmatched) in self.files.iter() {
            if unmatched.rows == 0 {
                folders::delete_empty_unmatched(&unmatched.full_filename).unwrap(); // TODO: Don't unwrap
            }
        }
        // Rename any remaining .inprogress files.
        // info log the creation of these files.
    }
}
