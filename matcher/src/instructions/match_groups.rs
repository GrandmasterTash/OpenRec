use uuid::Uuid;
use rlua::Context;
use serde_json::json;
use itertools::Itertools;
use bytes::{BufMut, Bytes, BytesMut};
use std::{cell::Cell, fs::File, io::{BufWriter, Write}, time::{Duration, Instant}};
use crate::{charter::{Charter, Constraint, formatted_duration_rate}, error::MatcherError, folders::{self, ToCanoncialString}, grid::Grid, record::Record, schema::GridSchema};

///
/// Bring groups of records together using the columns specified.
///
/// If a group of records matches all the constraint rules specified, the group is written to a matched
/// file and any records which fail to be matched are written to un-matched files.
///
pub fn match_groups(group_by: &[String], constraints: &[Constraint], grid: &mut Grid, lua: &rlua::Lua, job_id: Uuid, charter: &Charter)
    -> Result<(), MatcherError> {

    log::info!("Grouping by {:?}", group_by);

    // Create a match file containing job details and match results.
    let mut matched_file = create_matched_file(job_id, charter, grid)?;

    let mut group_count = 0;
    let mut match_count = 0;
    let lua_time = Cell::new(Duration::from_millis(0));

    lua.context(|lua_ctx| {
        // Form groups from the records.
        for (_key, group) in &grid.records().iter()

            // Build a 'group key' from the record using the grouping columns.
            .map(|record| (match_key(record, group_by, grid.schema()), record) )

            // Sort data by the group key to form contiguous runs of records belonging to the same group.
            .sorted_by(|(key1, _record1), (key2, _record2)| Ord::cmp(&key1, &key2))

            // Group records by the group key.
            .group_by(|(key, _record)| key.clone()) {

            // Collect the records in the group.
            let records = group
                .map(|(_key, record)| record)
                .collect::<Vec<&Box<Record>>>();

            // Test any constraints on the group to see if it's a match.
            if is_match(&records, constraints, grid.schema(), &lua_ctx, &lua_time)? {
                append_group(&records, &mut matched_file, group_count).unwrap();
                match_count += 1;

            } else {
                // TODO: Write an unmatched record to base/unmatched/<filename>.inprogress
                // TODO: Only write the original columns - use ByteRecord.truncate to chop off the appended fields.
                

            }

            group_count += 1;
        }

        Ok(())
    })
    .map_err(|source| MatcherError::MatchScriptError { source })?;

    // Terminate the matched file to make it's contents valid JSON.
    write!(&mut matched_file, "]\n}}\n]\n").unwrap();

    // TODO: If match_groups.debug=true, write an output.csv file (dump grid) now - with number of instructions run in the filename (incase there's multiple match_group intructions).

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

    let accumulated = lua_time.get() + start.elapsed();
    lua_time.replace(accumulated);

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
        "files": grid.files().iter().map(|f|f.filename()).collect::<Vec<&str>>()
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

    let json = records.iter().map(|r| json!(vec!(r.row(), r.file_idx()))).collect::<Vec<serde_json::Value>>();
    serde_json::to_writer(file, &json).unwrap(); // TODO: Don't unwrap - throw external.

    Ok(())
}