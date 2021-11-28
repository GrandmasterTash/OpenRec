use rlua::Context;
use itertools::Itertools;
use bytes::{BufMut, Bytes, BytesMut};
use std::{cell::Cell, time::{Duration, Instant}};
use crate::{charter::Constraint, error::MatcherError, formatted_duration_rate, grid::Grid, matched::MatchedHandler, record::Record, schema::{GridSchema}};

///
/// Bring groups of records together using the columns specified.
///
/// If a group of records matches all the constraint rules specified, the group is written to a matched
/// file and any records which fail to be matched are written to un-matched files.
///
pub fn match_groups(
    group_by: &[String],
    constraints: &[Constraint],
    grid: &mut Grid,
    lua: &rlua::Lua,
    matched: &mut MatchedHandler) -> Result<(), MatcherError> {

    log::info!("Grouping by {}", group_by.iter().join(", "));

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
                records.iter().for_each(|r| r.set_matched());
                matched.append_group(&records).map_err(|source| rlua::Error::external(source))?;
                match_count += 1;
            }

            group_count += 1;
        }

        Ok(())
    })
    .map_err(|source| MatcherError::MatchGroupError { source })?;

    // Remove matched records from the grid now.
    grid.remove_matched();

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

    lua_time.replace(lua_time.get() + start.elapsed());

    Ok(failed.is_empty())
}