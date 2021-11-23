use rlua::Context;
use uuid::Uuid;
use itertools::Itertools;
use bytes::{BufMut, Bytes, BytesMut};
use crate::{charter::Constraint, error::MatcherError, grid::Grid, record::Record, schema::GridSchema};

///
/// Bring groups of records together using the columns specified.
///
/// If a group of records matches all the constraint rules specified, the group is written to a matched
/// file and any records which fail to be matched are written to un-matched files.
///
pub fn match_groups(group_by: &[String], constraints: &[Constraint], grid: &mut Grid, lua: &rlua::Lua, _job_id: &Uuid)
    -> Result<(), MatcherError> {

    log::info!("Grouping by {:?}", group_by);

    let mut group_count = 0;
    let mut match_count = 0;

    lua.context(|lua_ctx| {
        // Form groups from the records.
        for (_key, group) in &grid.records().iter()

            // Build a grouping key from the record using the grouping columns.
            .map(|record| (match_key(record, group_by, grid.schema()), record) )

            // Sort data by the 'group-key' to form contiguous chunks belonging to the same group.
            .sorted_by(|(key1, _record1), (key2, _record2)| Ord::cmp(&key1, &key2))

            // Group data by the group key.
            .group_by(|(key, _record)| key.clone()) {

            let records = group
                .map(|(_key, record)| record)
                .collect::<Vec<&Box<Record>>>();

            // Test any constraints on the group to see if it's a match.
            if is_match(records, constraints, grid.schema(), &lua_ctx)? {
                match_count += 1;
            }

            // TODO: Record cumulative time spent testing constraints.

            // TODO: If passes, add to our match groups and remove the record from the grid.
            // https://stackoverflow.com/questions/49983101/serialization-of-large-struct-to-disk-with-serde-and-bincode-is-slow
            group_count += 1;
        }

        Ok(())
    })
    .map_err(|source| MatcherError::MatchScriptError { source })?;

    log::info!("Matched {} out of {} groups", match_count, group_count);

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
fn is_match(group: Vec<&Box<Record>>, constraints: &[Constraint], schema: &GridSchema, lua_ctx: &Context) -> Result<bool, rlua::Error> {
    let mut failed = vec!();

    for constraint in constraints {
        if !constraint.passes(&group, schema, lua_ctx)? {
            failed.push(constraint);
        }
    }

    Ok(failed.is_empty())
}