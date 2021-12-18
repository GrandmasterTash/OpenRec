use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{io::BufReader, fs::{File, self}, collections::HashMap};
use crate::{Context, error::MatcherError, folders::{self, ToCanoncialString}, lua, model::{grid::Grid, datafile::DataFile, record::Record, schema::GridSchema}, data_accessor::DataAccessor};

// TODO: Update the comments here to reflect latest implemented.
// TODO: Implement some changeset examples and tests.

/*
    Changeset files are instructions to modified unmatched or yet-to-be-matched data.
    The following shows how the flow of files through thr folder 'pipe-line' functions when a changeset is involved.
    Note: Only the date portion of the filename timestamp is shown for brevity.

    --------------------------------------------- BEFORE JOB ------------------------------------------------------------------------------
    WAITING                   UNMATCHED                      MATCHING                              MATCHED                 ARCHIVE
    20210110_inv.csv          20210109_inv.unmatched.csv                                           20210109_matched.json   20210109_inv.csv
    20210111_changeset.json
    --------------------------------------------- GRID DATA SOURCED -----------------------------------------------------------------------
    WAITING                   UNMATCHED                      MATCHING                              MATCHED                 ARCHIVE
                                                             20210109_inv.unmatched.csv            20210109_matched.json   20210109_inv.csv
                                                             20210110_inv.csv
                                                             20210111_changeset.json
    ------------------------------------ CHANGSETS APPLIED (affecting both data files) ----------------------------------------------------
    WAITING                   UNMATCHED                      MATCHING                              MATCHED                 ARCHIVE
                                                             20210109_inv.unmatched.csv            20210109_matched.json   20210109_inv.csv
                                                             20210110_inv.csv.modified.csv                                 20210110_inv.csv
                                                             20210111_changeset.json
                                                             20210109_inv.unmatched.bak
    ------------------------------------ MATCH COMPLETE (unmatch data remains from both file) ---------------------------------------------
    WAITING                   UNMATCHED                              MATCHING                      MATCHED                 ARCHIVE
                              20210109_inv.unmatched.csv                                           20210109_matched.json   20210109_inv.csv
                              20210110_inv.unmatched.csv                                           20210111_matched.json   20210110_inv.csv
                                                                                                   20210111_changeset.json

    Note: The .modified section of the filename above is dropped when unmatched data is created. That-is, only original files will
    be given a .modified tag in the filename, but not unmatched files.

    There are some intermediary stages within the CHANGESETS APPLIED stage above which are not shown.
    They involve the creation of a temporary named version of file of the modified unmatched file. The pre-modified unmatched file
    is renamed to .bak (which is deleted at the end of the job.).

    Given the above unmatched file '20210109_inv.unmatched.csv' then a new file will be created '20210109_inv.unmatched.csv.<job_id>'
    where modified data (the unmatched data with the changes in the changeset being applied) are written to.

    Once all changsets have been processed, the original unmatched file '20210109_inv.unmatched.csv' will be deleted and the job_id
    suffix on the new version of the file removed.
*/

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Change {
    UpdateFields { updates: Vec<FieldChange>, lua_filter: String }, // TODO: Prevent filter containing any field in updates otherwise cyclic dependency = stack overflow
    IgnoreRecords { lua_filter: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FieldChange {
    field: String,
    value: String
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChangeSet {
    id: uuid::Uuid,
    change: Change,
    source: String,
    comment: String,
    approved_by: Vec<String>,
    timestamp: DateTime<Utc>,
}

impl ChangeSet {
    pub fn change(&self) -> &Change {
        &self.change
    }
}

///
/// The Chisel is used to create new versions of DataFiles with ChangeSets applied.
///
struct Metrics {
    modified: usize,
    ignored: usize
}

///
/// A flag to indiciate if ANY changes were applied to data.
/// A list of all changesets evaluated.
/// A map of DataFiles to Metrics indicating details of any changes in those files.
///
pub type ChangeSetResult = Result<(bool, Vec<ChangeSet>), MatcherError>;

///
/// Apply any ChangeSets to the csv data now.
///
pub fn apply(ctx: &Context, grid: &mut Grid, lua: &rlua::Lua) -> ChangeSetResult {

    // Load any changesets for the data.
    let changesets = load_changesets(ctx)?;

    // Clone the grid schema - some lower level fns need mut accessor, mut grid and an immutable schema.
    let schema = grid.schema().clone();

    // Create a DataAccessor to read real CSV data only (derived data wont exist yet) and to write any
    // required modified data out to new files.
    let mut accessor = DataAccessor::for_modifying(grid)?;

    // Track how many changes are made to each file.
    let mut metrics = init_metrics(grid);

    lua.context(|lua_ctx| {
        lua::init_context(&lua_ctx)?;
        // TODO: Use an eval_ctx like merge and projections to report exact file/row failure points. Log changeset name.

        // Apply each changeset in order to each record.
        for record in grid.records_mut() {
            let data_file = &schema.files()[record.file_idx()];

            // Load the current record into a buffer.
            accessor.load_modifying_record(record)?;

            for changeset in &changesets {
                let lua_filter = match changeset.change() {
                    Change::UpdateFields { updates: _, lua_filter } => lua_filter,
                    Change::IgnoreRecords { lua_filter }            => lua_filter,
                };

                if record_effected(record, &lua_filter, &lua_ctx, &mut accessor, &schema)? {
                    match changeset.change() {
                        Change::UpdateFields { updates, .. } => {
                            // Modify the record in a buffer.
                            for update in updates {
                                accessor.update(record, &update.field, &update.value)?;
                            }
                            metrics.get_mut(data_file).expect("No metrics for record").modified += 1;
                        },
                        Change::IgnoreRecords { .. } => {
                            // Stops the modified record being written and index is removed from memory.
                            record.set_deleted();
                            metrics.get_mut(data_file).expect("No metrics for record").ignored += 1;
                        },
                    }
                }
            }

            // Copy the record across now as-is or modified - or skip if ignored.
            if !record.deleted() {
                accessor.modifying_accessor().flush(record)?;
            }
        }
        Ok(())
    })
    .map_err(|source| MatcherError::MatchGroupError { source })?;

    // Delete any ignored records from memory.
    grid.remove_deleted();

    // Finalise the modifying files, renaming and archiving things as required.
    let any_applied = finalise_files(ctx, &metrics)?;

    /* 
    TODO: Produce some output like this.
    It must be logged here.
    It must be returned from this fn so it can be recorded in the job.json.
    wibble_changeset.json      Updated      Ignored
        wibble_inv.csv     100,000,000  100,000,000
        wobble_pay.csv     100,000,000  100,000,000
    wobble_changeset.json      Updated      Ignored
        wibble_inv.csv     100,000,000  100,000,000
        wibble_pay.csv               0          100
        wobble_pay.csv     100,000,000  100,000,000
    nobble_changeset.json      Updated      Ignored
                            --- nothing updated ---
    */

    Ok((any_applied, changesets))
}

///
/// Archive replace original data with the modified files (archiving original data files immediate).
///
/// Replace unmatched files with the modified variants.
///
/// Progress the changeset.json to matched to ensure future errors wont re-process it on processed data.
///
/// Report on any changes made to the data.
///
fn finalise_files(ctx: &Context, metrics: &HashMap<DataFile, Metrics>) -> Result<bool, MatcherError> {
    let mut any_applied = false;

    for (data_file, metric) in metrics.iter() {
        if metric.modified > 0 || metric.ignored > 0 {
            any_applied = true;

            if !is_unmatched(data_file) {
                // For new data files, we need to archive the original file immediately.
                // Then rename the modifying file, eg. 20210110_inv.csv.modifying -> 20210110_inv.csv
                folders::archive_immediately(ctx, data_file.path())?;

                log::info!("Moving {} to {}", data_file.modifying_filename(), data_file.filename());
                fs::rename(data_file.modifying_path(), data_file.path())?;

            } else {
                // For un-matched files, we'll rename the current file with a .pre_modified suffix.
                fs::rename(data_file.path(), data_file.pre_modified_path())?; // TODO: Map these errors so we know which files failed.
                fs::rename(data_file.modifying_path(), data_file.path())?;
                fs::remove_file(data_file.pre_modified_path())?;
            }
        } else {
            log::debug!("Removing unmodified modifying file {}", data_file.modifying_path());
            fs::remove_file(data_file.modifying_path())?;
        }
    }

    // Move all the changesets (.json) to the matched folder now. This means, any future error wont
    // attempt to re-apply them to already modified data.
    for file in &folders::changesets_in_matching(ctx)? {
        folders::progress_to_matched_now(ctx, file)?;
    }

    Ok(any_applied)
}

///
/// Returns true if the record matches the filter criteria evaluated from the Lua script.
///
fn record_effected(
    record: &Record,
    lua_filter: &str,
    lua_ctx: &rlua::Context,
    accessor: &mut DataAccessor,
    schema: &GridSchema) -> Result<bool, MatcherError> {

    Ok(!lua::lua_filter(&vec!(record), &lua_filter, lua_ctx, accessor, schema)?.is_empty())
}

///
/// Load and parse all the json changeset files waiting to be processed.
///
fn load_changesets(ctx: &Context) -> Result<Vec<ChangeSet>, MatcherError> {

    let mut changesets: Vec<ChangeSet> = vec!();

    for file in folders::changesets_in_matching(ctx)? {
        let reader = BufReader::new(File::open(file.path())?);
        let mut content: Vec<ChangeSet> = serde_json::from_reader(reader)
            .map_err(|source| MatcherError::UnableToParseChangset { path: file.path().to_canoncial_string(), source } )?;

        log::info!("Loaded changeset {}", file.file_name().to_string_lossy());
        changesets.append(&mut content);
    }

    Ok(changesets)
}

///
/// Initialise or increment the ChangeSet metrics for the DataFile and return true if this is the first
/// Record to effect the given DataFile.
///
fn init_metrics(grid: &Grid) -> HashMap<DataFile, Metrics> {
    grid.schema().files()
        .iter()
        .map(|file| (file.clone(), Metrics { modified: 0, ignored: 0 }) )
        .collect()
}

///
/// Returns true if the file is an unmatched file. e.g.
/// 20201118_053000000_invoices.csv -> false
/// 20201118_053000000_invoices.unmatched.csv -> true
/// 20201118_053000000_invoices.unmatched.csv.modifying -> true
///
fn is_unmatched(file: &DataFile) -> bool {
    folders::UNMATCHED_REGEX.is_match(file.original_filename())
}