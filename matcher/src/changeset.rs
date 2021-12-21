use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{io::BufReader, fs::File, collections::HashMap, time::{Duration, Instant}};
use crate::{Context, error::MatcherError, folders::{self, ToCanoncialString}, lua, model::{grid::Grid, datafile::DataFile, record::Record, schema::GridSchema}, data_accessor::DataAccessor, formatted_duration_rate, blue};

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
                                                             20210110_inv.csv.                                             20210110_inv.csv
                                                             20210111_changeset.json
                                                             20210109_inv.unmatched.csv.pre_modified
    ------------------------------------ MATCH COMPLETE (unmatch data remains from both file) ---------------------------------------------
    WAITING                   UNMATCHED                              MATCHING                      MATCHED                 ARCHIVE
                              20210109_inv.unmatched.csv                                           20210109_matched.json   20210109_inv.csv
                              20210110_inv.unmatched.csv                                           20210111_matched.json   20210110_inv.csv
                                                                                                   20210111_changeset.json 20210110_inv.csv.pre_modified

    Whilst changesets are being applied, new data files are written into the matching folder with the .modifying extension. These files
    contain the original data with any changeset modifications applied. Note: If a record is ignored by a changeset, it is absent from the
    new file.

    At the end of ChangeSet processing, the original unmatched files are given a _pre_modified suffix and for data which is new for this
    match job (i.e. it arrived in the waiting folder) the original file is immediately moved to the archive folder.

    Next the .modifying suffix is removed from the new copies of the data and matching continues by re-sourcing the grid from the latest set
    of files in the matching folder.

    At the end of the job, _pre_modified files (i.e. the unmatched files) are dropped and data which was new at the start of the job will be
    archived as normal, except, because the filename already exists, the modified data files are archived and given a new extension of
    '.modified.csv' to avoid a collision with the unmodified version of the datafile.
*/

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Change {
    UpdateFields { updates: Vec<FieldChange>, lua_filter: String },
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
    source: Option<String>,
    comment: Option<String>,
    approved_by: Option<Vec<String>>, // TODO: Consider metadata for changesets and charters.
    timestamp: DateTime<Utc>,

    #[serde(skip)]
    effected: usize,

    #[serde(skip)]
    elapsed: Duration,

    #[serde(skip)]
    filename: String,
}

impl ChangeSet {
    pub fn change(&self) -> &Change {
        &self.change
    }

    pub fn effected(&self) -> usize {
        self.effected
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn set_filename(&mut self, filename: String) {
        self.filename = filename;
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
/// Apply any ChangeSets to the csv data now.
///
pub fn apply(ctx: &Context, grid: &mut Grid) -> Result<(bool, Vec<ChangeSet>), MatcherError> {

    // Load any changesets for the data.
    let mut changesets = load_changesets(ctx)?;

    // Clone the grid schema - some lower level fns need mut accessor, mut grid and an immutable schema.
    let schema = grid.schema().clone();

    // Create a DataAccessor to read real CSV data only (derived data wont exist yet) and to write any
    // required modified data out to new files.
    let mut accessor = DataAccessor::for_modifying(grid)?;

    // Track how many changes are made to each file.
    let mut metrics = init_metrics(grid);

    // Track the record and changeset being processed.
    let mut eval_ctx = (0, 0);

    ctx.lua().context(|lua_ctx| {
        lua::init_context(&lua_ctx)?;

        // Apply each changeset in order to each record.
        for (r_idx, record) in grid.records_mut().iter().enumerate() {
            eval_ctx = (r_idx, 0);

            let data_file = &schema.files()[record.file_idx()];

            // Load the current record into a buffer.
            accessor.load_modifying_record(record)?;

            for (c_idx, changeset) in &mut changesets.iter_mut().enumerate() {
                let started = Instant::now();
                eval_ctx = (r_idx, c_idx);

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

                    changeset.effected += 1;
                    changeset.elapsed += started.elapsed();
                }
            }

            // Copy the record across now as-is or modified - or skip if ignored.
            if !record.deleted() {
                accessor.modifying_accessor().flush(record)?;
            }
        }
        Ok(())
    })
    .map_err(|source| MatcherError::ChangeSetError {
        changeset: format!("{:?}", changesets[eval_ctx.1].id),
        row: grid.records()[eval_ctx.0].row(),
        file: grid.schema().files()[grid.records()[eval_ctx.0].file_idx()].filename().into(),
        source
    })?;

    // Delete any ignored records from memory.
    grid.remove_deleted();

    // Finalise the modifying files, renaming and archiving things as required.
    let any_applied = finalise_files(ctx, &metrics)?;

    for changeset in &changesets {
        let (duration, rate) = formatted_duration_rate(grid.records().len(), changeset.elapsed);
        log::info!("ChangeSet {} effected {} record(s) in {} ({}/row)", changeset.id, changeset.effected, blue(&duration), rate);
    }

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

                log::debug!("Renaming {} to {}", data_file.modifying_filename(), data_file.filename());
                folders::rename(data_file.modifying_path(), data_file.path())?;

            } else {
                // For un-matched files, we'll rename the current file with a .pre_modified suffix.
                folders::rename(data_file.path(), data_file.pre_modified_path())?;
                folders::rename(data_file.modifying_path(), data_file.path())?;
                folders::remove_file(data_file.pre_modified_path())?;
            }
        } else {
            log::debug!("Removing unmodified modifying file {}", data_file.modifying_path());
            folders::remove_file(data_file.modifying_path())?;
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

    let effected = !lua::lua_filter(&vec!(record), &lua_filter, lua_ctx, accessor, schema)?.is_empty();

    log::trace!("record_effected: {} : {:?} : {}", lua_filter, record.as_strings(schema, accessor), effected);

    Ok(effected)
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

        for changeset in &mut content {
            changeset.set_filename(file.file_name().to_string_lossy().into());
        }

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
    folders::UNMATCHED_REGEX.is_match(file.filename())
}