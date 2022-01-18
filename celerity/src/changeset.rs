use chrono::{DateTime, Utc};
use anyhow::Context as ErrContext;
use serde::{Deserialize, Serialize};
use core::lua::init_context;
use std::{io::BufReader, fs::File, collections::HashMap, time::{Duration, Instant}};
use crate::{Context, error::{MatcherError, here}, folders::{self, ToCanoncialString}, lua, model::{grid::Grid, datafile::DataFile, record::Record, schema::GridSchema}, formatted_duration_rate, blue, utils::{self, csv::{CsvWriters, CsvWriter}}};

/*
    Whilst changesets are being applied, new data files are written into the matching folder with the .modifying extension. These files
    contain the original data with any changeset modifications applied. Note: If a record is ignored by a changeset, it is absent from the
    new file.

    At the end of ChangeSet processing, the original file is immediately moved to the archive folder.

    Next the .modifying suffix is removed from the new copies of the data files and matching continues by re-sourcing the grid from the
    latest set of files in the matching folder.

    At the end of the job, modified data files will be archived as normal (note: unmatched files are never archived), except, because the
    filename already exists now in the archive, the modified data files are archived and given a unique numeric suffix to avoid a
    collision with the unmodified version of the original datafile.
*/

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Change {
    UpdateFields { updates: Vec<FieldChange>, lua_filter: String },
    IgnoreRecords { lua_filter: String },
    IgnoreFile { filename: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FieldChange {
    field: String,
    value: String
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChangeSet {
    id: uuid::Uuid,                     // A unique UUID for the changeset. May be used in logs.
    change: Change,                     // The change to apply.
    timestamp: DateTime<Utc>,           // The time this change was created.

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

struct EvalContext {
    change_idx: usize,  // Which changeset is being evaluated?
    row: usize,         // What row number is being evalutated?
    file: usize         // Which file is being evaludated?
}

///
/// Apply any ChangeSets to the csv data now.
///
pub fn apply(ctx: &Context, grid: &mut Grid) -> Result<(bool, Vec<ChangeSet>), MatcherError> {

    // Load any changesets for the data.
    let mut changesets = load_changesets(ctx)?;
    let mut any_applied = false;

    if !changesets.is_empty() {
        // Clone the grid schema - some lower level fns need mut accessor, mut grid and an immutable schema.
        let schema = grid.schema().clone();

        // Create a DataAccessor to read real CSV data only (derived data wont exist yet) and to write any
        // required modified data out to new files.
        let mut writers = writers(grid)?;

        // Debug the grid if we have any changesets - before they are evaluated.
        grid.debug_grid(ctx, 1);

        // Track how many changes are made to each file.
        let mut metrics = init_metrics(grid);

        // Track the record and changeset being processed.
        let mut eval_ctx = EvalContext { change_idx: 0, row: 0, file: 0 };

        ctx.lua().context(|lua_ctx| {
            init_context(&lua_ctx, ctx.charter().global_lua(), &folders::lookups(ctx))?;

            // TODO: Apply IgnoreFiles first.

            // Apply each changeset in order to each record.
            for mut record in grid.iter(ctx) {
                eval_ctx.change_idx = 0;
                eval_ctx.row = record.row();
                eval_ctx.file = record.file_idx();

                let data_file = &schema.files()[record.file_idx()];
                let mut deleted = false;

                // Populate all the fields of the record into it's writer buffer.
                record.load_buffer();

                for (c_idx, changeset) in &mut changesets.iter_mut().enumerate() {
                    let started = Instant::now();
                    eval_ctx.change_idx = c_idx;

                    let lua_filter = match changeset.change() {
                        Change::UpdateFields { updates: _, lua_filter } => lua_filter,
                        Change::IgnoreRecords { lua_filter }            => lua_filter,
                    };

                    if record_effected(&record, lua_filter, &lua_ctx, &schema)? {
                        match changeset.change() {
                            Change::UpdateFields { updates, .. } => {
                                // Modify the record in a buffer.
                                for update in updates {
                                    record.update(&update.field, &update.value)?;
                                }
                                metrics.get_mut(data_file).expect("No metrics for record").modified += 1;
                            },
                            Change::IgnoreRecords { .. } => {
                                // Stops the modified record being written and index is removed from memory.
                                deleted = true;
                                metrics.get_mut(data_file).expect("No metrics for record").ignored += 1;
                            },
                        }

                        changeset.effected += 1;
                        changeset.elapsed += started.elapsed();
                    }
                }

                // Copy the record across now as-is or modified - or skip if ignored.
                if !deleted {
                    let csv = record.flush();
                    let writer = &mut writers[record.file_idx()];
                    writer.write_byte_record(&csv).map_err(MatcherError::CSVError)?;
                }
            }
            Ok(())
        })
        .map_err(|source| MatcherError::ChangeSetError {
            changeset: format!("{:?}", eval_ctx.change_idx),
            row: eval_ctx.row,
            file: grid.schema().files()[eval_ctx.file].filename().into(),
            source
        })?;

        // Finalise the modifying files, renaming and archiving things as required.
        any_applied = finalise_files(ctx, &metrics, grid)?;

        for changeset in &changesets {
            let (duration, rate) = formatted_duration_rate(grid.len(), changeset.elapsed);
            log::info!("ChangeSet {} effected {} record(s) in {} ({}/row)", changeset.id, changeset.effected, blue(&duration), rate);
        }
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
fn finalise_files(ctx: &Context, metrics: &HashMap<DataFile, Metrics>, grid: &mut Grid) -> Result<bool, MatcherError> {

    let mut any_applied = false;

    for (data_file, metric) in metrics.iter() {
        if metric.modified > 0 || metric.ignored > 0 {
            any_applied = true;

            if !is_unmatched(data_file) {
                // For new data files, we need to archive the original file immediately. Find the mutable grid instance
                // so we can archive and set the archived filename.
                if let Some(grid_df) = grid.schema_mut().files_mut()
                    .find(|df| df.path().to_canoncial_string() == data_file.path().to_canoncial_string()) {

                    folders::archive_data_file(ctx, grid_df)?;
                }

                // Then rename the modifying file, eg. 20210110_inv.csv.modifying -> 20210110_inv.csv
                log::debug!("Renaming {} to {}", data_file.modifying_path().to_canoncial_string(), data_file.filename());
                folders::rename(data_file.modifying_path(), data_file.path())?;

            } else {
                // For un-matched files, we'll rename the current file with a .pre_modified suffix.
                folders::rename(data_file.path(), data_file.pre_modified_path())?;
                folders::rename(data_file.modifying_path(), data_file.path())?;
                folders::remove_file(data_file.pre_modified_path())?;
            }
        } else {
            log::debug!("Removing unmodified modifying file {}", data_file.modifying_path().to_canoncial_string());
            folders::remove_file(data_file.modifying_path())?;
        }
    }

    // Move all the changesets (.json) to the matched folder now. This means, any future error wont
    // attempt to re-apply them to already modified data.
    for file in &folders::changesets_in_matching(ctx)? {
        folders::progress_to_archive_now(ctx, file)?;
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
    schema: &GridSchema) -> Result<bool, MatcherError> {

    let effected = !lua::lua_filter(&[record], lua_filter, lua_ctx, schema)?.is_empty();

    log::trace!("record_effected: {} : {:?} : {}", lua_filter, record.as_strings(), effected);

    Ok(effected)
}

///
/// Load and parse all the json changeset files waiting to be processed.
///
fn load_changesets(ctx: &Context) -> Result<Vec<ChangeSet>, MatcherError> {

    let mut changesets: Vec<ChangeSet> = vec!();

    for file in folders::changesets_in_matching(ctx)? {
        let reader = BufReader::new(File::open(file.path())
            .with_context(|| format!("Unable to open {}{}", file.path().to_canoncial_string(), here!()))?);

        let mut content: Vec<ChangeSet> = serde_json::from_reader(reader)
            .with_context(|| format!("Unable to parse {}{}", file.path().to_canoncial_string(), here!()))?;

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

///
/// A list of open CSV writers in the order of files sourced into the Grid.
///
fn writers(grid: &Grid) -> Result<CsvWriters, MatcherError> {
    let mut writers = grid.schema()
        .files()
        .iter()
        .map(|f| utils::csv::writer(f.modifying_path()))
        .collect::<Vec<CsvWriter>>();

    // Write the headers and schema rows.
    for (idx, file) in grid.schema().files().iter().enumerate() {
        let writer = &mut writers[idx];
        let schema = &grid.schema().file_schemas()[file.schema_idx()];

        writer.write_record(schema.columns().iter().map(|c| c.header_no_prefix()).collect::<Vec<&str>>())
            .map_err(|source| MatcherError::CannotWriteHeaders{ filename: file.derived_filename().into(), source })?;

        writer.write_record(schema.columns().iter().map(|c| c.data_type().as_str()).collect::<Vec<&str>>())
            .map_err(|source| MatcherError::CannotWriteSchema{ filename: file.derived_filename().into(), source })?;

        writer.flush()?;
    }

    Ok(writers)
}