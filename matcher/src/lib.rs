mod lua;
mod error;
mod model;
mod convert;
mod folders;
mod matched;
mod changeset;
mod unmatched;
mod instructions;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator, IntoParallelRefMutIterator, IndexedParallelIterator};
use uuid::Uuid;
use anyhow::Result;
use ubyte::ToByteUnit;
use error::MatcherError;
use itertools::Itertools;
use changeset::ChangeSet;
use lazy_static::lazy_static;
use model::schema::GridSchema;
use core::{charter::{Charter, Instruction}, blue, formatted_duration_rate};
use std::{time::{Duration, Instant}, collections::HashMap, cell::Cell, path::{PathBuf, Path}, str::FromStr, fs::File};
use crate::{model::{grid::Grid, schema::Column, record::Record}, instructions::{project_col::{project_column, script_cols}, merge_col}, matched::MatchedHandler, unmatched::UnmatchedHandler};

// TODO: Need to be able to group on dates and ignore the time aspect. Also need tolerance for dates (unit = days)
// TODO: Flesh-out examples.
// TODO: Check code coverage. Need error tests.
// TODO: Remove panics! and unwraps / expects where possible.
// TODO: Clippy!
// TODO: An 'abort' changeset to cancel an erroneous/stuck changeset (maybe it has a syntx error). This would avoid manual tampering.
// TODO: Rename this lib to celerity.
// TODO: Thread-per source file for col-projects and col-merges.
// TODO: https://en.wikipedia.org/wiki/External_sorting external-merge-sort.

lazy_static! {
    // TODO: Read from env - enforce sensible lower limit.
    pub static ref MEMORY_BOUNDS: usize = 150.megabytes().as_u64() as usize; // External merge sort memory bounds.
    pub static ref CSV_BUFFER: usize = 1.megabytes().as_u64() as usize;      // For CSV writer buffers.
}

pub type CsvReader = csv::Reader<File>;
pub type CsvWriter = csv::Writer<File>;
pub type CsvReaders = Vec<CsvReader>;
pub type CsvWriters = Vec<CsvWriter>;

///
/// These are the linear state transitions of a match Job.
///
/// Any error encountered will suspend the job at that phase. It should be safe to start a new job assuming
/// amendments are made to the charter to correct any error, or changesets are aborted.
///
#[derive(Clone, Copy, Debug)]
pub enum Phase {
     FolderInitialisation,
     SourceData,
     ApplyChangeSets,
     DeriveSchema,
     DeriveData,
     MatchAndGroup,
     ComleteAndArchive,
     Complete
}

impl Phase {
    pub fn ordinal(&self) -> usize {
        match self {
            Phase::FolderInitialisation => 1,
            Phase::SourceData           => 2,
            Phase::ApplyChangeSets      => 3,
            Phase::DeriveSchema         => 4,
            Phase::DeriveData           => 5,
            Phase::MatchAndGroup        => 6,
            Phase::ComleteAndArchive    => 7,
            Phase::Complete             => 8,
        }
    }
}

///
/// Created for each match job. Used to pass the main top-level job 'things' around.
///
pub struct Context {
    started: Instant,      // When the job started.
    job_id: Uuid,          // Each job is given a unique id.
    charter: Charter,      // The charter of instructions to run.
    charter_path: PathBuf, // The path to the charter being run.
    base_dir: String,      // The root of the working folder for data (see the folders module).
    timestamp: String,     // A unique timestamp to prefix any generated files with for this job.
    lua: rlua::Lua,        // Lua engine state.
    phase: Cell<Phase>,    // The current point in the linear state transition of the job.
}

impl Context {
    pub fn new(charter: Charter, charter_path: PathBuf, base_dir: String) -> Self {
        let job_id = match std::env::var("OPENREC_FIXED_JOB_ID") {
            Ok(job_id) => uuid::Uuid::from_str(&job_id).expect("Test JOB_ID has invalid format"),
            Err(_) => uuid::Uuid::new_v4(),
        };

        Self {
            started: Instant::now(),
            job_id,
            charter,
            charter_path,
            base_dir,
            timestamp: folders::new_timestamp(),
            lua: rlua::Lua::new(),
            phase: Cell::new(Phase::FolderInitialisation),
        }
    }

    pub fn started(&self) -> Instant {
        self.started
    }

    pub fn job_id(&self) -> &Uuid {
        &self.job_id
    }

    pub fn charter(&self) -> &Charter {
        &self.charter
    }

    pub fn charter_path(&self) -> &PathBuf {
        &self.charter_path
    }

    pub fn base_dir(&self) -> &str {
        &self.base_dir
    }

    pub fn ts(&self) -> &str {
        &self.timestamp
    }

    pub fn lua(&self) -> &rlua::Lua {
        &self.lua
    }

    pub fn phase(&self) -> Phase {
        self.phase.get()
    }

    pub fn set_phase(&self, phase: Phase) {
        self.phase.set(phase);
    }
}

///
/// Create a new match job and run the charter.
///
/// If this library is used as part of a wider solution, care must be taken to synchronise these match jobs
/// so only one can exclusively run against a given charter/folder of data at any one time.
///
pub fn run_charter(charter: &str, base_dir: &str) -> Result<()> {

    let ctx = init_job(charter, base_dir)?;

    ctx.set_phase(Phase::FolderInitialisation);
    init_folders(&ctx)?;

    ctx.set_phase(Phase::SourceData);
    let grid = Grid::load(&ctx)?;

    ctx.set_phase(Phase::ApplyChangeSets);
    let (mut grid, changesets) = apply_changesets(&ctx, grid)?;

    ctx.set_phase(Phase::DeriveSchema);
    let (projection_cols, writers) = create_derived_schema(&ctx, &mut grid)?;

    ctx.set_phase(Phase::DeriveData);
    derive_data_par(&ctx, &grid, projection_cols, writers)?;

    ctx.set_phase(Phase::MatchAndGroup);
    let (matched, unmatched) = match_and_group(&ctx, &mut grid)?;

    ctx.set_phase(Phase::ComleteAndArchive);
    complete_and_archive(&ctx, grid, matched, unmatched, changesets)?;

    ctx.set_phase(Phase::Complete);
    Ok(())
}

///
/// Parse and load the charter configuration, return a job Context.
///
fn init_job(charter: &str, base_dir: &str) -> Result<Context, MatcherError> {
    let ctx = Context::new(Charter::load(charter)?, Path::new(charter).canonicalize()?.to_path_buf(),  base_dir.into());

    log::info!("Starting match job:");
    log::info!("    Job ID: {}", ctx.job_id());
    log::info!("   Charter: {} (v{})", ctx.charter().name(), ctx.charter().version());
    log::info!("  Base dir: {}", ctx.base_dir());

    Ok(ctx)
}

///
/// Prepare the working folders before loading data.
///
fn init_folders(ctx: &Context) -> Result<(), MatcherError> {
    folders::ensure_dirs_exist(&ctx)?;

    // On start-up, any matching files should log warning and be moved to waiting.
    folders::rollback_any_incomplete(&ctx)?;

    // Move any waiting files to the matching folder.
    folders::progress_to_matching(&ctx)?;

    Ok(())
}

///
/// Load and apply changesets to transform new and unmatched data prior to matching.
///
/// If data is modified, we re-load/index the records in a new instance of the grid.
///
fn apply_changesets(ctx: &Context, mut grid: Grid) -> Result<(Grid, Vec<ChangeSet>), MatcherError> {

    let (any_applied, changesets) = changeset::apply(ctx, &mut grid)?;
    if any_applied {
        grid.debug_grid(ctx, 2);
        return Ok((Grid::load(ctx)?, changesets))
    }
    Ok((grid, changesets))
}

///
/// Add a derived column for each projection or merger and calculate which columns each projection
/// is dependant on.
///
fn create_derived_schema(ctx: &Context, grid: &mut Grid) -> Result<(HashMap<usize, Vec<Column>>, CsvWriters), MatcherError> {

    // Debug the grid before the new columns are added.
    grid.debug_grid(ctx, 1);

    let mut projection_cols = HashMap::new();

    // Because both grid needs to be borrowed mutablly, we'll copy an immutable schema to pass around.
    let schema = grid.schema().clone();

    for (idx, inst) in ctx.charter().instructions().iter().enumerate() {
        match inst {
            Instruction::Project { column, as_a, from, when } => {
                projection_cols.insert(idx, script_cols(from, when.as_ref().map(String::as_ref), &schema));
                grid.schema_mut().add_projected_column(Column::new(column.into(), None, *as_a))?;
            },
            Instruction::Merge { into, columns } => {
                if grid.is_empty() {
                    continue;
                }
                let data_type = merge_col::validate(columns, grid)?;
                grid.schema_mut().add_merged_column(Column::new(into.into(), None, data_type))?;
            },
            _ => { /* Ignore other instructions. */}
        }
    }

    // Now we know what columns are derived, write their headers to the .derived files.
    let mut writers = derived_writers(grid)?;
    write_derived_headers(grid.schema(), &mut writers)?;

    // Debug the grid after the columns are added (but before values are derived).
    grid.debug_grid(ctx, 2);

    Ok((projection_cols, writers))
}

///
/// Create a csv::Writer<File> for every sourced data file - it should point to the derived csv file.
///
fn derived_writers(grid: &Grid) -> Result<CsvWriters, MatcherError> {
    Ok(grid.schema()
        .files()
        .iter()
        .map(|f| {
            csv::WriterBuilder::new()
                .has_headers(false)
                // .buffer_capacity(*MEM_CSV_WTR)
                .quote_style(csv::QuoteStyle::Always)
                .from_path(f.derived_path())
                .map_err(|source| MatcherError::CannotOpenCsv{ path: f.derived_path().into(), source } )
        })
        .collect::<Result<Vec<_>, _>>()?)
}

///
/// Write the column headers and schema row for the derived csv files.
///
fn write_derived_headers(schema: &GridSchema, writers: &mut CsvWriters) -> Result<(), MatcherError> {
    for (idx, file) in schema.files().iter().enumerate() {
        let writer = &mut writers[idx];

        writer.write_record(schema.derived_columns().iter().map(|c| c.header_no_prefix()).collect::<Vec<&str>>())
            .map_err(|source| MatcherError::CannotWriteHeaders{ filename: file.derived_filename().into(), source })?;

        writer.write_record(schema.derived_columns().iter().map(|c| c.data_type().as_str()).collect::<Vec<&str>>())
            .map_err(|source| MatcherError::CannotWriteSchema{ filename: file.derived_filename().into(), source })?;
        writer.flush()?;
    }
    Ok(())
}


fn derive_data_par(ctx: &Context, grid: &Grid, projection_cols: HashMap<usize, Vec<Column>>, writers: CsvWriters)
    -> Result<(), MatcherError> {

    // We need one thread per file. TODO: files...num_cpus allow option to use single thread for lowest mem usage.
    rayon::ThreadPoolBuilder::new().num_threads(grid.schema().files().len()).build_global().unwrap();

    // Create a data reader per sourced file. Skip the schema rows.
    let readers: Vec<CsvReader> = grid.schema()
        .files()
        .iter()
        .map(|file| {
            let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_path(file.path())
                .unwrap_or_else(|err| panic!("Failed to open {} : {}", file.path(), err)); // TODO: Consider putting all these in util somewhere.
            let mut ignored = csv::ByteRecord::new();
            rdr.read_byte_record(&mut ignored).unwrap();
            rdr
        })
        .collect();

    let mut zipped: Vec<(CsvReader, CsvWriter)> = readers.into_iter().zip(writers).collect();

    // Wrap the schema and charter in arcs to share amongst threads.
    let schema = std::sync::Arc::new(grid.schema().clone());
    let charter = std::sync::Arc::new(ctx.charter().clone());

    zipped.par_iter_mut()
        .enumerate()
        .map(|(file_idx, (reader, writer))| { // TODO: Consider a tuple of reader+path for error reporting and metrics logging.

            // Track the record and instruction being processed. Used in logs should an error occur.
            //let mut eval_ctx = (0 /* file */, 0 /* row */, 0 /* instruction */);

            let lua = rlua::Lua::new();

            lua.context(|lua_ctx| {
                lua::init_context(&lua_ctx)?;
                // let mut csv_record = csv::ByteRecord::new();
                // reader.read_byte_record(&csv_record)?;
                for csv_record in reader.byte_records() {
                    let mut record = Record::new(file_idx, schema.clone(), csv_record?, csv::ByteRecord::new());

                    for (i_idx, inst) in charter.instructions().iter().enumerate() {
                        match inst {
                            Instruction::Project { column: _, as_a, from, when } => {
                                let script_cols = projection_cols.get(&i_idx).ok_or(MatcherError::MissingScriptCols { instruction: i_idx })?;
                                project_column(*as_a, from, &when, &mut record, script_cols, &lua_ctx)?;
                                // record_duration(i_idx, &mut metrics, started.elapsed());
                            },

                            Instruction::Merge { into: _, columns } => {
                                record.merge_col_from(columns)?;
                                // record_duration(i_idx, &mut metrics, started.elapsed());
                            },

                            _ => {}, // Ignore other instructions in this phase.
                        };
                    }

                    // Flush the current record's buffer to the appropriate derived file.
                    writer.write_byte_record(&record.flush()).map_err(|err| MatcherError::CSVError(err))?;
                }

                Ok(())
            })
        })
        .collect::<Result<(), MatcherError>>()?; // Map error and add eval_context.

    Ok(())
}

///
/// Calculate all projected and derived columns and write them to a .derived file per sourced
/// file. Every corresponding row in the source files will have a row in the derived files which contains
/// projected and merged column data.
///
fn derive_data(ctx: &Context, grid: &Grid, projection_cols: HashMap<usize, Vec<Column>>, mut writers: CsvWriters) -> Result<(), MatcherError> {

    // Track the record and instruction being processed. Used in logs should an error occur.
    let mut eval_ctx = (0 /* file */, 0 /* row */, 0 /* instruction */);

    // Track accumulated time in each project and merge instruction.
    let mut metrics: HashMap<usize, Duration> = HashMap::new();

    // TODO: Write a log info saying we're deriving data...

    ctx.lua().context(|lua_ctx| {
        lua::init_context(&lua_ctx)?;

        // TODO: Derive files in parallel.

        // Calculate all projected and merged column values for each record.
        for mut record in grid.iter(ctx) {
            // TODO: PERF: Create lua record ONCE - here.
            for (i_idx, inst) in ctx.charter().instructions().iter().enumerate() {
                let started = Instant::now();
                eval_ctx = (record.file_idx(), record.row(), i_idx);

                match inst {
                    Instruction::Project { column: _, as_a, from, when } => {
                        let script_cols = projection_cols.get(&i_idx)
                            .ok_or(MatcherError::MissingScriptCols { instruction: i_idx })?;

                        project_column(
                            *as_a,
                            from,
                            &when,
                            &mut record,
                            script_cols,
                            &lua_ctx)?;

                        record_duration(i_idx, &mut metrics, started.elapsed());
                    },

                    Instruction::Merge { into: _, columns } => {
                        record.merge_col_from(columns)?;
                        record_duration(i_idx, &mut metrics, started.elapsed());
                    },

                    _ => {}, // Ignore other instructions in this phase.
                };
            }

            // Flush the current record buffer to the appropriate derived file.
            let csv = record.flush();
            let writer = &mut writers[record.file_idx()];
            writer.write_byte_record(&csv).map_err(|err| MatcherError::CSVError(err))?;
        }
        Ok(())
    })
    .map_err(|source| MatcherError::DeriveDataError {
        instruction: format!("{:?}", ctx.charter().instructions()[eval_ctx.2]),
        row: eval_ctx.1,
        file: grid.schema().files()[eval_ctx.0].filename().into(),
        source
    })?;

    // Report the duration spent performing each projection and merge instruction.
    for idx in metrics.keys().sorted_by(Ord::cmp) {
        let (duration, rate) = formatted_duration_rate(grid.len(), *metrics.get(idx).expect("Duration metric missing"));

        match &ctx.charter().instructions()[*idx] {
            Instruction::Project { column, .. } => log::info!("Projecting Column {} took {} ({}/row)", column, blue(&duration), rate),
            Instruction::Merge { into, .. } => log::info!("Merging Column {} took {} ({}/row)", into, blue(&duration), rate),
            _ => {},
        }
    }

    // Debug the derived data now.
    grid.debug_grid(ctx, 1);

    Ok(())
}

///
/// Set the initial or increment the existing duration for the specified charter instruction.
///
fn record_duration(instruction: usize, metrics: &mut HashMap<usize, Duration>, elapsed: Duration) {
    if !metrics.contains_key(&instruction) {
        metrics.insert(instruction, Duration::ZERO);
    }

    metrics.insert(instruction, elapsed + *metrics.get(&instruction).expect("Instruction metric missing"));
}

///
/// Run all other instructions that don't create derived data. Create a new accessor which
/// can read from our persisted .derived files.
///
fn match_and_group(ctx: &Context, grid: &mut Grid) -> Result<(MatchedHandler, UnmatchedHandler), MatcherError> {

    // Create a match file containing job details and giving us a place to append match results.
    let mut matched = MatchedHandler::new(ctx, grid)?;

    // Create unmatched files for each sourced file.
    let unmatched = UnmatchedHandler::new(ctx, grid)?;

    let schema = grid.schema().clone();

    for (i_idx, inst) in ctx.charter().instructions().iter().enumerate() {
        match inst {
            Instruction::Project { .. } => {},
            Instruction::Merge { .. } => {},
            Instruction::Group { by, match_when } => {
                // instructions::match_groups::match_groups(
                //     ctx,
                //     i_idx,
                //     by,
                //     match_when,
                //     grid,
                //     &schema,
                //     ctx.lua(),
                //     &mut matched)?;

                instructions::match_groups::match_groups_new(
                    ctx,
                    i_idx,
                    by,
                    match_when,
                    grid,
                    &mut matched)?;
            },
        };

        log::debug!("Grid Memory Size: {}",
            blue(&format!("{:.0}", grid.memory_usage().bytes())));
    }

    Ok((matched, unmatched))
}

///
/// Complete the matched file and write-out the unmatched records.
///
/// Move data to the archive folders and delete any temporary files.
///
fn complete_and_archive(
    ctx: &Context,
    grid: Grid,
    mut matched: MatchedHandler,
    mut unmatched: UnmatchedHandler,
    changesets: Vec<ChangeSet>) -> Result<(), MatcherError> {

    // Write all unmatched records now - this will be optimised at a later stage to be a single call.
    unmatched.write_records(ctx, &grid)?;

    // Complete the matched JSON file.
    matched.complete_files(&unmatched, changesets)?;

    // Debug the final grid now.
    grid.debug_grid(ctx, 1);

    // Move matching files to the archive.
    folders::progress_to_archive(&ctx, grid)?;

    // Log a warning for any file left in matching at the end of a job.
    let left_overs = folders::matching(ctx).read_dir()?
        .map(|entry| entry.expect("unable to read matching file").file_name().to_str().unwrap_or("no-name").to_string())
        .join("\n");

    if !left_overs.is_empty() {
        log::warn!("The following files were still in the matching folder at the end of the job:\n{}", left_overs);
    }

    // TODO: Log (and record in job json) how many records processed, rate, MB size, etc.
    log::info!("Completed match job {} in {}", ctx.job_id(), blue(&formatted_duration_rate(1, ctx.started().elapsed()).0));

    Ok(())
}
