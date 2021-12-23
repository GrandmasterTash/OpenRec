mod lua;
mod error;
mod model;
mod convert;
mod folders;
mod matched;
mod changeset;
mod unmatched;
mod instructions;
mod data_accessor;

use uuid::Uuid;
use anyhow::Result;
use ubyte::ToByteUnit;
use error::MatcherError;
use itertools::Itertools;
use changeset::ChangeSet;
use std::{time::{Duration, Instant}, collections::HashMap, cell::Cell, path::{PathBuf, Path}, str::FromStr};
use crate::{model::{charter::{Charter, Instruction}, grid::Grid, schema::Column}, instructions::{project_col::{project_column, script_cols}, merge_col}, matched::MatchedHandler, unmatched::UnmatchedHandler, data_accessor::DataAccessor};

// TODO: Flesh-out examples.
// TODO: Check code coverage. Need error tests.
// TODO: Remove panics! and unwraps / expects where possible.
// TODO: Clippy!
// TODO: An 'abort' changeset to cancel an erroneous/stuck changeset (maybe it has a syntx error). This would avoid manual tampering.
// TODO: Rename this lib to celerity.
// TODO: Thread-per source file for col-projects and col-merges.
// TODO: https://en.wikipedia.org/wiki/External_sorting external-merge-sort.

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
    let (projection_cols, mut accessor) = create_derived_schema(&ctx, &mut grid)?;

    ctx.set_phase(Phase::DeriveData);
    derive_data(&ctx, &mut grid, &mut accessor, projection_cols)?;

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
        // Debug the grid after any changesets have been applied.
        let mut accessor = DataAccessor::for_reading_no_derived(&mut grid)?;
        grid.debug_grid(ctx, 2, &mut accessor);

        return Ok((Grid::load(ctx)?, changesets))
    }
    Ok((grid, changesets))
}

///
/// Add a derived column for each projection or merger and calculate which columns each projection
/// is dependant on.
///
fn create_derived_schema(ctx: &Context, grid: &mut Grid) -> Result<(HashMap<usize, Vec<Column>>, DataAccessor), MatcherError> {

    // Create a DataAccessor now to use through the first two instruction passes. It will run in write mode
    // meaning it will be writing derived values to a buffer for each record and flushing to disk.
    let mut accessor = DataAccessor::for_deriving(&grid)?;

    // Debug the grid before the new columns are added.
    grid.debug_grid(ctx, 1, &mut accessor);

    let mut projection_cols = HashMap::new();

    // Because both grid and accessor need to be borrowed mutablly, we'll copy an immutable schema
    // to pass around.
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

    // Ensure the accessor's schema is sync'd with the modified grid's schema.
    accessor.set_schema(grid.schema().clone());

    // Now we know what columns are derived, write their headers to the .derived files.
    accessor.write_derived_headers()?;

    // Debug the grid after the columns are added (but before values are derived).
    grid.debug_grid(ctx, 2, &mut accessor);

    Ok((projection_cols, accessor))
}

///
/// Calculate all projected and derived columns and write them to a .derived file per sourced
/// file. Every corresponding row in the source files will have a row in the derived files which contains
/// projected and merged column data.
///
fn derive_data(ctx: &Context, grid: &mut Grid, accessor: &mut DataAccessor, projection_cols: HashMap<usize, Vec<Column>>) -> Result<(), MatcherError> {

    // Track the record and instruction being processed. Used in logs should an error occur.
    let mut eval_ctx = (0, 0);

    // Track accumulated time in each project and merge instruction.
    let mut metrics: HashMap<usize, Duration> = HashMap::new();

    ctx.lua().context(|lua_ctx| {
        lua::init_context(&lua_ctx)?;

        // Calculate all projected and merged column values for each record.
        for (r_idx, record) in grid.records().iter().enumerate() {
            for (i_idx, inst) in ctx.charter().instructions().iter().enumerate() {
                let started = Instant::now();
                eval_ctx = (r_idx, i_idx);

                match inst {
                    Instruction::Project { column: _, as_a, from, when } => {
                        let script_cols = projection_cols.get(&i_idx)
                            .ok_or(MatcherError::MissingScriptCols { instruction: i_idx })?;

                        project_column(
                            *as_a,
                            from,
                            when,
                            record,
                            accessor,
                            script_cols,
                            &lua_ctx)?;

                        record_duration(i_idx, &mut metrics, started.elapsed());
                    },

                    Instruction::Merge { into: _, columns } => {
                        record.merge_col_from(columns, accessor)?;
                        record_duration(i_idx, &mut metrics, started.elapsed());
                    },

                    _ => {}, // Ignore other instructions in this phase.
                };
            }

            // Flush the current record buffer to the appropriate derived file.
            record.flush(accessor)?;
        }
        Ok(())
    })
    .map_err(|source| MatcherError::DeriveDataError {
        instruction: format!("{:?}", ctx.charter().instructions()[eval_ctx.1]),
        row: grid.records()[eval_ctx.0].row(),
        file: grid.schema().files()[grid.records()[eval_ctx.0].file_idx()].filename().into(),
        source
    })?;

    // Report the duration spent performing each projection and merge instruction.
    for idx in metrics.keys().sorted_by(Ord::cmp) {
        let (duration, rate) = formatted_duration_rate(grid.records().len(), *metrics.get(idx).expect("Duration metric missing"));

        match &ctx.charter().instructions()[*idx] {
            Instruction::Project { column, .. } => log::info!("Projecting Column {} took {} ({}/row)", column, blue(&duration), rate),
            Instruction::Merge { into, .. } => log::info!("Merging Column {} took {} ({}/row)", into, blue(&duration), rate),
            _ => {},
        }
    }

    // Debug the derived data now.
    let mut debug_accessor = DataAccessor::for_reading(grid)?;
    grid.debug_grid(ctx, 1, &mut debug_accessor);

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

    // Create a read-mode derived accessor used to read real and derived data.
    let mut accessor = DataAccessor::for_reading(grid)?;
    let schema = grid.schema().clone();

    for (i_idx, inst) in ctx.charter().instructions().iter().enumerate() {
        match inst {
            Instruction::Project { .. } => {},
            Instruction::Merge { .. } => {},
            Instruction::Group { by, match_when } => {
                instructions::match_groups::match_groups(
                    ctx,
                    i_idx,
                    by,
                    match_when,
                    grid,
                    &schema,
                    &mut accessor,
                    ctx.lua(),
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
    mut grid: Grid,
    mut matched: MatchedHandler,
    mut unmatched: UnmatchedHandler,
    changesets: Vec<ChangeSet>) -> Result<(), MatcherError> {

    // Write all unmatched records now - this will be optimised at a later stage to be a single call.
    unmatched.write_records(ctx, grid.records(), &grid)?;

    // Complete the matched JSON file.
    matched.complete_files(&unmatched, changesets)?;

    // Debug the final grid now.
    let mut accessor = DataAccessor::for_reading(&mut grid)?;
    grid.debug_grid(ctx, 1, &mut accessor);

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

///
/// Provide a consistent formatting for durations and rates.
///
/// The format_duration will show micro and nano seconds but we typically only need to see ms.
///
pub fn formatted_duration_rate(amount: usize, elapsed: Duration) -> (String, String) {
    let duration = Duration::new(elapsed.as_secs(), elapsed.subsec_millis() * 1000000); // Keep precision to ms.
    let rate = (elapsed.as_millis() as f64 / amount as f64) as f64;
    (
        humantime::format_duration(duration).to_string(),
        format!("{:.3}ms", rate)
    )
}

///
/// Highlight some log output with ansi colour codes.
///
pub fn blue(msg: &str) -> ansi_term::ANSIGenericString<'_, str> {
    ansi_term::Colour::RGB(70, 130, 180).paint(msg)
}
