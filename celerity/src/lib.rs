mod lua;
mod utils;
mod error;
mod model;
mod folders;
mod matching;
mod changeset;
mod instructions;

use uuid::Uuid;
use error::MatcherError;
use itertools::Itertools;
use changeset::ChangeSet;
use utils::csv::CsvWriters;
use model::schema::GridSchema;
use folders::ToCanoncialString;
use anyhow::{Result, Context as ErrContext};
use rayon::iter::{IntoParallelRefMutIterator, IndexedParallelIterator, ParallelIterator};
use core::{charter::{Charter, Instruction}, blue, formatted_duration_rate, lua::init_context};
use std::{time::{Instant, Duration}, collections::HashMap, cell::Cell, path::{PathBuf, Path}, str::FromStr, sync::Arc};
use crate::{model::{grid::Grid, schema::Column, record::Record}, instructions::{project_col::{project_column, referenced_cols}, merge_col}, matching::matched::MatchedHandler, matching::unmatched::UnmatchedHandler, utils::csv::{CsvReader, CsvWriter}};

// BUG: Example 5 can 'work' if grouping by 'Ref' not 'REF' - but ref doesn't exist so it's just 1 bug group!
// BUG: Derived data not in the debug output during the derived phase - but it IS in the first grouping phase!
// TODO: Option in charter (effects celerity and jetwash) to NOT archive data. Enable in the big data examples
// TODO: Change generator to group by ref not date. It's not a good example to set....
// TODO: Opt-out of archiving for both JW and Cel.

///
/// These are the linear state transitions of a match Job.
///
/// Any error encountered will suspend the job at that phase. It should be safe to start a new job assuming
/// amendments are made to the charter to correct any error, or changesets are aborted.
///
#[derive(Clone, Copy, Debug)]
pub enum Phase {
     FolderInitialisation,
    //  SourceData,
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
            // Phase::SourceData           => 2,
            Phase::ApplyChangeSets      => 2,
            Phase::DeriveSchema         => 3,
            Phase::DeriveData           => 4,
            Phase::MatchAndGroup        => 5,
            Phase::ComleteAndArchive    => 6,
            Phase::Complete             => 7,
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
    base_dir: PathBuf,     // The root of the working folder for data (see the folders module).
    timestamp: String,     // A unique timestamp to prefix any generated files with for this job.
    lua: rlua::Lua,        // Lua engine state.
    phase: Cell<Phase>,    // The current point in the linear state transition of the job.
}

impl Context {
    pub fn new(charter: Charter, charter_path: PathBuf, base_dir: PathBuf) -> Self {
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

    pub fn base_dir(&self) -> &PathBuf {
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
pub fn run_charter<P: AsRef<Path>>(charter: P, base_dir: P) -> Result<()> {

    let ctx = init_job(charter, base_dir)?;

    ctx.set_phase(Phase::FolderInitialisation);
    init_folders(&ctx)?;

    // ctx.set_phase(Phase::SourceData);
    // let grid = Grid::load(&ctx)?; // TODO: This phase should come out. changesets can remove files before it should be loaded.

    ctx.set_phase(Phase::ApplyChangeSets);
    let (mut grid, changesets) = apply_changesets(&ctx/* , grid */)?;

    ctx.set_phase(Phase::DeriveSchema);
    let (projection_cols, writers) = create_derived_schema(&ctx, &mut grid)?;

    ctx.set_phase(Phase::DeriveData);
    derive_data(&ctx, &grid, projection_cols, writers)?;

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
fn init_job<P: AsRef<Path>>(charter: P, base_dir: P) -> Result<Context, MatcherError> {
    let charter_pb = charter.as_ref().to_path_buf().canonicalize().with_context(|| format!("charter path {:?}", charter.as_ref()))?;
    let base_dir_pb = base_dir.as_ref().to_path_buf().canonicalize().with_context(|| format!("base dir {:?}", base_dir.as_ref()))?;
    let ctx = Context::new(Charter::load(&charter_pb)?, charter_pb, base_dir_pb);

    log::info!("Starting match job:");
    log::info!("    Job ID: {}", ctx.job_id());
    log::info!("   Charter: {} (v{})", ctx.charter().name(), ctx.charter().version());
    log::info!("  Base dir: {}", ctx.base_dir().to_canoncial_string());

    Ok(ctx)
}

///
/// Prepare the working folders before loading data.
///
fn init_folders(ctx: &Context) -> Result<(), MatcherError> {
    folders::ensure_dirs_exist(ctx)?;

    // On start-up, any matching files should log warning and be moved to waiting.
    folders::rollback_any_incomplete(ctx)?;

    // Move any waiting files to the matching folder.
    folders::progress_to_matching(ctx)?;

    Ok(())
}

///
/// Load and apply changesets to transform new and unmatched data prior to matching.
///
/// If data is modified, we re-load/index the records in a new instance of the grid.
///
fn apply_changesets(ctx: &Context) -> Result<(Grid, Vec<ChangeSet>), MatcherError> {

    let changesets = changeset::apply(ctx)?;
    return Ok((Grid::load(ctx)?, changesets))
}

///
/// Add a derived column for each projection or merger and calculate which columns each projection
/// is dependant on.
///
fn create_derived_schema(ctx: &Context, grid: &mut Grid) -> Result<(HashMap<usize, Vec<Column>>, CsvWriters), MatcherError> {

    // Debug the grid before the new columns are added.
    grid.debug_grid(ctx, 1);

    let mut projection_cols = HashMap::new();

    for (idx, inst) in ctx.charter().instructions().iter().enumerate() {
        let schema = grid.schema().clone();
        match inst {
            Instruction::Project { column, as_a, from, when } => {
                projection_cols.insert(idx, referenced_cols(from, when.as_ref().map(String::as_ref), &schema));
                grid.schema_mut().add_projected_column(Column::new(column.into(), None, *as_a))?;
            },
            Instruction::Merge { into, columns } => {
                let data_type = merge_col::validate(columns, grid)?;
                grid.schema_mut().add_merged_column(Column::new(into.into(), None, data_type))?;
            },
            _ => { /* Ignore other instructions. */}
        }
    }

    // Now we know what columns are derived, write their headers to the .derived files.
    let mut writers = derived_writers(grid);
    write_derived_headers(grid.schema(), &mut writers)?;

    // Debug the grid after the columns are added (but before values are derived).
    grid.debug_grid(ctx, 2);

    Ok((projection_cols, writers))
}

///
/// Create a csv::Writer<File> for every sourced data file - it should point to the derived csv file.
///
fn derived_writers(grid: &Grid) -> CsvWriters {
    grid.schema()
        .files()
        .iter()
        .map(|f| utils::csv::writer(f.derived_path()))
        .collect::<CsvWriters>()
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

///
/// Calculate all projected and derived columns and write them to a .derived file per sourced
/// file. Every corresponding row in the source files will have a row in the derived files which contains
/// projected and merged column data.
///
/// This implementation uses rayon to create a thread per file.
///
fn derive_data(ctx: &Context, grid: &Grid, projection_cols: HashMap<usize, Vec<Column>>, writers: CsvWriters)
    -> Result<(), MatcherError> {

    log::info!("Deriving projected and merged data");

    type Metrics = HashMap<usize, Duration>; // Accumulated duration per instruction.

    // We need one thread per file.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(std::cmp::min(grid.schema().files().len(), num_cpus::get()))
        .build()
        .expect("can't build rayon thread pool");

    // Create a data reader per sourced file. Skip the schema rows.
    let readers: Vec<CsvReader> = grid.schema()
        .files()
        .iter()
        .map(|file| utils::csv::reader(file.path(), true))
        .collect();

    let mut zipped: Vec<(CsvReader, CsvWriter)> = readers.into_iter().zip(writers).collect();

    // Wrap the schema and charter in arcs to share amongst threads.
    let schema = Arc::new(grid.schema().clone());
    let charter = Arc::new(ctx.charter());
    let lookup_path = folders::lookups(ctx);

    pool.install::<_, Result<(), MatcherError>>(|| {
        // Derive each file in a parallel iterator.
        let results = zipped
            .par_iter_mut()
            .enumerate()
            .map(|(file_idx, (reader, writer))| {
                derive_file(file_idx, reader, writer, schema.clone(), charter.clone(), projection_cols.clone(), &lookup_path)
            })
            .collect::<Result<Vec<Metrics>, MatcherError>>()?
            .into_iter() // Result IntoIterator takes the R not the E
            .collect::<Vec<Metrics>>();

        // Accumulate all of the time spent per instruction across all derived files.
        let mut total_metrics = Metrics::new();
        results.into_iter()
            .for_each(|metric| merge_metrics(metric, &mut total_metrics));

        // Report the duration spent performing each projection and merge instruction.
        for idx in total_metrics.keys().sorted_by(Ord::cmp) {
            let (duration, rate) = formatted_duration_rate(grid.len(), *total_metrics.get(idx).expect("Duration metric missing"));

            match &charter.instructions()[*idx] {
                Instruction::Project { column, .. } => log::info!("Projecting Column {} took {} ({}/row)", column, blue(&duration), rate),
                Instruction::Merge { into, .. } => log::info!("Merging Column {} took {} ({}/row)", into, blue(&duration), rate),
                _ => {},
            }
        }

        Ok(())
    })?;

    // Debug the derived data now.
    grid.debug_grid(ctx, 1);

    Ok(())
}

fn merge_metrics(merge: HashMap<usize, Duration>, into: &mut HashMap<usize, Duration>) {
    for (km, vm) in merge {
        *into.entry(km).or_insert(Duration::ZERO) += vm;
    }
}

///
/// Derive all the data in a single file.
///
fn derive_file(
    file_idx: usize,
    reader: &mut CsvReader,
    writer: &mut CsvWriter,
    schema: Arc<GridSchema>,
    charter: Arc<&Charter>,
    avail_cols: HashMap<usize, Vec<Column>>,
    lookup_path: &Path) -> Result<HashMap<usize, Duration>, MatcherError> {

    // Track accumulated time in each project and merge instruction.
    let mut metrics: HashMap<usize, Duration> = HashMap::new();

    // Track the record and instruction being processed. Used in logs should an error occur.
    let mut eval_ctx = (file_idx /* file */, 0 /* row */, 0 /* instruction */);

    let lua = rlua::Lua::new();

    lua.context(|lua_ctx| {
        init_context(&lua_ctx, charter.global_lua(), lookup_path)?;
        for csv_record in reader.byte_records() {
            let mut record = Record::new(file_idx, schema.clone(), csv_record?, csv::ByteRecord::new());

            for (i_idx, inst) in charter.instructions().iter().enumerate() {
                let started = Instant::now();
                eval_ctx = (file_idx, record.row(), i_idx);

                match inst {
                    Instruction::Project { column: _, as_a, from, when } => {
                        let avail_cols = avail_cols.get(&i_idx).ok_or(MatcherError::MissingScriptCols { instruction: i_idx })?;
                        project_column(*as_a, from, &when, &mut record, avail_cols, &lua_ctx)?;
                        record_duration(i_idx, &mut metrics, started.elapsed());
                    },

                    Instruction::Merge { into: _, columns } => {
                        record.merge_col_from(columns)?;
                        record_duration(i_idx, &mut metrics, started.elapsed());
                    },

                    _ => {}, // Ignore other instructions in this phase.
                };
            }

            // Flush the current record's buffer to the appropriate derived file.
            writer.write_byte_record(&record.flush()).map_err(MatcherError::CSVError)?;
        }

        Ok(metrics)

    }).map_err(|err: MatcherError| MatcherError::DeriveDataError {
        instruction: format!("{:?}", charter.instructions()[eval_ctx.2]),
        row: eval_ctx.1,
        file: schema.files()[eval_ctx.0].filename().into(),
        err: err.to_string()
    })
}

///
/// Set the initial or increment the existing duration for the specified charter instruction.
///
fn record_duration(instruction: usize, metrics: &mut HashMap<usize, Duration>, elapsed: Duration) {
    // Ensure there's an entry.
    metrics.entry(instruction).or_insert(Duration::ZERO);

    // Now add the ellapsed to it.
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

    // Debug the grid after each group instruction.
    grid.debug_grid(ctx, 0);

    for (idx, inst) in ctx.charter().instructions().iter().enumerate() {
        if let Instruction::Group { by, match_when } = inst {
            matching::match_groups(
                ctx,
                by,
                match_when,
                grid,
                &mut matched)?;

            // Debug the grid after each group instruction.
            grid.debug_grid(ctx, idx);
        }
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

    // Write all unmatched records now.
    unmatched.write_records(ctx, &grid)?;

    let duration = ctx.started().elapsed();

    // Complete the matched JSON file.
    matched.complete_files(&unmatched, changesets, duration)?;

    // Debug the final grid now.
    grid.debug_grid(ctx, 1);

    // Move matching files to the archive.
    folders::progress_to_archive(ctx, grid)?;

    // Log a warning for any file left in matching at the end of a job.
    let left_overs = folders::matching(ctx).read_dir()?
        .map(|entry| entry.expect("unable to read matching file").file_name().to_str().unwrap_or("no-name").to_string())
        .join("\n");

    if !left_overs.is_empty() {
        log::warn!("The following files were still in the matching folder at the end of the job:\n{}", left_overs);
    }

    log::info!("Completed match job {} in {}", ctx.job_id(), blue(&formatted_duration_rate(1, duration).0));

    Ok(())
}
