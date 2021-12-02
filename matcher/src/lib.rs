mod lua;
mod grid;
mod error;
mod schema;
mod record;
mod folders;
mod matched;
mod charter;
mod datafile;
mod unmatched;
mod data_type;
mod instructions;

use uuid::Uuid;
use anyhow::Result;
use ubyte::ToByteUnit;
use error::MatcherError;
use std::time::{Duration, Instant};
use crate::{charter::{Charter, Instruction}, grid::Grid, instructions::merge_col::merge_cols, instructions::project_col::project_column, matched::MatchedHandler, unmatched::UnmatchedHandler};

// TODO: Change dates to use ISO8601 UTC format for clarity.
// TODO: Unit/integration tests. Lots.
// TODO: Check code coverage.
// TODO: Alter source_data to only retain columns required for matching.
//   This will mean unmatched data will be written in a different way.
//   Also - consider memory compaction, string table, etc.
// TODO: Flesh-out examples.

///
/// Created for each match job. Used to pass the main top-level things around.
///
pub struct Context {
    job_id: Uuid,
    charter: Charter,
    base_dir: String,
}

impl Context {
    pub fn new(charter: Charter, base_dir: String) -> Self {
        Self {
            job_id: Uuid::new_v4(),
            charter,
            base_dir,
        }
    }

    pub fn job_id(&self) -> &Uuid {
        &self.job_id
    }

    pub fn charter(&self) -> &Charter {
        &self.charter
    }

    pub fn base_dir(&self) -> &str {
        &self.base_dir
    }
}

pub fn run_charter(charter: &str, base_dir: String) -> Result<()> {
    log::info!("{}", BANNER);

    let start = Instant::now();
    let ctx = Context::new(Charter::load(charter)?, base_dir);
    log::info!("Starting match job {}", ctx.job_id());

    folders::ensure_exist(&ctx)?;

    // On start-up, any matching files should log warning and be moved to waiting.
    folders::rollback_incomplete(&ctx)?;

    // Move any waiting files to the matching folder.
    folders::progress_to_matching(&ctx)?;

    // Iterate alphabetically matching files.
    process_charter(&ctx)?;

    // Move matching files to the archive.
    // BUG: ONLY progress processed files by the charter, not everything in the waiting folder.
    folders::progress_to_archive(&ctx)?;

    // TODO: Log how many records processed, rate, MB size, etc.
    log::info!("Completed match job {} in {}", ctx.job_id(), ansi_term::Colour::RGB(70, 130, 180).paint(formatted_duration_rate(1, start.elapsed()).0));

    Ok(())
}

///
/// Process the matching instructions.
///
fn process_charter(ctx: &Context) -> Result<(), MatcherError> {

    log::info!("Running charter [{}] v{:?}",
        ctx.charter().name(),
        ctx.charter().version());

    let ts = folders::new_timestamp();

    // Load all data into memory (for now).
    let mut grid = Grid::new();

    // Create Lua engine bindings.
    let lua = rlua::Lua::new();

    // Source data now to build the grid schema.
    grid.source_data(ctx)?;

    // Create a match file containing job details and giving us a place to append match results.
    let mut matched = MatchedHandler::new(ctx, &grid)?;

    // Create unmatched files for each sourced file.
    let mut unmatched = UnmatchedHandler::new(ctx, &grid)?;

    // If charter.debug - dump the grid with instr idx in filename.
    if ctx.charter().debug() {
        grid.debug_grid(ctx, &format!("0_{}output.csv", ts));
    }

    for (idx, inst) in ctx.charter().instructions().iter().enumerate() {
        match inst {
            Instruction::Project { column, as_type, from, when } => project_column(column, *as_type, from, when.as_ref().map(String::as_ref), &mut grid, &lua)?,
            Instruction::MergeColumns { into, from } => merge_cols(into, from, &mut grid)?,
            Instruction::MatchGroups { group_by, constraints } => instructions::match_groups::match_groups(group_by, constraints, &mut grid, &lua, &mut matched)?,
            Instruction::_Filter   => todo!(),
            Instruction::_UnFilter => todo!(),
        };

        // If charter.debug - dump the grid with instr idx in filename.
        if ctx.charter().debug() {
            grid.debug_grid(ctx, &format!("{}_{}output.csv", idx + 1, ts));
        }

        log::info!("Grid Memory Size: {}",
            ansi_term::Colour::RGB(70, 130, 180).paint(format!("{:.0}", grid.memory_usage().bytes())));
    }

    // Complete the matched JSON file.
    matched.complete_files()?;

    // Write all unmatched records now - this will be optimised at a later stage to be a single call.
    unmatched.write_records(grid.records(), &grid)?;
    unmatched.complete_files(ctx)?;

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

const BANNER: &str = r#"
  ___                   ____  _____ ____
 / _ \ _ __   ___ _ __ |  _ \| ____/ ___|
| | | | '_ \ / _ \ '_ \| |_) |  _|| |
| |_| | |_) |  __/ | | |  _ <| |__| |___
 \___/| .__/ \___|_| |_|_| \_\_____\____|
      |_|
"#;