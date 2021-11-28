mod lua;
mod grid;
mod error;
mod schema;
mod record;
mod folders;
mod charter;
mod datafile;
mod data_type;
mod instructions;

use uuid::Uuid;
use anyhow::Result;
use ubyte::ToByteUnit;
use error::MatcherError;
use std::time::{Duration, Instant};
use crate::{charter::{Charter, Instruction}, grid::Grid, instructions::merge_col::merge_cols, instructions::project_col::project_column, instructions::source_data::source_data};

// TODO: Create a 2-stage match charter and example data files and implement stages.
// TODO: Alter source_data to only retain columns required for matching. This will mean unmatched data will be written in a different way.
// TODO: Unit tests. Lots.

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    log::info!("{}", BANNER);

    // TODO: Clap interface and a lib interface.
    // let charter = Charter::load("../examples/3-way-match.yaml")?;
    let charter = Charter::load("../examples/2-stage.yaml")?;

    // TODO: move to run_match_job()
    let start = Instant::now();
    let job_id = Uuid::new_v4();
    log::info!("Starting match job {}", job_id);

    folders::ensure_exist(charter.debug())?;

    // On start-up, any matching files should log warning and be moved to waiting.
    folders::rollback_incomplete()?;

    // Move any waiting files to the matching folder.
    folders::progress_to_matching()?;

    // Iterate alphabetically matching files.
    process_charter(&charter, job_id)?;

    // TODO: Finalise matched and unmatched. (remove .inprogress suffix).
    folders::progress_to_archive()?;

    // TODO: Log how many records processed, rate, MB size, etc.
    log::info!("Completed match job {} in {}", job_id, ansi_term::Colour::RGB(70, 130, 180).paint(formatted_duration_rate(1, start.elapsed()).0));

    Ok(())
}

///
/// Process the matching instructions.
///
fn process_charter(charter: &Charter, job_id: Uuid) -> Result<(), MatcherError> {
    //TODO: Move this to charter.rs

    // Load all data into memory (for now).
    let mut grid = Grid::new();

    // Create Lua engine bindings.
    let lua = rlua::Lua::new();

    log::info!("Running charter [{}] v{:?}",
        charter.name(),
        charter.version());

    for inst in charter.instructions() {
        // BUG: Skip instructions if the grid is empty.
        match inst {
            Instruction::SourceData { file_patterns, field_prefixes } => source_data(file_patterns, &mut grid, field_prefixes.unwrap_or(true))?,
            Instruction::Project { column, as_type, from, when } => project_column(column, *as_type, from, when, &mut grid, &lua)?,
            Instruction::MergeColumns { into, from } => merge_cols(into, from, &mut grid)?,
            Instruction::MatchGroups { group_by, constraints } => instructions::match_groups::match_groups(group_by, constraints, &mut grid, &lua, job_id, charter)?,
            Instruction::_Filter   => todo!(),
            Instruction::_UnFilter => todo!(),
        };

        log::info!("Grid Memory Size: {}",
            ansi_term::Colour::RGB(70, 130, 180).paint(format!("{:.0}", grid.memory_usage().bytes())));
    }

    // dump_grid(&grid);

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