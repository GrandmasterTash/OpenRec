mod lua;
mod error;
mod model;
mod convert;
mod folders;
mod matched;
mod unmatched;
mod instructions;

use uuid::Uuid;
use anyhow::Result;
use ubyte::ToByteUnit;
use error::MatcherError;
use std::{time::{Duration, Instant}, collections::HashMap};
use crate::{model::{charter::{Charter, Instruction}, grid::Grid, data_accessor::DataAccessor, schema::Column}, instructions::merge_col::merge_cols, instructions::{project_col::{/* project_column, */ project_column_new, script_cols}, merge_col}, matched::MatchedHandler, unmatched::UnmatchedHandler};

// TODO: Alter source_data to only retain columns required for matching.
//   Also - consider memory compaction, string table, etc.
//   After memory compaction appears any projection (even bools) add signigicant overhead.
//   Will look at 2021-12-05_064000000_invoices.derived.csv files to store projections and merged columns.
//   Writing new file very fast. Suggest new strategy. Write all projected and merged columns to .derived file.
//   Record should source from that file instead. CREATE A FILE ACCESSOR type FIRST!!!!!!!!!!!!!!!
//   Suggest derived vec used during all projections and mergers, then flushed to disk and dropped.


// TODO: Flesh-out examples.
// TODO: Unit/integration tests. Lots.
// TODO: Check code coverage.
// TODO: Clippy!
// TODO: Investigate sled for disk based groupings...

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

// TODO: Make these parameters consistent.
pub fn run_charter(charter: &str, base_dir: String) -> Result<()> {
    log::info!("{}", BANNER);

    let start = Instant::now();
    let ctx = Context::new(Charter::load(charter)?, base_dir);
    log::info!("Starting match job {}", ctx.job_id());

    folders::ensure_exist(&ctx)?;

    // TODO: Ensure nothing in waiting folder is already in the archive folder.

    // On start-up, any matching files should log warning and be moved to waiting.
    folders::rollback_incomplete(&ctx)?;

    // Move any waiting files to the matching folder.
    folders::progress_to_matching(&ctx)?;

    // Experiment....
    // let rows = 1000000;
    // let path = folders::matching(&ctx).join("write_test.csv");

    // println!("Writing {} records to CSV {:?}", rows, path.to_str());
    // let mut writer = csv::WriterBuilder::new().quote_style(csv::QuoteStyle::Always).from_path(path).unwrap();

    // for idx in 1..1000 {
    //     let mut record = csv::ByteRecord::new();
    //     record.push_field(&Decimal::ONE_THOUSAND.serialize());
    //     record.push_field(format!("REF{}", idx).as_bytes());
    //     writer.write_record(&record).unwrap();
    // }

    // writer.flush().unwrap();

    // println!("Sleeping for 8...");
    // std::thread::sleep(std::time::Duration::from_secs(8));

    // Iterate alphabetically matching files.
    process_charter(&ctx)?;

    // Move matching files to the archive.
    // BUG: ONLY progress processed files by the charter, not everything in the waiting folder.
    folders::progress_to_archive(&ctx)?;

    // TODO: Log how many records processed, rate, MB size, etc.
    log::info!("Completed match job {} in {}", ctx.job_id(), blue(&formatted_duration_rate(1, start.elapsed()).0));

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

    // Create a DataAccessor now to use through both instruction passes.
    let mut accessor = DataAccessor::with_buffer(&grid);

    // Because both grid and accessor need to be borrow mutablly, we'll copy an immutable schema to pass around.
    let schema = grid.schema().clone();

    // If charter.debug - dump the grid with instr idx in filename.
    if ctx.charter().debug() {
        grid.debug_grid(ctx, &format!("0_{}.output.csv", ts), &mut accessor);
    }

    // Pass 0 - calculate for each instruction, the Lua columns needed and added derived columns to the grid.
    let mut projection_cols = HashMap::new();
    for (idx, inst) in ctx.charter().instructions().iter().enumerate() {
        match inst {
            Instruction::Project { column, as_type, from, when } => {
                projection_cols.insert(idx, script_cols(from, when.as_ref().map(String::as_ref), &schema));

                grid.schema_mut().add_projected_column(Column::new(column.into(), None, *as_type))?;
            },
            Instruction::MergeColumns { into, from } => {
                let data_type = merge_col::validate(from, &mut grid)?;
                grid.schema_mut().add_merged_column(Column::new(into.into(), None, data_type))?;
            },
            _ => {}
        }
    }

    // println!("Pass 0 complete - Sleeping for 8...");
    // std::thread::sleep(std::time::Duration::from_secs(8));

    // The above phase will likely have modified the schema. Take new snapshots.
    // Because both grid and accessor need to be borrow mutablly, we'll copy an immutable schema to pass around.
    let schema = grid.schema().clone();
    accessor.set_schema(schema.clone());

    // Now we know what columns are derived, write their headers to the .derived files.
    accessor.write_derived_headers()?;

    // TODO: Pass 1 - calculate all projected and derived columns and write them to a .derived file per sourced
    // file. Every corresponding row in the source files will have a row in the derived files which contains 
    // projected and merged column data.
    lua.context(|lua_ctx| {
        // let globals = lua_ctx.globals();
        // TODO: Put column headers in the derived files.
        for record in grid.records() {
            for (idx, inst) in ctx.charter().instructions().iter().enumerate() {
                match inst {
                    Instruction::Project { column, as_type, from, when } => {
                        project_column_new(
                            column,
                            *as_type,
                            from,
                            when.as_ref().map(String::as_ref), // TODO: De-ugly this (and above).
                            &record,
                            &mut accessor,
                            projection_cols.get(&idx).unwrap(),
                            &lua_ctx/* ,
                            &globals */).unwrap(); // TODO: Don't unwrap.
                    },
                    Instruction::MergeColumns { into, from } => {
                        merge_cols(into, from, &record, &mut accessor).unwrap(); // TODO: Don't unwrap.
                    },
                    _ => {},
                };
            }

            // Flush the current record buffer to the appropriate derived file.
            record.flush(&mut accessor).unwrap(); // TODO: Don't unwrap.
        }
    });

    // println!("Pass 1 complete - Sleeping for 8...");
    // std::thread::sleep(std::time::Duration::from_secs(8));

    // Pass 2 - run all other instructions that don't create derived data.
    // Create a new accessor which can read from our persisted .derived files.
    let mut accessor = DataAccessor::with_no_buffer(&mut grid);

    // println!("Sleeping for 8...");
    // std::thread::sleep(std::time::Duration::from_secs(8));

    for (idx, inst) in ctx.charter().instructions().iter().enumerate() {
        match inst {
            // Instruction::Project { column, as_type, from, when } => project_column(column, *as_type, from, when.as_ref().map(String::as_ref), &mut grid, &mut accessor, &lua)?,
            Instruction::Project { .. } => {},
            // Instruction::MergeColumns { into, from } => merge_cols(into, from, &mut grid)?,
            Instruction::MergeColumns { .. } => {},
            // Instruction::MatchGroups { group_by, constraints } => instructions::match_groups::match_groups(group_by, constraints, &mut grid, &lua, &mut matched)?,
            Instruction::MatchGroups { group_by, constraints } => instructions::match_groups::match_groups(group_by, constraints, &mut grid, &schema, &mut accessor, &lua, &mut matched)?,
            Instruction::_Filter   => todo!(),
            Instruction::_UnFilter => todo!(),
        };

        // If charter.debug - dump the grid with instr idx in filename.
        if ctx.charter().debug() {
            grid.debug_grid(ctx, &format!("{}_{}.output.csv", idx + 1, ts), &mut accessor);
        }

        log::info!("Grid Memory Size: {}",
            blue(&format!("{:.0}", grid.memory_usage().bytes())));
    }

    // Complete the matched JSON file.
    matched.complete_files()?;

    // Write all unmatched records now - this will be optimised at a later stage to be a single call.
    unmatched.write_records(grid.records(), &grid)?;
    unmatched.complete_files(ctx)?;

    // println!("Pass 2 complete - Sleeping for 8...");
    // std::thread::sleep(std::time::Duration::from_secs(8));

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

const BANNER: &str = r#"
  ___                   ____  _____ ____
 / _ \ _ __   ___ _ __ |  _ \| ____/ ___|
| | | | '_ \ / _ \ '_ \| |_) |  _|| |
| |_| | |_) |  __/ | | |  _ <| |__| |___
 \___/| .__/ \___|_| |_|_| \_\_____\____|
      |_|
"#;