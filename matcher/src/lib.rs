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
use std::{time::{Duration, Instant}, collections::HashMap};
use crate::{model::{charter::{Charter, Instruction}, grid::Grid, schema::Column}, instructions::{project_col::{project_column, script_cols}, merge_col}, matched::MatchedHandler, unmatched::UnmatchedHandler, data_accessor::DataAccessor};

// TODO: Ensure all lua script errors log the script in the message.
// TODO: Re-instate accumulated timings for projections and mergers.
// TODO: Debug per instruction. Currently all derived are debugged at once.
// TODO: Changesets
// TODO: Rollback commands.
// TODO: Flesh-out examples.
// TODO: Unit/integration tests. Lots.
// TODO: Check code coverage.
// TODO: Remove panics! and unwraps / expects where possible.
// TODO: Clippy!
// TODO: Thread-per source file for projects and merges.
// TODO: Investigate sled for disk based groupings. Seems I'm not a pioneer :( https://en.wikipedia.org/wiki/External_sorting
// TODO: Journal file - event log.
// TODO: Jetwash to generate changesets for update files (via business key).
// TODO: Consider an 'abort' changeset to cancel an erroneous/stuck changeset (maybe it has a syntx error). This would avoid manual tampering.

/*
  - Changesets - only unmatched data is effected.
  - Direct manual instruction.
    - Update record 123, set amount = 100.00 and date = 1/1/2020
    - WONT DO THIS Update records where date = 1/1/2020, set amount = amount * 100
    - WONT DO THIS Update records in source file xxxxxx_inv.csv, yyyyyyy_inv.csv  change amount to decimal.
    * Delete records where xxxx.

    [
        {
            "id": "uuid_1",
            "change": "Data", <<< discriminator
            "where": "record[uuid] = xxx-yyy-zzzz",
            "changes": [
                { "Field": "amount", "Value": "100.00" },
                { "Field": "date", "Value": "2020-01-01T00:00:00.000Z" }
            ],
            "source": "user_abc",
            "approved_by": ["user_abc", "user_xyz"],
            "timestamp": 123456789
        },
        {
            "id": "uuid_2",
            "change": "Data", <<< discriminator
            "where": "record[date] = 1577836800000",
            "changes": [
                { "Script": "record[amount] = record[amount] * 100" }
            ],
            "source": "user_xyz",
            "approved_by": ["user_abc", "user_xyz"],
            "timestamp": 123456789
        },
        {
            "id": "uuid_3",
            "change": "Schema", <<< discriminator
            "in": [ "xxxxxx_inv.csv", "yyyyyyy_inv.csv" ],
            "changes": [
                { Column: "Amount", type: "Decimal" }
            ],
            "source": "user_123",
            "approved_by": ["user_abc", "user_xyz"],
            "timestamp": 123456789
        },
        {
            "id": "uuid_4",
            "change": "delete", <<< discriminator
            "where": "record[uuid] = xxx-yyy-zzzz-wwww",
            "source": "user_xyz",
            "approved_by": ["user_abc", "user_xyz"],
            "timestamp": 123456789
        },
    ]

    - Record changesets in the matched json files.
    - Changesets are archived at end of a job they are used in.
    - Changesets look file YYYYMMDD_HHMMSSMMM_changeset.json
    - Changesets only apply to unmatched data in their current job with a ts earlier than the changeset's ts.
*/

///
/// Created for each match job. Used to pass the main top-level things around.
///
pub struct Context {
    job_id: Uuid,
    charter: Charter,
    base_dir: String,
    timestamp: String,
}

impl Context {
    pub fn new(charter: Charter, base_dir: String) -> Self {
        Self {
            job_id: Uuid::new_v4(),
            charter,
            base_dir,
            timestamp: folders::new_timestamp(),
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

    pub fn ts(&self) -> &str {
        &self.timestamp
    }
}

// TODO: Make these parameters consistent.
pub fn run_charter(charter: &str, base_dir: String) -> Result<()> {
    log::info!("{}", BANNER);

    let start = Instant::now();
    let ctx = Context::new(Charter::load(charter)?, base_dir);
    log::info!("Starting match job {}", ctx.job_id());

    folders::ensure_dirs_exist(&ctx)?;

    // TODO: Ensure nothing in waiting folder is already in the archive folder.

    // On start-up, any matching files should log warning and be moved to waiting.
    // TODO: Delete any modified unmatched files (if there's an associated .bak file)
    // TODO: rename any unmatched.bak to remove the .bak suffix.
    folders::rollback_any_incomplete(&ctx)?;

    // Move any waiting files to the matching folder.
    folders::progress_to_matching(&ctx)?;

    // Iterate alphabetically matching files.
    let grid = process_charter(&ctx)?;

    // Move matching files to the archive.
    // TODO: Delete unmatched.bak files if there has been no error.
    folders::progress_to_archive(&ctx, grid)?;

    // TODO: Log how many records processed, rate, MB size, etc.
    log::info!("Completed match job {} in {}", ctx.job_id(), blue(&formatted_duration_rate(1, start.elapsed()).0));

    Ok(())
}

///
/// Process the matching instructions.
///
fn process_charter(ctx: &Context) -> Result<Grid, MatcherError> {

    log::info!("Running charter [{}] v{:?}", ctx.charter().name(), ctx.charter().version());

    // Create Lua engine bindings.
    let lua = rlua::Lua::new();

    // Load all data into memory (for now).
    let mut grid = Grid::default();
    grid.source_data(ctx)?; // TODO: Wrap these two lines into one.

    // Load and apply changesets to transform new and unmatched data prior to matching.
    let (any_applied, _changsets) = changeset::apply(ctx, &mut grid, &lua)?;

    if any_applied {
        // Re-source the grid now, as we'll need to re-index record positions.
        grid = Grid::default();
        grid.source_data(ctx)?;
    }

    // Create a DataAccessor now to use through the first two instruction passes. It will run in write mode
    // meaning it will be writing derived values to a buffer for each record and flushing to disk.
    let mut accessor = DataAccessor::for_deriving(&grid)?;

    // If charter.debug - dump the grid with instr idx in filename.
    if ctx.charter().debug() {
        grid.debug_grid(ctx, &format!("0_{}.output.csv", ctx.ts()), &mut accessor);
    }

    // Pass 1 - calculate for each instruction, the Lua columns needed and added derived columns to the grid.
    let projection_cols = pass_1_derived_schema(ctx, &mut grid)?;

    // println!("Pass 1 complete - Sleeping for 8...");
    // std::thread::sleep(std::time::Duration::from_secs(8));

    // Pass 2 - calculate all projected and derived columns and write them to a .derived file per sourced
    // file. Every corresponding row in the source files will have a row in the derived files which contains
    // projected and merged column data.
    pass_2_derived_data(ctx, &lua, &mut grid, &mut accessor, projection_cols)?;

    // println!("Pass 2 complete - Sleeping for 8...");
    // std::thread::sleep(std::time::Duration::from_secs(8));

    // Pass 3 - run all other instructions that don't create derived data.
    // Create a new accessor which can read from our persisted .derived files.
    pass_3_match_data(ctx, &lua, &mut grid)?;

    // println!("Pass 3 complete - Sleeping for 8...");
    // std::thread::sleep(std::time::Duration::from_secs(8));

    Ok(grid)
}

///
/// Add a derived column for each projection or merger and calculate which columns each projection
/// is dependant on.
///
fn pass_1_derived_schema(ctx: &Context, grid: &mut Grid) -> Result<HashMap<usize, Vec<Column>>, MatcherError> {

    let mut projection_cols = HashMap::new();

    // Because both grid and accessor need to be borrowed mutablly, we'll copy an immutable schema
    // to pass around.
    let schema = grid.schema().clone();

    for (idx, inst) in ctx.charter().instructions().iter().enumerate() {
        match inst {
            Instruction::Project { column, as_type, from, when } => {
                projection_cols.insert(idx, script_cols(from, when.as_ref().map(String::as_ref), &schema));
                grid.schema_mut().add_projected_column(Column::new(column.into(), None, *as_type))?;
            },
            Instruction::MergeColumns { into, from } => {
                let data_type = merge_col::validate(from, grid)?;
                grid.schema_mut().add_merged_column(Column::new(into.into(), None, data_type))?;
            },
            _ => { /* Ignore other instructions. */}
        }
    }
    Ok(projection_cols)
}

///
/// Calculate all projected and derived columns and write them to a .derived file per sourced
/// file. Every corresponding row in the source files will have a row in the derived files which contains
/// projected and merged column data.
///
fn pass_2_derived_data(ctx: &Context, lua: &rlua::Lua, grid: &mut Grid, accessor: &mut DataAccessor, projection_cols: HashMap<usize, Vec<Column>>) -> Result<(), MatcherError> {

    // Ensure the accessor's schema is sync'd with the modified grid's schema.
    accessor.set_schema(grid.schema().clone());

    // Now we know what columns are derived, write their headers to the .derived files.
    accessor.write_derived_headers()?;

    // Track the record and instruction being processed.
    let mut eval_ctx = (0, 0);

    lua.context(|lua_ctx| {
        lua::init_context(&lua_ctx)?;

        // Calculate all projected and merged column values for each record.
        for (r_idx, record) in grid.records().iter().enumerate() {
            for (i_idx, inst) in ctx.charter().instructions().iter().enumerate() {
                eval_ctx = (r_idx, i_idx);

                match inst {
                    Instruction::Project { column: _, as_type, from, when } => {
                        let script_cols = projection_cols.get(&i_idx)
                            .ok_or(MatcherError::MissingScriptCols { instruction: i_idx })?;

                        project_column(
                            *as_type,
                            from,
                            when,
                            record,
                            accessor,
                            script_cols,
                            &lua_ctx)?;
                    },

                    Instruction::MergeColumns { into: _, from } => {
                        record.merge_col_from(from, accessor)?;
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

    Ok(())
}

///
/// Run all other instructions that don't create derived data. Create a new accessor which
/// can read from our persisted .derived files.
///
fn pass_3_match_data(ctx: &Context, lua: &rlua::Lua, grid: &mut Grid) -> Result<(), MatcherError> {

    // Create a match file containing job details and giving us a place to append match results.
    let mut matched = MatchedHandler::new(ctx, grid)?;

    // Create unmatched files for each sourced file.
    let mut unmatched = UnmatchedHandler::new(ctx, grid)?;

    // Create a read-mode derived accessor used to read real and derived data.
    let mut accessor = DataAccessor::for_reading(grid)?;
    let schema = grid.schema().clone();

    for (idx, inst) in ctx.charter().instructions().iter().enumerate() {
        match inst {
            Instruction::Project { .. } => {},
            Instruction::MergeColumns { .. } => {},
            Instruction::MatchGroups { group_by, constraints } => instructions::match_groups::match_groups(group_by, constraints, grid, &schema, &mut accessor, lua, &mut matched)?,
            Instruction::_Filter   => todo!(),
            Instruction::_UnFilter => todo!(),
        };

        // If charter.debug - dump the grid with instr idx in filename.
        if ctx.charter().debug() {
            grid.debug_grid(ctx, &format!("{}_{}.output.csv", idx + 1, ctx.ts()), &mut accessor);
        }

        log::info!("Grid Memory Size: {}",
            blue(&format!("{:.0}", grid.memory_usage().bytes())));
    }

    // Complete the matched JSON file.
    matched.complete_files()?;

    // Write all unmatched records now - this will be optimised at a later stage to be a single call.
    unmatched.write_records(ctx, grid.records(), grid)?;

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
  ____     _           _ _
 / ___|___| | ___ _ __(_) |_ _   _
| |   / _ \ |/ _ \ '__| | __| | | |
| |__|  __/ |  __/ |  | | |_| |_| |
 \____\___|_|\___|_|  |_|\__|\__, |
 OpenRec: Matching Engine    |___/
"#;
