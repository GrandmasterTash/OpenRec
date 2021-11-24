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

use std::time::Instant;

use uuid::Uuid;
use anyhow::Result;
use ubyte::ToByteUnit;
use error::MatcherError;
use crate::{charter::{Charter, Constraint, Instruction, formatted_duration_rate}, data_type::DataType, folders::ToCanoncialString, grid::Grid, instructions::merge_col::merge_cols, instructions::project_col::project_column, instructions::source_data::source_data};

// TODO: Unit tests. Lots.
// TODO: Refactor to work with data streamed from files.

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    log::info!("{}", BANNER);

    // Build a charter model to match our three files with.
    let charter = Charter::new("test invoices".into(), false, "EUR".into(), chrono::Utc::now().timestamp_millis() as u64, vec!(
        Instruction::SourceData { filename: ".*invoices\\.csv".into() },
        Instruction::SourceData { filename: ".*payments\\.csv".into() },
        Instruction::SourceData { filename: ".*receipts\\.csv".into() },
        Instruction::ProjectColumn { name: "PAYMENT_AMOUNT_BASE".into(), data_type: DataType::DECIMAL, eval: r#"record["payments.Amount"] * record["payments.FXRate"]"#.into(), when: r#"meta["prefix"] == "payments""#.into() },
        Instruction::ProjectColumn { name: "RECEIPT_AMOUNT_BASE".into(), data_type: DataType::DECIMAL, eval: r#"record["receipts.Amount"] * record["receipts.FXRate"]"#.into(), when: r#"meta["prefix"] == "receipts""#.into() },
        Instruction::ProjectColumn { name: "TOTAL_AMOUNT_BASE".into(),   data_type: DataType::DECIMAL, eval: r#"record["invoices.TotalAmount"] * record["invoices.FXRate"]"#.into(), when: r#"meta["prefix"] == "invoices""#.into() },
        Instruction::MergeColumns { name: "SETTLEMENT_DATE".into(), source: vec!("invoices.SettlementDate".into(), "payments.PaymentDate".into(), "receipts.ReceiptDate".into() )},
        Instruction::MergeColumns { name: "AMOUNT_BASE".into(), source: vec!("PAYMENT_AMOUNT_BASE".into(), "RECEIPT_AMOUNT_BASE".into(), "TOTAL_AMOUNT_BASE".into() )},
        Instruction::MergeColumns { name: "DBG_REF".into(), source: vec!("invoices.Reference".into(), "payments.Reference".into(), "receipts.Reference".into() ) },
        Instruction::MatchGroups { group_by: vec!("SETTLEMENT_DATE".into()), constraints: vec!(
                Constraint::NetsToZero { column: "AMOUNT_BASE".into(), lhs: r#"meta["prefix"] == 'payments'"#.into(), rhs: r#"meta["prefix"] == 'invoices'"#.into(), debug: true },
                Constraint::NetsToZero { column: "AMOUNT_BASE".into(), lhs: r#"meta["prefix"] == 'receipts'"#.into(), rhs: r#"meta["prefix"] == 'invoices'"#.into(), debug: true },
            )
        },
    ));

    let start = Instant::now();
    let job_id = Uuid::new_v4();
    log::info!("Starting match job {}", job_id);

    folders::ensure_exist()?;

    // On start-up, any matching files should log warning and be moved to waiting.
    folders::rollback_incomplete()?;

    // Move any waiting files to the matching folder.
    folders::progress_to_matching()?;

    // Iterate alphabetically matching files.
    process_charter(&charter, job_id)?;

    // TODO: Finalise matched and unmatched. (remove .inprogress suffix).
    folders::progress_to_archive()?;

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

    log::info!("Running charter [{}] v{:?} using BASE [{}]",
        charter.name(),
        charter.version(),
        charter.base_currency());

    for inst in charter.instructions() {
        // BUG: Skip instructions if the grid is empty.
        match inst {
            Instruction::SourceData { filename } => source_data(filename, &mut grid)?,
            Instruction::ProjectColumn { name, data_type, eval, when } => project_column(name, *data_type, eval, when, &mut grid, &lua)?,
            Instruction::MergeColumns { name, source } => merge_cols(name, source, &mut grid)?,
            Instruction::MatchGroups { group_by, constraints } => instructions::match_groups::match_groups(group_by, constraints, &mut grid, &lua, job_id, charter)?,
            Instruction::_Filter   => todo!(),
            Instruction::_UnFilter => todo!(),
        };

        log::info!("Grid Memory Size: {}",
            ansi_term::Colour::RGB(70, 130, 180).paint(format!("{:.0}", grid.memory_usage().bytes())));
    }

    dump_grid(&grid);


    Ok(())
}

fn dump_grid(grid: &Grid) {
    // TODO: BufWriter
    // Output a new result csv file.
    let output_path = std::path::Path::new("./tmp/output.csv");
    let mut wtr = csv::WriterBuilder::new().quote_style(csv::QuoteStyle::Always).from_path(output_path).unwrap();
    wtr.write_record(grid.schema().headers()).unwrap();

    for record in grid.records() {
        let data: Vec<&[u8]> = grid.record_data(record)
            .iter()
            .map(|v| v.unwrap_or(b""))
            .collect();
        wtr.write_byte_record(&data.into()).unwrap();
    }

    wtr.flush().unwrap();
    log::info!("{} rows written to {}", grid.records().len(), output_path.to_canoncial_string());
}


const BANNER: &str = r#"
  ___                   ____  _____ ____
 / _ \ _ __   ___ _ __ |  _ \| ____/ ___|
| | | | '_ \ / _ \ '_ \| |_) |  _|| |
| |_| | |_) |  __/ | | |  _ <| |__| |___
 \___/| .__/ \___|_| |_|_| \_\_____\____|
      |_|
"#;