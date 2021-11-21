mod grid;
mod error;
mod schema;
mod record;
mod folders;
mod charter;
mod datafile;
mod data_type;
mod instructions;

use anyhow::Result;
use error::MatcherError;
use ubyte::ToByteUnit;
use crate::{charter::{Charter, Instruction}, data_type::DataType, folders::ToCanoncialString, grid::Grid};

// TODO: Refactor to work with data streamed from files.
// TODO: Consider rayon when we're streaming from files.

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
        Instruction::ProjectColumn { name: "RECEIPT_AMOUNT_BASE".into(), data_type: DataType::DECIMAL, eval: r#"record["receipts.Amount"]"#.into(), when: r#"meta["prefix"] == "receipts""#.into() },
        Instruction::ProjectColumn { name: "TOTAL_AMOUNT_BASE".into(),   data_type: DataType::DECIMAL, eval: r#"record["invoices.TotalAmount"] * record["invoices.FXRate"]"#.into(), when: r#"meta["prefix"] == "invoices""#.into() },
        Instruction::MergeColumns { name: "SETTLEMENT_DATE".into(), source: vec!("invoices.SettlementDate".into(), "payments.PaymentDate".into(), "receiptes.ReceiptDate".into() )},
        Instruction::MergeColumns { name: "AMOUNT_BASE".into(), source: vec!("PAYMENT_AMOUNT_BASE".into(), "RECEIPT_AMOUNT_BASE".into(), "TOTAL_AMOUNT_BASE".into() )},
        // Instruction::GROUP_BY { columns: vec!("SETTLEMENT_DATE".into()) },
        // Instruction::MATCH_GROUPS { constraints: vec!(
        //     Constraint::NETS_TO_ZERO { column: "AMOUNT_BASE".into(), lhs: r#"filename = 'payments'"#.into(), rhs: r#"filename = 'invoices'"#.into() },
        //     Constraint::NETS_TO_ZERO { column: "AMOUNT_BASE".into(), lhs: r#"filename = 'receipts'"#.into(), rhs: r#"filename = 'invoices'"#.into() },
        // )},
    ));

    folders::ensure_exist()?;

    let job_id = uuid::Uuid::new_v4();
    log::info!("Starting match job {}", job_id);

    // On start-up, any matching files should log warning and be moved to waiting.
    folders::rollback_incomplete()?;

    // Move any waiting files to the matching folder.
    folders::progress_to_matching()?;

    // Iterate alphabetically matching files.
    process_charter(&charter)?;

    folders::progress_to_archive()?;

    log::info!("Completed match job {}", job_id);

    Ok(())
}

///
/// Process the matching instructions.
///
fn process_charter(charter: &Charter) -> Result<(), MatcherError> {

    // Load all data into memory (for now).
    let mut grid = Grid::new();

    // Create Lua engine bindings.
    let lua = rlua::Lua::new();

    log::info!("Running charter [{}] v{:?} using BASE [{}]",
        charter.name(),
        charter.version(),
        charter.base_currency());

    for inst in charter.instructions() {
        match inst {
            Instruction::SourceData { filename } => instructions::source_data::source_data(filename, &mut grid)?,
            Instruction::ProjectColumn { name, data_type, eval, when } => instructions::project_col::project_column(name, *data_type, eval, when, &mut grid, &lua)?,
            Instruction::MergeColumns { name, source }  => {},
            Instruction::GroupBy { columns }            => {},
            Instruction::UnGroup                        => {},
            Instruction::MatchGroups { constraints }    => {},
            Instruction::Filter                         => {},
            Instruction::UnFilter                       => {},
        };

        log::info!("Memory Grid Size: {}",
            ansi_term::Colour::RGB(70, 130, 180).paint(format!("{:.0}", grid.memory_usage().bytes())));
    }

    // dump_grid(&grid);
    Ok(())
}

fn dump_grid(grid: &Grid) {
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