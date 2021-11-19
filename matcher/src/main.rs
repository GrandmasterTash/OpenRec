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
use crate::{charter::{Charter, Constraint, Instruction}, folders::ToCanoncialString, grid::Grid};

// TODO: Make this charter work with in-memory data.
// TODO: Refactor to work with data streamed from files.

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    log::info!("{}", BANNER);

    // Build a charter model to match our three files with.
    let charter = Charter::new("test invoices".into(), false, "EUR".into(), chrono::Utc::now().timestamp_millis() as u64, vec!(
        Instruction::SOURCE_DATA { filename: ".*invoices\\.csv".into() },
        Instruction::SOURCE_DATA { filename: ".*payments\\.csv".into() },
        Instruction::SOURCE_DATA { filename: ".*receipts\\.csv".into() },
        Instruction::PROJECT_COLUMN { name: "PAYMENT_AMOUNT_BASE".into(), lua: r#"payments.Amount * payments.FXRate"#.into() },
        Instruction::PROJECT_COLUMN { name: "RECEIPT_AMOUNT_BASE".into(), lua: r#"receipts.Amount * receipts.FXRate"#.into() },
        Instruction::PROJECT_COLUMN { name: "TOTAL_AMOUNT_BASE".into(),   lua: r#"invoices.TotalAmount * invoices.FXRate"#.into() },
        Instruction::MERGE_COLUMNS { name: "SETTLEMENT_DATE".into(), source: vec!("invoices.SettlementDate".into(), "payments.PaymentDate".into(), "receiptes.ReceiptDate".into() )},
        Instruction::MERGE_COLUMNS { name: "AMOUNT_BASE".into(),     source: vec!("PAYMENT_AMOUNT_BASE".into(), "RECEIPT_AMOUNT_BASE".into(), "TOTAL_AMOUNT_BASE".into() )},
        Instruction::GROUP_BY { columns: vec!("SETTLEMENT_DATE".into()) },
        Instruction::MATCH_GROUPS { constraints: vec!(
            Constraint::NETS_TO_ZERO { column: "AMOUNT_BASE".into(), lhs: r#"filename = 'payments'"#.into(), rhs: r#"filename = 'invoices'"#.into() },
            Constraint::NETS_TO_ZERO { column: "AMOUNT_BASE".into(), lhs: r#"filename = 'receipts'"#.into(), rhs: r#"filename = 'invoices'"#.into() },
        )},
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

    log::info!("Running charter [{}] v{:?} using BASE [{}]",
        charter.name(),
        charter.version(),
        charter.base_currency());

    for inst in charter.instructions() {
        match inst {
            Instruction::SOURCE_DATA { filename }       => instructions::source_data::source_data(filename, &mut grid)?,
            Instruction::PROJECT_COLUMN { name, lua }   => instructions::project_col::project_col(name, lua, &mut grid)?,
            Instruction::MERGE_COLUMNS { name, source } => println!("TODO: MERGE_COLUMNS {} {:?}", name, source),
            Instruction::GROUP_BY { columns }           => println!("TODO: GROUP_BY {:?}", columns),
            Instruction::UN_GROUP                       => println!("TODO: UN_GROUP"),
            Instruction::MATCH_GROUPS { constraints }   => println!("TODO: MATCH_GROUPS: {} constraints", constraints.len()),
            Instruction::FILTER                         => println!("TODO: FILTER"),
            Instruction::UN_FILTER                      => println!("TODO: UN_FILTER"),
        }

        log::info!("Memory Grid Size: {}",
            ansi_term::Colour::RGB(70, 130, 180).paint(format!("{:.0}", grid.memory_usage().bytes())));
    }

    dump_grid(&grid);
    Ok(())
}

fn dump_grid(grid: &Grid) {
    // Output a new result csv file.
    let output_path = std::path::Path::new("./tmp/output.csv");
    let mut wtr = csv::WriterBuilder::new().quote_style(csv::QuoteStyle::Always).from_path(output_path).unwrap();
    wtr.write_record(grid.headers()).unwrap();

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