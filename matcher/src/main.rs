mod grid;
mod error;
mod schema;
mod record;
mod folders;
mod charter;
mod datafile;
mod data_type;
use std::time;
use anyhow::Result;
use schema::Schema;
use error::MatcherError;
use folders::ToCanoncialString;
use crate::{charter::{Charter, Constraint, Instruction}, datafile::DataFile, grid::Grid, record::Record};

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    // Build a charter model to match our three files with.
    let charter = Charter::new("test invoices".into(), false, "EUR".into(), time::Instant::now(), vec!(
        Instruction::SOURCE_DATA { filename: "invoices".into() },
        Instruction::SOURCE_DATA { filename: "payments".into() },
        Instruction::SOURCE_DATA { filename: "receipts".into() },
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

    // TODO: Iterate the instructions and process each one - output memory after each instruction.

    for file in folders::files_in_matching()? {
        log::info!("Reading file {}", file.path().to_string_lossy());

        // For now, just count all the records in a file and log them.
        let mut count = 0;

        let mut rdr = csv::ReaderBuilder::new()
            .from_path(file.path())
            .map_err(|source| MatcherError::CannotOpenCsv { source, path: file.path().to_canoncial_string() })?;

        // Build a schema from the file's header rows.
        grid.add_file(DataFile::new(&file, Schema::new(&mut rdr)?));

        // Load the data as bytes into memory.
        for result in rdr.byte_records() {
            let record = result
                .map_err(|source| MatcherError::CannotParseCsvRow { source, path: file.path().to_canoncial_string() })?;

            count += 1;
            grid.add_record(Record::new(grid.files().len() - 1, record));
        }

        log::info!("{} records read from file {}", count, file.file_name().to_string_lossy());
    }

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
    Ok(())
}


