mod error;
mod schema;
mod record;
mod folders;
mod charter;
mod datafile;
mod data_type;
use anyhow::Result;
use data_type::DataType;
use error::MatcherError;
use folders::ToCanoncialString;
use log::Log;
use schema::Schema;
use std::{collections::HashMap, fs::{self, DirEntry}, ops::Index, time};
use crate::{charter::{Charter, Constraint, Instruction}, datafile::DataFile, record::Record};

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

    // TODO: Charter and embed schema as 2nd header row in each file - bloody genious!
    // TODO: Do some actual matching.

    Ok(())
}

///
/// Process the matching instructions.
///
fn process_charter(charter: &Charter) -> Result<(), MatcherError> {

    // Load all data into memory (for now).
    // TODO: Consider a Grid with both of these aggregated.
    let mut files = vec!();
    let mut data = vec!();

    // TODO: Iterate the instructions and process each one - output memory after each instruction.

    for file in folders::files_in_matching()? {
        log::info!("Reading file {}", file.path().to_string_lossy());

        // For now, just count all the records in a file and log them.
        let mut count = 0;

        let mut rdr = csv::ReaderBuilder::new()
            .from_path(file.path())
            .map_err(|source| MatcherError::CannotOpenCsv { source, path: file.path().to_canoncial_string() })?;

        // Build a schema from the file's header rows.
        files.push(DataFile::new(&file, Schema::new(&mut rdr)?));

        // Load the data as bytes into memory.
        for result in rdr.byte_records() {
            let record = result
                .map_err(|source| MatcherError::CannotParseCsvRow { source, path: file.path().to_canoncial_string() })?;

            count += 1;
            data.push(Record::new(files.len() - 1, record));
        }

        log::info!("{} records read from file {}", count, file.file_name().to_string_lossy());
    }

    // Output a new result csv file.
    let mut cells = vec!();
    for file in &files {
        file.schema().headers().iter().for_each(|hdr| {
            cells.push(format!("{}.{}", file.shortname(), hdr));
        });
    }

    let output_path = std::path::Path::new("./tmp/output.csv");
    let mut wtr = csv::WriterBuilder::new().quote_style(csv::QuoteStyle::Always).from_path(output_path).unwrap();
    wtr.write_record(cells).unwrap();

    // TODO: Encapsulate this logic into a DataGrid.
    for record in data {
        let mut cells = vec!();

        for (f_idx, file) in files.iter().enumerate() {
            for col in 0..file.schema().headers().len() {
                if f_idx == record.file_idx() {
                    // TODO: data-type getters.
                    // record.string(idx)
                    // record.long(idx)
                    // record.uuid(idx) etc...
                    cells.push(record.inner().get(col).unwrap());
                } else {
                    cells.push(b""); // Pad cells with empty values.
                }
            }
        }

        wtr.write_byte_record(&cells.into()).unwrap();
    }

    wtr.flush().unwrap();
    Ok(())
}


