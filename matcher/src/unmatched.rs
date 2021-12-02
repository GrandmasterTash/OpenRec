use csv::Writer;
use std::{collections::HashMap, fs::File, path::PathBuf};
use crate::{error::MatcherError, folders::{self, ToCanoncialString}, model::{grid::Grid, record::Record, schema::FileSchema}, Context};

///
/// Manages the unmatched files for the current job.
///
pub struct UnmatchedHandler {
    files: HashMap<String /* ORIGINAL filename, e.g. 20211126_072400000_invoices.csv. */, UnmatchedFile>,
}

///
/// Represents an unmatched file potentially being written to as part of the current job.
///
struct UnmatchedFile {
    rows: usize,
    path: PathBuf,
    full_filename: String, // CURRENT filename, e.g. 20211126_072400000_invoices.unmatched.csv.
    schema: FileSchema,    // A copy of the original fileschema.
    writer: Writer<File>,
}

impl UnmatchedHandler {
    ///
    /// Creating a handler will create an unmatched file for each data file loaded into the grid.
    /// The unmatched files will be ready to have unmatched data appended to them. At the end of the job,
    /// if there are any files that didn't have data appended, they are deleted.
    ///
    pub fn new(ctx: &Context, grid: &Grid) -> Result<Self, MatcherError> {
        let mut files: HashMap<String, UnmatchedFile> = HashMap::new();

        // Create an unmatched file for each original sourced data file (i.e. there may be )
        for file in grid.files() {
            // We're using original_filename here to ensure files like 'x.unmatched.csv' don't create
            // files 'x.unmatched.unmatched.csv'.
            if !files.contains_key(file.original_filename()) {
                // Create an new unmatched file.
                let output_path = folders::new_unmatched_file(ctx, file); // $REC_HOME/unmatched/timestamp_invoices.unmatched.csv
                let full_filename = folders::filename(&output_path)?; // timestamp_invoices.unmatched.csv

                let mut writer = csv::WriterBuilder::new().quote_style(csv::QuoteStyle::Always).from_path(&output_path)
                    .map_err(|source| MatcherError::CannotCreateUnmatchedFile{ path: output_path.to_canoncial_string(), source })?;

                // Add the column header and schema rows.
                let schema = grid.schema().file_schemas().get(file.schema())
                    .ok_or(MatcherError::MissingSchemaInGrid{ filename: file.filename().into(), index: file.schema()  })?;

                writer.write_record(schema.columns().iter().map(|c| c.header_no_prefix()).collect::<Vec<&str>>())
                    .map_err(|source| MatcherError::CannotWriteHeaders{ filename: file.filename().into(), source })?;

                writer.write_record(schema.columns().iter().map(|c| c.data_type().to_str()).collect::<Vec<&str>>())
                    .map_err(|source| MatcherError::CannotWriteSchema{ filename: file.filename().into(), source })?;

                files.insert(file.original_filename().into(), UnmatchedFile{ full_filename, path: output_path.clone(), rows: 0, writer, schema: schema.clone() });

                log::trace!("Created file {}", output_path.to_canoncial_string());
            }
        }

        Ok(Self { files })
    }

    pub fn write_records(&mut self, records: &[Box<Record>], grid: &Grid) -> Result<(), MatcherError> {
        // TODO: This can be optimised but wait unti we're streaming data.

        for record in records {
            // Get the unmatched-file for this record.
            let filename = grid.files().get(record.file_idx())
                .ok_or(MatcherError::UnmatchedFileNotInGrid { file_idx: record.file_idx() })?
                .filename();

            let mut unmatched = self.files.get_mut(filename)
                .ok_or(MatcherError::UnmatchedFileNotInHandler { filename: filename.to_string() })?;

            // Track how many records are written to each unmatched file.
            unmatched.rows += 1;

            // Copy the record and truncate any projected or merged fields from it so we only write the
            // original record to disc.
            let mut copy = record.inner().clone();
            copy.truncate(unmatched.schema.columns().len());

            unmatched.writer.write_byte_record(&copy)
                .map_err(|source| MatcherError::CannotWriteUnmatchedRecord { filename: unmatched.full_filename.clone(), row: record.row(), source })?;
        }

        Ok(())
    }

    pub fn complete_files(&mut self, ctx: &Context) -> Result<(), MatcherError> {
        // Delete any unmatched files we didn't write records to.
        for (_filename, unmatched) in self.files.iter() {
            if unmatched.rows == 0 {
                folders::delete_empty_unmatched(ctx, &unmatched.full_filename)?;
            } else {
                // Rename any remaining .inprogress files.
                folders::complete_file(&unmatched.path.to_canoncial_string())?;
            }
        }

        // TODO: info log the creation of these files.
        Ok(())
    }
}