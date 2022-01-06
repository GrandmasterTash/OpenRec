use csv::Writer;
use std::{collections::HashMap, fs::File, path::PathBuf};
use crate::{error::MatcherError, folders::{self, ToCanoncialString}, model::grid::Grid, Context, utils};

///
/// Manages the unmatched files for the current job.
///
pub struct UnmatchedHandler {
    files: HashMap<String /* ORIGINAL filename, e.g. 20211126_072400000_invoices.csv. */, UnmatchedFile>,
}

///
/// Represents an unmatched file potentially being written to as part of the current job.
///
pub struct UnmatchedFile {
    rows: usize,
    path: PathBuf,
    full_filename: String, // CURRENT filename, e.g. 20211126_072400000_invoices.unmatched.csv.
    writer: Writer<File>,
}

impl UnmatchedFile {
    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn filename(&self) -> &str {
        &self.full_filename
    }
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
        for file in grid.schema().files() {
            if !files.contains_key(file.filename()) {
                // Create an new unmatched file.
                let output_path = folders::new_unmatched_file(ctx, file); // $REC_HOME/unmatched/timestamp_invoices.unmatched.csv
                let full_filename = folders::filename(&output_path); // timestamp_invoices.unmatched.csv
                let mut writer = utils::csv::writer(&output_path);

                // Add the column header and schema rows.
                let schema = &grid.schema().file_schemas()[file.schema_idx()];

                writer.write_record(schema.columns().iter().map(|c| c.header_no_prefix()).collect::<Vec<&str>>())
                    .map_err(|source| MatcherError::CannotWriteHeaders{ filename: file.filename().into(), source })?;

                writer.write_record(schema.columns().iter().map(|c| c.data_type().as_str()).collect::<Vec<&str>>())
                    .map_err(|source| MatcherError::CannotWriteSchema{ filename: file.filename().into(), source })?;

                files.insert(file.filename().into(), UnmatchedFile{ full_filename, path: output_path.clone(), rows: 0, writer });

                log::debug!("Created file {}", output_path.to_canoncial_string());
            }
        }

        Ok(Self { files })
    }

    pub fn write_records(&mut self, ctx: &Context, grid: &Grid) -> Result<(), MatcherError> {
        for record in grid.iter(ctx) {
            // Get the unmatched-file for this record.
            let filename = grid.schema().files().get(record.file_idx())
                .ok_or(MatcherError::UnmatchedFileNotInGrid { file_idx: record.file_idx() })?
                .filename();

            let mut unmatched = self.files.get_mut(filename)
                .ok_or(MatcherError::UnmatchedFileNotInHandler { filename: filename.to_string() })?;

            // Track how many records are written to each unmatched file.
            unmatched.rows += 1;

            // Copy the original CSV record to the unmatched file.
            unmatched.writer.write_byte_record(record.data())
                .map_err(|source| MatcherError::CannotWriteUnmatchedRecord {
                    filename: unmatched.full_filename.clone(),
                    row: record.row(), source
                })?;
        }

        self.complete_files(ctx)
    }

    fn complete_files(&mut self, ctx: &Context) -> Result<(), MatcherError> {
        // Delete any unmatched files we didn't write records to.
        for (_filename, mut unmatched) in self.files.iter_mut() {
            if unmatched.rows == 0 {
                folders::delete_empty_unmatched(ctx, &unmatched.full_filename)?;
            } else {
                // Rename any remaining .inprogress files.
                let path = folders::complete_file(&unmatched.path.to_canoncial_string())?;
                unmatched.full_filename = folders::filename(&path);
                log::debug!("Created unmatched file {}", path.to_canoncial_string());
            }
        }

        Ok(())
    }

    pub fn unmatched_files(&self) -> Vec<&UnmatchedFile> {
        self.files.values().collect()
    }
}