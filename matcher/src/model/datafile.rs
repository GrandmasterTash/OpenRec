use std::fs::DirEntry;
use crate::{error::MatcherError, folders::{self, ToCanoncialString}};

#[derive(Clone, Debug)]
pub struct DataFile {
    shortname: String, // 'invoices' if filename is '/tmp/20201118_053000000_invoices.csv' or '/tmp/20201118_053000000_invoices.unmatched.csv'
    filename: String,  // '20201118_053000000_invoices.csv' if path is '/tmp/20201118_053000000_invoices.csv'
    path: String,      // The full canonical path to the file.
    timestamp: String, // '20201118_053000000' if the filename is '20201118_053000000_invoices.csv'.
    original_filename: String, // 20201118_053000000_invoices.unmatched.csv -> 20201118_053000000_invoices.csv
    schema: usize,     // Index of the file's schema in the Grid.
}

impl DataFile {
    pub fn new(entry: &DirEntry, schema: usize) -> Result<Self, MatcherError> {
        let filename: String = entry.file_name().to_string_lossy().into();
        let shortname = folders::shortname(&filename).to_string();
        let timestamp = folders::timestamp(&filename)?.to_string();
        Ok(Self {
            shortname: shortname.clone(),
            filename: filename.clone(),
            path: entry.path().to_canoncial_string(),
            timestamp: timestamp.clone(),
            original_filename: format!("{}_{}.csv", timestamp, shortname), // TODO: this should be done in folders module.
            schema,
        })
    }

    pub fn schema_idx(&self) -> usize {
        self.schema
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn shortname(&self) -> &str {
        &self.shortname
    }

    pub fn timestamp(&self) -> &str {
        &self.timestamp
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn original_filename(&self) -> &str {
        &self.original_filename
    }
}