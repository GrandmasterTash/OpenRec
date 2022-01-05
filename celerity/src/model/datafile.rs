use std::fs::DirEntry;
use crate::{error::MatcherError, folders::{self, ToCanoncialString}};

///
/// Represents a physical sourced file of data.
///
/// Contains various representations of it's path along with the index of it's schema
/// held in the GridSchema.
///
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DataFile {
    // TODO: Revisit this jumble.
    shortname: String,         // 'invoices' if filename is '/tmp/20201118_053000000_invoices.csv' or '/tmp/20201118_053000000_invoices.unmatched.csv'
    filename: String,          // '20201118_053000000_invoices.csv' if path is '/tmp/20201118_053000000_invoices.csv'
    path: String,              // The full canonical path to the file.
    derived_path: String,      // The path to any temporary file storing derived data, eg. '/tmp/20201118_053000000_invoices.derived.csv'
    derived_filename: String,  // '20201118_053000000_invoices.derived.csv'
    modifying_path: String,    // The path to any temporary file storing modified data, eg. '/tmp/20201118_053000000_invoices.csv.modifying'
    modifying_filename: String,// '20201118_053000000_invoices.csv.modifying'
    pre_modified_path: String, // The path to an unmatched file before a changeset is applied, eg. '/tmp/20201118_053000000_invoices.unmatched.csv.pre_modified'
    timestamp: String,         // '20201118_053000000' if the filename is '20201118_053000000_invoices.csv'.
    original_filename: String, // 20201118_053000000_invoices.unmatched.csv -> 20201118_053000000_invoices.csv
    schema: usize,             // Index of the file's schema in the Grid.
}

impl DataFile {
    pub fn new(entry: &DirEntry, schema: usize) -> Result<Self, MatcherError> {
        let path = entry.path().to_canoncial_string();
        let filename: String = entry.file_name().to_string_lossy().into();
        let original_filename = folders::original_filename(&filename)?;
        let shortname = folders::shortname(&filename).to_string();
        let timestamp = folders::timestamp(&filename)?.to_string();

        let derived = folders::derived(entry)?;
        let derived_path = derived.to_string_lossy().into();
        let derived_filename = derived.file_name()
            .unwrap_or_else(|| panic!("{} is not a file/has no filename",  derived.to_canoncial_string()))
            .to_string_lossy()
            .into();

        let modifying = folders::modifying(entry)?;
        let modifying_path = modifying.to_string_lossy().into();
        let modifying_filename = modifying.file_name()
            .unwrap_or_else(|| panic!("{} is not a file/has no filename",  derived.to_canoncial_string()))
            .to_string_lossy()
            .into();

        let pre_modified_path = folders::pre_modified(entry).to_string_lossy().into();

        Ok(Self {
            shortname,
            filename,
            path,
            derived_path,
            derived_filename,
            modifying_path,
            modifying_filename,
            pre_modified_path,
            timestamp,
            original_filename,
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

    pub fn derived_path(&self) -> &str {
        &self.derived_path
    }

    pub fn derived_filename(&self) -> &str {
        &self.derived_filename
    }

    pub fn modifying_path(&self) -> &str {
        &self.modifying_path
    }

    pub fn modifying_filename(&self) -> &str {
        &self.modifying_filename
    }

    pub fn pre_modified_path(&self) -> &str {
        &self.pre_modified_path
    }

}