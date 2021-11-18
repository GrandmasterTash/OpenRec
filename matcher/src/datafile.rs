use std::fs::DirEntry;
use crate::{folders, schema::Schema};

#[derive(Debug)]
pub struct DataFile {
    shortname: String, // 'invoices' if path is '/tmp/20201118_053000000_invoices.csv'
    filename: String,  // '20201118_053000000_invoices.csv' if path is '/tmp/20201118_053000000_invoices.csv'
    path: String,
    schema: Schema
}

impl DataFile {
    pub fn new(entry: &DirEntry, schema: Schema) -> Self {
        let filename: String = entry.file_name().to_string_lossy().into();
        Self {
            shortname: folders::shortname(&filename).into(),
            filename,
            path: entry.path().to_string_lossy().into(),
            schema,
        }
    }

    pub fn shortname(&self) -> &str {
        &self.shortname
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}