use crate::folders;
use std::fs::DirEntry;

#[derive(Clone, Debug)]
pub struct DataFile {
    shortname: String, // 'invoices' if path is '/tmp/20201118_053000000_invoices.csv'
    filename: String,  // '20201118_053000000_invoices.csv' if path is '/tmp/20201118_053000000_invoices.csv'
    path: String,
    schema: usize,     // Index of the file schema in the Grid.
}

impl DataFile {
    pub fn new(entry: &DirEntry, schema: usize) -> Self {
        let filename: String = entry.file_name().to_string_lossy().into();
        Self {
            shortname: folders::shortname(&filename).into(),
            filename,
            path: entry.path().to_string_lossy().into(),
            schema,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }
}