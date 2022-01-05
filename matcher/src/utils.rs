use std::{fs::File, path::Path};

use crate::folders::ToCanoncialString;

pub type CsvReader = csv::Reader<File>;
pub type CsvWriter = csv::Writer<File>;
pub type CsvReaders = Vec<CsvReader>;
pub type CsvWriters = Vec<CsvWriter>;

///
/// Create a csv data file reader, with the option to skip the schema row so the next row will be the
/// first data row.
///
pub fn reader<P>(path: P, skip_schema: bool) -> CsvReader
where
    P: AsRef<Path>
{
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&path)
        .unwrap_or_else(|err| {
            let path: &Path = path.as_ref();
            panic!("Failed to open {} : {}", path.to_canoncial_string(), err)
        });

    if skip_schema {
        let mut ignored = csv::ByteRecord::new();
        reader.read_byte_record(&mut ignored).unwrap();
    }

    reader
}

///
/// Create a CSV writer ready to write...
///
pub fn writer<P>(path: P) -> CsvWriter
where
    P: AsRef<Path>
{
    csv::WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Always)
        .from_path(&path)
        .unwrap_or_else(|err| {
            let path: &Path = path.as_ref();
            panic!("Failed to open {} : {}", path.to_canoncial_string(), err)
        })
}

///
/// Create a CSV reader for an index.xxxx.xxx file. These have no schema row or headers.
///
pub fn index_reader<P>(path: P) -> CsvReader
where
    P: AsRef<Path>
{
    csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(&path)
        .unwrap_or_else(|err| {
            let path: &Path = path.as_ref();
            panic!("Failed to open {} : {}", path.to_canoncial_string(), err)
        })
}