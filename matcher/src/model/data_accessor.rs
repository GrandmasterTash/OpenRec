use bytes::{Bytes, BytesMut, BufMut};
use crate::error::MatcherError;
use super::{schema::GridSchema, grid::Grid};

///
/// A list of open CSV reader in the order of files sourced into the Grid.
///
pub type CsvReaders = Vec<csv::Reader<std::fs::File>>;

///
/// A list of open CSV writers in the order of files sourced into the Grid.
///
pub type CsvWriters = Vec<csv::Writer<std::fs::File>>;

///
/// Depending on the phase of the match job, we will access derived data either from an in-memory buffer,
/// or from a CSV file.
///
pub enum DerivedAccessor {
    // Derived data will be retreived from a persisted CSV file.
    File(CsvReaders), // TODO: Rename these enums to Read and Write respectively.

    // Derived data will be retreived from a memory buffer for the current record.
    Buffer(Vec<Bytes>, CsvWriters),
}

///
/// Passed to a record so it can get one or more columns of data from the appropriate location.
///
pub struct DataAccessor {
    schema: GridSchema,                // A clone of the Grid's schema - this allows us to iterate
                                       // mut Records in the grid without worry about non-mut access to
                                       // the schema.
    data_readers: CsvReaders,          // A list of open CSV file readers to seek record CSV data.
    derived_accessor: DerivedAccessor, // An accessor to retrieve derived data for a record.
}

impl DataAccessor {
    pub fn with_buffer(grid: &Grid) -> Self {
        Self {
            schema: grid.schema().clone(),
            data_readers: csv_readers(grid),
            derived_accessor: DerivedAccessor::Buffer(Vec::new(), derived_writers(grid)),
        }
    }

    pub fn with_no_buffer(grid: &mut Grid) -> Self {
        Self {
            schema: grid.schema().clone(),
            data_readers: csv_readers(grid),
            derived_accessor: DerivedAccessor::File(derived_readers(grid)),
        }
    }

    pub fn schema(&self) -> &GridSchema {
        &self.schema
    }

    pub fn set_schema(&mut self, schema: GridSchema) {
        self.schema = schema;
    }

    pub fn data_readers(&mut self) -> &mut CsvReaders {
        &mut self.data_readers
    }

    pub fn derived_accessor(&mut self) -> &mut DerivedAccessor {
        &mut self.derived_accessor
    }

    pub fn write_derived_headers(&mut self) -> Result<(), MatcherError> {
        self.derived_accessor.write_headers(&self.schema)
    }
}

impl DerivedAccessor {
    pub fn get(&mut self, col: usize, file_idx: usize, pos: Option<csv::Position>) -> Option<Bytes> {
        match self {
            DerivedAccessor::File(readers) => {
                let rdr = readers.get_mut(file_idx).unwrap(); // TODO: Don't unwrap.
                rdr.seek(pos.unwrap()).unwrap(); // TODO: Don't unwrap.
                let mut buffer = csv::ByteRecord::new();
                rdr.read_byte_record(&mut buffer).unwrap(); // TODO: Don't unwrap.

                match buffer.get(col as usize) {
                    Some(bytes) if bytes.len() > 0 => {
                        let mut bm = BytesMut::new();
                        bm.put(bytes);
                        Some(bm.freeze())
                    },
                    Some(_) |
                    None    => None,
                }
            },
            DerivedAccessor::Buffer(buffer, _writers) => buffer.get(col).cloned(),
        }
    }

    pub fn append(&mut self, bytes: Bytes) {
        match self {
            DerivedAccessor::File(_) => panic!("File based accessor cannot be written to"),
            DerivedAccessor::Buffer(buffer, _) => {
                buffer.push(bytes)
            },
        }
    }

    pub fn flush(&mut self, file_idx: u16) -> Result<(), MatcherError> {
        match self {
            DerivedAccessor::File(_) => panic!("File based accessor cannot be flushed"),
            DerivedAccessor::Buffer(buffer, writers) => {
                let mut record = csv::ByteRecord::new();
                buffer.iter().for_each(|f| record.push_field(f));
                buffer.clear();

                let writer = writers.get_mut(file_idx as usize).unwrap(); // TODO: Don't unwrap.
                writer.write_byte_record(&record).unwrap(); // TODO: Don't unwrap.
                writer.flush().unwrap(); // TODO: Don't unwrap.
            },
        }
        Ok(())
    }

    pub fn write_headers(&mut self, schema: &GridSchema) -> Result<(), MatcherError> {
        let writers = match self {
            DerivedAccessor::File(_) => panic!("Can't write derived headers, accessor is for reading only"),
            DerivedAccessor::Buffer(_, writers) => writers,
        };

        for (idx, file) in schema.files().iter().enumerate() {
            // let file_schema = &schema.file_schemas()[file.schema_idx()];
            let writer = &mut writers[idx];

        writer.write_record(schema.derived_columns().iter().map(|c| c.header_no_prefix()).collect::<Vec<&str>>())
            .map_err(|source| MatcherError::CannotWriteHeaders{ filename: file.derived_filename().into(), source })?;

        writer.write_record(schema.derived_columns().iter().map(|c| c.data_type().to_str()).collect::<Vec<&str>>())
            .map_err(|source| MatcherError::CannotWriteSchema{ filename: file.derived_filename().into(), source })?;
            writer.flush()?;
        }

        Ok(())
    }
}


fn csv_readers(grid: &Grid) -> CsvReaders {
    grid.schema()
        .files()
        .iter()
        .map(|f| csv::ReaderBuilder::new().from_path(f.path()).unwrap()) // TODO: Don't unwrap
        .collect()
}

fn derived_readers(grid: &mut Grid) -> CsvReaders {
    let mut readers: CsvReaders = grid.schema()
        .files()
        .iter()
        .map(|f| csv::ReaderBuilder::new().has_headers(false).from_path(f.derived_path()).unwrap()) // TODO: Don't unwrap
        .collect();

    // Index all the record derived positions in their respective files.

    // Skip the schema row in each reader.
    let mut ignored = csv::ByteRecord::new();
    for reader in &mut readers {
        reader.read_byte_record(&mut ignored).unwrap(); // TODO: Don't unwrap.
    }

    // Advance the reader in the appropriate file for each record in the grid.
    for record in grid.records_mut() {
        let reader = &mut readers[record.file_idx()];
        reader.read_byte_record(&mut ignored).unwrap();
        record.set_derived_pos(reader.position());
    }

    readers
}

fn derived_writers(grid: &Grid) -> CsvWriters {
    grid.schema()
        .files()
        .iter()
        .map(|f| {
            println!("DERIVED_PATH: {:?}", f.derived_path());
            f
        }) // TODO: Don't unwrap
        .map(|f| csv::WriterBuilder::new().has_headers(false).quote_style(csv::QuoteStyle::Always).from_path(f.derived_path()).unwrap()) // TODO: Don't unwrap
        .collect()
}