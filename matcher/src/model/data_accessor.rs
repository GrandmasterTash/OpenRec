use crate::error::MatcherError;
use bytes::{Bytes, BytesMut, BufMut};
use super::{schema::GridSchema, grid::Grid, record::Record};

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
    Read(CsvReaders),

    // Derived data will be retreived from a memory buffer for the current record.
    Write(Vec<Bytes>, CsvWriters),
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
    pub fn with_buffer(grid: &Grid) -> Result<Self, MatcherError> {
        Ok(Self {
            schema: grid.schema().clone(),
            data_readers: csv_readers(grid)?,
            derived_accessor: DerivedAccessor::Write(Vec::new(), derived_writers(grid)?),
        })
    }

    pub fn with_no_buffer(grid: &mut Grid) -> Result<Self, MatcherError> {
        Ok(Self {
            schema: grid.schema().clone(),
            data_readers: csv_readers(grid)?,
            derived_accessor: DerivedAccessor::Read(derived_readers(grid)?),
        })
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

    ///
    /// Get data from the real CSV file.
    ///
    pub fn get(&mut self, col: usize, record: &Record) -> Result<Option<Bytes>, MatcherError> {
        let rdr = self.data_readers.get_mut(record.file_idx())
            .ok_or(MatcherError::MissingFileInSchema { index: record.file_idx() })?;
        rdr.seek(record.pos())?;

        let mut buffer = csv::ByteRecord::new();
        rdr.read_byte_record(&mut buffer)?;

        match buffer.get(col as usize) {
            Some(bytes) if !bytes.is_empty() => {
                let mut bm = BytesMut::new();
                bm.put(bytes);
                Ok(Some(bm.freeze()))
            },
            Some(_) |
            None    => Ok(None),
        }
    }
}

impl DerivedAccessor {
    pub fn get(&mut self, col: usize, record: &Record)
        -> Result<Option<Bytes>, MatcherError> {

        match self {
            DerivedAccessor::Read(readers) => {
                let rdr = readers.get_mut(record.file_idx())
                    .ok_or(MatcherError::MissingFileInSchema{ index: record.file_idx() })?;

                match record.derived_pos() {
                    Some(pos) => rdr.seek(pos)?,
                    None => return Err(MatcherError::NoDerivedPosition { row: record.row(), file_idx: record.file_idx() }),
                };

                let mut buffer = csv::ByteRecord::new();
                rdr.read_byte_record(&mut buffer)?;

                match buffer.get(col as usize) {
                    Some(bytes) if !bytes.is_empty() => {
                        let mut bm = BytesMut::new();
                        bm.put(bytes);
                        Ok(Some(bm.freeze()))
                    },

                    Some(_) |
                    None    => Ok(None),
                }
            },
            DerivedAccessor::Write(buffer, _writers) => Ok(buffer.get(col).cloned()),
        }
    }

    pub fn append(&mut self, bytes: Bytes) {
        match self {
            DerivedAccessor::Read(_) => panic!("File based accessor cannot be written to"),
            DerivedAccessor::Write(buffer, _) => {
                buffer.push(bytes)
            },
        }
    }

    pub fn flush(&mut self, file_idx: u16) -> Result<(), MatcherError> {
        match self {
            DerivedAccessor::Read(_) => panic!("File based accessor cannot be flushed"),
            DerivedAccessor::Write(buffer, writers) => {
                let mut record = csv::ByteRecord::new();
                buffer.iter().for_each(|f| record.push_field(f));
                buffer.clear();

                let writer = writers.get_mut(file_idx as usize)
                    .ok_or(MatcherError::MissingFileInSchema{ index: file_idx as usize })?;
                writer.write_byte_record(&record)?;
                writer.flush()?;
            },
        }
        Ok(())
    }

    pub fn write_headers(&mut self, schema: &GridSchema) -> Result<(), MatcherError> {
        let writers = match self {
            DerivedAccessor::Read(_) => panic!("Can't write derived headers, accessor is for reading only"),
            DerivedAccessor::Write(_, writers) => writers,
        };

        for (idx, file) in schema.files().iter().enumerate() {
            let writer = &mut writers[idx];

        writer.write_record(schema.derived_columns().iter().map(|c| c.header_no_prefix()).collect::<Vec<&str>>())
            .map_err(|source| MatcherError::CannotWriteHeaders{ filename: file.derived_filename().into(), source })?;

        writer.write_record(schema.derived_columns().iter().map(|c| c.data_type().as_str()).collect::<Vec<&str>>())
            .map_err(|source| MatcherError::CannotWriteSchema{ filename: file.derived_filename().into(), source })?;
            writer.flush()?;
        }

        Ok(())
    }
}


fn csv_readers(grid: &Grid) -> Result<CsvReaders, MatcherError> {
    Ok(grid.schema()
        .files()
        .iter()
        .map(|f| csv::ReaderBuilder::new().from_path(f.path()))
        .collect::<Result<Vec<_>, _>>()?)
}


fn derived_readers(grid: &mut Grid) -> Result<CsvReaders, MatcherError> {
    let mut readers = grid.schema()
        .files()
        .iter()
        .map(|f| csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(f.derived_path()))
        .collect::<Result<Vec<_>, _>>()?;

    // Index all the record derived positions in their respective files.

    // Skip the schema row in each reader.
    let mut ignored = csv::ByteRecord::new();
    for reader in &mut readers {
        reader.read_byte_record(&mut ignored)?;
    }

    // Advance the reader in the appropriate file for each record in the grid.
    for record in grid.records_mut() {
        let reader = &mut readers[record.file_idx()];
        reader.read_byte_record(&mut ignored).unwrap();
        record.set_derived_pos(reader.position());
    }

    Ok(readers)
}


fn derived_writers(grid: &Grid) -> Result<CsvWriters, MatcherError> {
    Ok(grid.schema()
        .files()
        .iter()
        .map(|f| csv::WriterBuilder::new()
            .has_headers(false)
            .quote_style(csv::QuoteStyle::Always)
            .from_path(f.derived_path()))
        .collect::<Result<Vec<_>, _>>()?)
}