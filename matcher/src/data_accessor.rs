use crate::{error::MatcherError, model::{schema::GridSchema, grid::Grid, record::Record}};
use bytes::{Bytes, BytesMut, BufMut};

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
    None, // Derived data is not available to read or write.
    Read(CsvReaders), // Derived data will be retreived from a persisted CSV file.
    Write(Vec<Bytes>, CsvWriters), // Derived data will be retreived from a memory buffer for the current record.
}

///
/// Depending on the phase of the match job, we will modify the origin or unmatched data via an in-memory buffer.
/// These modifications are done via ChangeSets.
///
pub enum ModifyingAccessor {
    None, // The match phase requires no modifying.
    Write(Vec<Bytes>, CsvWriters), // Modified data will be writtern to on a record-by-record basis using the byte buffer.
}

///
/// Passed to a record so it can get one or more columns of data from the appropriate location (buffer or file).
///
pub struct DataAccessor {
    schema: GridSchema,                  // A clone of the Grid's schema - this allows us to iterate
                                         // mut Records in the grid without worry about non-mut access to
                                         // the schema.
    data_readers: CsvReaders,            // A list of open CSV file readers to seek record CSV data.
    derived_accessor: DerivedAccessor,   // An accessor to retrieve derived data for a record.
    modifying_accessor: ModifyingAccessor, // An accessor to modify records of data.
}

impl DataAccessor {
    pub fn for_deriving(grid: &Grid) -> Result<Self, MatcherError> {
        Ok(Self {
            schema: grid.schema().clone(),
            data_readers: csv_readers(grid)?,
            derived_accessor: DerivedAccessor::Write(Vec::new(), derived_writers(grid)?),
            modifying_accessor: ModifyingAccessor::None,
        })
    }

    pub fn for_modifying(grid: &mut Grid) -> Result<Self, MatcherError> {
        Ok(Self {
            schema: grid.schema().clone(),
            data_readers: csv_readers(grid)?,
            derived_accessor: DerivedAccessor::None,
            modifying_accessor: ModifyingAccessor::Write(Vec::new(), modified_writers(grid)?),
        })
    }

    pub fn for_reading(grid: &mut Grid) -> Result<Self, MatcherError> {
        Ok(Self {
            schema: grid.schema().clone(),
            data_readers: csv_readers(grid)?,
            derived_accessor: DerivedAccessor::Read(derived_readers(grid)?),
            modifying_accessor: ModifyingAccessor::None,
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

    pub fn modifying_accessor(&mut self) -> &mut ModifyingAccessor {
        &mut self.modifying_accessor
    }

    ///
    /// Get data from the real CSV file.
    ///
    pub fn get(&mut self, col: usize, record: &Record) -> Result<Option<Bytes>, MatcherError> {

        // Otherwise read the value from the file.
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

    pub fn load_modifying_record(&mut self, record: &Record) -> Result<(), MatcherError> {
        match &mut self.modifying_accessor {
            ModifyingAccessor::None => panic!("Can't load a record into a ModifyingAccessor of None"),
            ModifyingAccessor::Write( .. ) => {
                let rdr = self.data_readers.get_mut(record.file_idx())
                    .ok_or(MatcherError::MissingFileInSchema{ index: record.file_idx() })?;

                rdr.seek(record.pos())?;

                let mut buffer = csv::ByteRecord::new();
                rdr.read_byte_record(&mut buffer)?;

                self.modifying_accessor.load(&buffer)?;
            }
        };

        Ok(())
    }

    pub fn update(&mut self, record: &Record, header: &str, value: &str) -> Result<(), MatcherError> {
        let file = &self.schema.files()[record.file_idx()];

        // Get the column in the buffer to update.
        let pos = match self.schema.position_in_record(header, record) {
            Some(pos) => pos,
            None => return Err(MatcherError::MissingColumn { column: header.into(), file: file.filename().into() }),
        };

        // Replace the value in the buffer with the new value.
        match &mut self.modifying_accessor {
            ModifyingAccessor::None => panic!("Can't apply change to record - no ModifyingAccessor"),
            ModifyingAccessor::Write(buffer, ..) => {
                let mut bytes = BytesMut::new();
                bytes.put_slice(value.as_bytes());
                let old = std::mem::replace(&mut buffer[*pos as usize], bytes.freeze());
                log::trace!("Set {header} to {new} (was {old:?}) in row {row} of {filename}",
                    header = header,
                    new = value,
                    old = old,
                    row = record.row(),
                    filename = file.modifying_filename());
            },
        };

        Ok(())
    }
}

impl DerivedAccessor {
    pub fn get(&mut self, col: usize, record: &Record)
        -> Result<Option<Bytes>, MatcherError> {

        match self {
            DerivedAccessor::None => panic!("Cannot read a derived value when the DerivedAccessor is None"),
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
            DerivedAccessor::None => panic!("Cannot write to a derived value when the DerivedAccessor is None"),
            DerivedAccessor::Read(_) => panic!("File based accessor cannot be written to"),
            DerivedAccessor::Write(buffer, _) => {
                buffer.push(bytes)
            },
        }
    }

    pub fn flush(&mut self, file_idx: u16) -> Result<(), MatcherError> {
        match self {
            DerivedAccessor::None => panic!("Cannot flush a DerivedAccessor of None"),
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
            DerivedAccessor::None => panic!("Cannot write derived headers, DerivedAccessor is None"),
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

impl ModifyingAccessor {
    pub fn load(&mut self, byte_record: &csv::ByteRecord) -> Result<(), MatcherError> {
        match self {
            ModifyingAccessor::None => panic!("Can't load a record into a ModifyingAccessor of None"),
            ModifyingAccessor::Write(buffer, _writers) => {
                buffer.clear();

                // Pump each field into our buffer.
                for raw in byte_record.iter() {
                    let mut bm = BytesMut::new();
                    bm.put(raw);
                    buffer.push(bm.freeze());
                }
            },
        }
        Ok(())
    }

    pub fn flush(&mut self, record: &Record) -> Result<(), MatcherError> {
        match self {
            ModifyingAccessor::None => panic!("Can't load a record into a ModifyingAccessor of None"),
            ModifyingAccessor::Write(buffer, writers) => {
                let writer = writers.get_mut(record.file_idx())
                    .ok_or(MatcherError::MissingFileInSchema{ index: record.file_idx() })?;

                let mut byte_record = csv::ByteRecord::new();
                buffer.iter().for_each(|f| byte_record.push_field(f));
                buffer.clear();

                writer.write_byte_record(&byte_record)?;
                writer.flush()?;
            }
        };
        Ok(())
    }
}


fn csv_readers(grid: &Grid) -> Result<CsvReaders, MatcherError> {
    Ok(grid.schema()
        .files()
        .iter()
        .map(|f| {
            csv::ReaderBuilder::new().from_path(f.path())
                .map_err(|source| MatcherError::CannotOpenCsv{ path: f.path().into(), source } )
        })
        .collect::<Result<Vec<_>, _>>()?)
}


fn derived_readers(grid: &mut Grid) -> Result<CsvReaders, MatcherError> {
    let mut readers = grid.schema()
        .files()
        .iter()
        .map(|f| {
            csv::ReaderBuilder::new()
                .has_headers(false)
                .from_path(f.derived_path())
                .map_err(|source| MatcherError::CannotOpenCsv{ path: f.derived_path().into(), source } )
        })
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
        .map(|f| {
            csv::WriterBuilder::new()
                .has_headers(false)
                .quote_style(csv::QuoteStyle::Always)
                .from_path(f.derived_path())
                .map_err(|source| MatcherError::CannotOpenCsv{ path: f.derived_path().into(), source } )
        })
        .collect::<Result<Vec<_>, _>>()?)
}


fn modified_writers(grid: &Grid) -> Result<CsvWriters, MatcherError> {
    let mut writers = grid.schema()
        .files()
        .iter()
        .map(|f| {
            csv::WriterBuilder::new()
                .has_headers(true)
                .quote_style(csv::QuoteStyle::Always)
                .from_path(f.modifying_path())
                .map_err(|source| MatcherError::CannotOpenCsv{ path: f.modifying_path().into(), source } )
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Write the headers and schema rows.
    for (idx, file) in grid.schema().files().iter().enumerate() {
        let writer = &mut writers[idx];
        let schema = &grid.schema().file_schemas()[file.schema_idx()];

        writer.write_record(schema.columns().iter().map(|c| c.header_no_prefix()).collect::<Vec<&str>>())
            .map_err(|source| MatcherError::CannotWriteHeaders{ filename: file.derived_filename().into(), source })?;

        writer.write_record(schema.columns().iter().map(|c| c.data_type().as_str()).collect::<Vec<&str>>())
            .map_err(|source| MatcherError::CannotWriteSchema{ filename: file.derived_filename().into(), source })?;

        writer.flush()?;
    }

    Ok(writers)
}