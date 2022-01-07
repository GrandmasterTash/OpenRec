use uuid::Uuid;
use std::sync::Arc;
use rust_decimal::Decimal;
use super::schema::GridSchema;
use core::data_type::DataType;
use bytes::{Bytes, BytesMut, BufMut};
use crate::{utils::convert, error::MatcherError, folders::ToCanoncialString};

pub struct Record {
    file_idx: usize,  // Index of the DataFile in the grid(schema).
    schema: Arc<GridSchema>,
    data: csv::ByteRecord,    // Source csv data for the record.
    derived: csv::ByteRecord, // Derived csv data for the record.
    buffer: Vec<Bytes>,       // Used to modify the record or create derived data (depending upon the phase).
}

impl Record {
    pub fn new(file_idx: usize, schema: Arc<GridSchema>, data: csv::ByteRecord, derived: csv::ByteRecord) -> Self {
        Self {
            file_idx,
            schema,
            data,
            derived,
            buffer: vec!()
        }
    }

    pub fn row(&self) -> usize {
        self.data.position().expect("no row position").line() as usize
    }

    pub fn data_position(&self) -> &csv::Position {
        self.data.position().expect("no data position")
    }

    pub fn derived_position(&self) -> &csv::Position {
        self.derived.position().expect("no derived position")
    }

    pub fn file_idx(&self) -> usize {
        self.file_idx
    }

    pub fn data(&self) -> &csv::ByteRecord {
        &self.data
    }

    pub fn schema(&self) -> Arc<GridSchema> {
        self.schema.clone()
    }

    ///
    /// Get the derived value, or load the real value from the backing csv reader.
    ///
    fn get_bytes(&self, col: isize) -> Result<Option<Bytes>, MatcherError> {
        match col < 0 {
            true  => { // Derived column.
                // These use negative indexes in the Grid and must be translated to a real
                // CSV column. -1 -> 0, -2 -> 1, -3 -> 2, etc.
                match self.derived.get((col.abs() - 1) as usize) {
                    Some(u8s) if !u8s.is_empty() => Ok(Some(u8s.to_bytes())),
                    Some(_) |
                    None    => Ok(None),
                }
            },
            false => { // File column.
                match self.data.get(col as usize) {
                    Some(u8s) if !u8s.is_empty() => Ok(Some(u8s.to_bytes())),
                    Some(_) |
                    None    => Ok(None),
                }
            },
        }
    }

    pub fn get_bool(&self, header: &str) -> Result<Option<bool>, MatcherError> {
        if let Some(column) = self.schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column)? {
                return Ok(Some(convert::csv_bytes_to_bool(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_datetime(&self, header: &str) -> Result<Option<u64>, MatcherError> {
        if let Some(column) = self.schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column)? {
                return Ok(Some(convert::csv_bytes_to_datetime(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_decimal(&self, header: &str) -> Result<Option<Decimal>, MatcherError> {
        if let Some(col) = self.schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*col)? {
                return Ok(Some(convert::csv_bytes_to_decimal(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_int(&self, header: &str) -> Result<Option<i64>, MatcherError> {
        if let Some(column) = self.schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column)? {
                return Ok(Some(convert::csv_bytes_to_int(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_string(&self, header: &str) -> Result<Option<String>, MatcherError> {
        if let Some(column) = self.schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column)? {
                return Ok(Some(convert::csv_bytes_to_string(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_uuid(&self, header: &str) -> Result<Option<Uuid>, MatcherError> {
        if let Some(column) = self.schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column)? {
                return Ok(Some(convert::csv_bytes_to_uuid(bytes)?))
            }
        }
        Ok(None)
    }

    ///
    /// Get the value in the column as a displayable string - if no value is present an empty string is returned.
    ///
    pub fn get_as_string(&self, header: &str) -> Result<String, MatcherError> {
        match self.schema.data_type(header) {
            Some(data_type) => match data_type {
                DataType::Unknown => Err(MatcherError::UnknownDataTypeForHeader { header: header.into() }),
                DataType::Boolean => Ok(self.get_bool(header)?.map(convert::bool_to_string).unwrap_or_default()),
                DataType::Datetime => Ok(self.get_datetime(header)?.map(convert::datetime_to_string).unwrap_or_default()),
                DataType::Decimal => Ok(self.get_decimal(header)?.map(convert::decimal_to_string).unwrap_or_default()),
                DataType::Integer => Ok(self.get_int(header)?.map(convert::int_to_string).unwrap_or_default()),
                DataType::String => Ok(self.get_string(header)?.unwrap_or_default()),
                DataType::Uuid => Ok(self.get_uuid(header)?.map(convert::uuid_to_string).unwrap_or_default()),
            },
            None => Ok(String::default()),
        }
    }

    ///
    /// Initialise the buffer if this is the first update. Populate it with all the data-fields.
    ///
    pub fn load_buffer(&mut self) {
        self.buffer.clear();

        // Pump each field into our buffer.
        for raw in self.data.iter() {
            let mut bm = BytesMut::new();
            bm.put(raw);
            self.buffer.push(bm.freeze());
        }
    }

    ///
    /// Update a field (real data - not a derived field) - as part of a changeset modification.
    ///
    /// This function updates a buffer which must be returned with flush() and then written to disk.
    ///
    pub fn update(&mut self, header: &str, value: &str) -> Result<(), MatcherError> {
        let file = &self.schema.files()[self.file_idx()];

        // Get the column in the buffer to update.
        let pos = match self.schema.position_in_record(header, self) {
            Some(pos) => pos,
            None => return Err(MatcherError::MissingColumn { column: header.into(), file: file.filename().into() }),
        };

        // Replace the value in the buffer with the new value.
        let mut bytes = BytesMut::new();
        bytes.put_slice(value.as_bytes());
        let old = std::mem::replace(&mut self.buffer[*pos as usize], bytes.freeze());
        log::trace!("Set {header} to {new} (was {old:?}) in row {row} of {filename}",
            header = header,
            new = value,
            old = old,
            row = self.row(),
            filename = file.modifying_path().to_canoncial_string());

        Ok(())
    }

    ///
    /// Add a derived boolean value to the buffer. Use flush to retrieve the buffer for writing.
    ///
    pub fn append_bool(&mut self, value: bool) {
        let string = convert::bool_to_string(value);
        self.derived.push_field(string.as_bytes());
        self.buffer.push(string.into());
    }

    ///
    /// Add a derived datetime value to the buffer. Use flush to retrieve the buffer for writing.
    ///
    pub fn append_datetime(&mut self, value: u64) {
        let string = convert::datetime_to_string(value);
        self.derived.push_field(string.as_bytes());
        self.buffer.push(string.into());
    }

    ///
    /// Add a derived decimal value to the buffer. Use flush to retrieve the buffer for writing.
    ///
    pub fn append_decimal(&mut self, value: Decimal) {
        let string = convert::decimal_to_string(value);
        self.derived.push_field(string.as_bytes());
        self.buffer.push(string.into());
    }

    ///
    /// Add a derived integer value to the buffer. Use flush to retrieve the buffer for writing.
    ///
    pub fn append_int(&mut self, value: i64) {
        let string = convert::int_to_string(value);
        self.derived.push_field(string.as_bytes());
        self.buffer.push(string.into());
    }

    ///
    /// Add a derived string value to the buffer. Use flush to retrieve the buffer for writing.
    ///
    pub fn append_string(&mut self, value: &str) {
        let string = value.to_string();
        self.derived.push_field(string.as_bytes());
        self.buffer.push(string.into());
    }

    ///
    /// Add a derived uuid value to the buffer. Use flush to retrieve the buffer for writing.
    ///
    pub fn append_uuid(&mut self, value: Uuid) {
        let string = convert::uuid_to_string(value);
        self.derived.push_field(string.as_bytes());
        self.buffer.push(string.into());
    }

    ///
    /// Return the buffer as a csv::ByteRecord and clear it.
    ///
    pub fn flush(&mut self) -> csv::ByteRecord {
        let mut record = csv::ByteRecord::new();
        self.buffer.iter().for_each(|f| record.push_field(f));
        self.buffer.clear();
        record
    }

    ///
    /// Return all values for the record, padding empty cells.
    ///
    /// Data is returned in a displayable, string format.
    ///
    pub fn as_strings(&self) -> Vec<String> {
        self.schema.headers()
            .iter()
            .map(|header| self.get_as_string(header).unwrap_or_default())
            .collect()
    }

    ///
    /// If there is a value for this header, get the compacted byte format for it.
    ///
    pub fn get_as_bytes(&self, header: &str) -> Result<Option<Bytes>, MatcherError> {
        match self.schema.position_in_record(header, self) {
            Some(col) => self.get_bytes(*col),
            None =>  Ok(None),
        }
    }

    ///
    /// Merge the first non-None value from the source columns into a new column.
    ///
    pub fn merge_col_from(&mut self, source: &[String]) -> Result<(), MatcherError> {

        for header in source {
            let data_type = match self.schema.data_type(header) {
                Some(data_type) => data_type,
                None => continue, // There may be source columns whose files aren't present.
            };

            let value = match data_type {
                DataType::Unknown => return Err(MatcherError::UnknownDataTypeForHeader { header: header.into() }),
                DataType::Boolean => {
                    match self.get_bool(header)? {
                        Some(value) => convert::bool_to_string(value),
                        None => continue,
                    }
                },
                DataType::Datetime => {
                    match self.get_datetime(header)? {
                        Some(value) => convert::datetime_to_string(value),
                        None => continue,
                    }
                },
                DataType::Decimal => {
                    match self.get_decimal(header)? {
                        Some(value) => convert::decimal_to_string(value),
                        None => continue,
                    }
                },
                DataType::Integer => {
                    match self.get_int(header)? {
                        Some(value) => convert::int_to_string(value),
                        None => continue,
                    }
                },
                DataType::String => {
                    match self.get_string(header)? {
                        Some(value) => value,
                        None => continue,
                    }
                },
                DataType::Uuid => {
                    match self.get_uuid(header)? {
                        Some(value) => convert::uuid_to_string(value),
                        None => continue,
                    }
                },
            };

            self.buffer.push(value.clone().into());
            self.derived.push_field(value.as_bytes());
            return Ok(())
        }

        // If none of the source columns has any data, we still need to 'pad' the buffer with a blank field.
        self.buffer.push(Bytes::new());
        self.derived.push_field(String::new().as_bytes());
        Ok(())
    }
}


pub trait ByteMe {
    fn to_bytes(&self) -> Bytes;
}

impl ByteMe for Option<&[u8]> {
    fn to_bytes(&self) -> Bytes {
        let mut bm = BytesMut::new();
        if let Some(bytes) = *self {
            bm.put(bytes);
        }
        bm.freeze()
    }
}

impl ByteMe for &[u8] {
    fn to_bytes(&self) -> Bytes {
        let mut bm = BytesMut::new();
        bm.put(*self);
        bm.freeze()
    }
}

