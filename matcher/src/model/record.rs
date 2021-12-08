use uuid::Uuid;
use bytes::Bytes;
use std::cell::Cell;
use rust_decimal::Decimal;
use csv::{Position, ByteRecord};
use super::{data_type::DataType, data_accessor::DataAccessor};
use crate::{model::schema::GridSchema, error::MatcherError, convert};

///
/// A record is essentially a point to one or two CSV rows on disk.
/// One index points to the original CSV file of data and the second can point to the derived (calculated)
/// data for projected columns, merged columns, etc.
///
pub struct Record {
    file_idx: u16,
    data_idx: Index,
    derived_idx: Index,
    matched: Cell<bool>,
}

///
/// Used to create a csv::Position to locate the record in a file.
///
struct Index {
    byte: u64,
    line: u32
}

impl Record {
    pub fn new(file_idx: u16, pos: &Position) -> Self {
        Self {
            data_idx: Index { byte: pos.byte(), line: pos.line() as u32 },
            derived_idx: Index { byte: 0, line: 0 },
            file_idx,
            matched: Cell::new(false)
        }
    }

    pub fn row(&self) -> usize {
        self.data_idx.line as usize
    }

    pub fn file_idx(&self) -> usize {
        self.file_idx as usize
    }

    pub fn matched(&self) -> bool {
        self.matched.get()
    }

    pub fn set_matched(&self) {
        self.matched.set(true);
    }

    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Record>()
    }

    ///
    /// Build a csv::Position struct to seek this record in the file. The inflated struct is slightly
    /// larger than what is needed, hence we build-on-demand to keep the footprint of Record reduced.
    ///
    pub fn pos(&self) -> Position {
        let mut pos = Position::new();
        pos.set_byte(self.data_idx.byte);
        pos.set_line(self.data_idx.line as u64);
        pos
    }

    ///
    /// Similar to pos, this points to the .dervived data file.
    ///
    pub fn derived_pos(&self) -> Option<Position> {
        if self.derived_idx.line == 0 {
            return None
        }
        let mut pos = Position::new();
        pos.set_byte(self.derived_idx.byte);
        pos.set_line(self.derived_idx.line as u64);
        Some(pos)
    }

    pub fn set_derived_pos(&mut self, pos: &Position) {
        self.derived_idx.byte = pos.byte();
        self.derived_idx.line = pos.line() as u32;
    }

    ///
    /// Read the source record from the CSV file.
    ///
    pub fn read_csv_record(&self, rdr: &mut csv::Reader<std::fs::File>) -> Result<ByteRecord, MatcherError> {
        let mut record = csv::ByteRecord::new();
        rdr.seek(self.pos())?;
        rdr.read_byte_record(&mut record)?;
        Ok(record)
    }

    ///
    /// Get the derived value, or load the real value from the backing csv reader.
    ///
    fn get_bytes(&self, col: isize, accessor: &mut DataAccessor) -> Result<Option<Bytes>, MatcherError> {
        match col < 0 {
            true  => { // Derived column.
                // These use negative indexes in the Grid and must be translated to a real
                // CSV column. -1 -> 0, -2 -> 1, -3 -> 2, etc.
                match accessor.derived_accessor().get((col.abs() - 1) as usize, &self)? {
                    Some(bytes) if bytes.len() > 0 => Ok(Some(bytes)),
                    Some(_) |
                    None    => Ok(None),
                }
            },
            false => { // File column.
                match accessor.get(col as usize, &self)? {
                    Some(bytes) if bytes.len() > 0 => Ok(Some(bytes)),
                    Some(_) |
                    None    => Ok(None),
                }
            },
        }
    }

    pub fn get_bool(&self, header: &str, accessor: &mut DataAccessor) -> Result<Option<bool>, MatcherError> {
        if let Some(column) = accessor.schema().position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, accessor)? {
                return Ok(Some(convert::csv_bytes_to_bool(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_datetime(&self, header: &str, accessor: &mut DataAccessor) -> Result<Option<u64>, MatcherError> {
        if let Some(column) = accessor.schema().position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, accessor)? {
                return Ok(Some(convert::csv_bytes_to_datetime(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_decimal(&self, header: &str, accessor: &mut DataAccessor) -> Result<Option<Decimal>, MatcherError> {
        if let Some(col) = accessor.schema().position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*col, accessor)? {
                return Ok(Some(convert::csv_bytes_to_decimal(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_int(&self, header: &str, accessor: &mut DataAccessor) -> Result<Option<i64>, MatcherError> {
        if let Some(column) = accessor.schema().position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, accessor)? {
                return Ok(Some(convert::csv_bytes_to_int(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_string(&self, header: &str, accessor: &mut DataAccessor) -> Result<Option<String>, MatcherError> {
        if let Some(column) = accessor.schema().position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, accessor)? {
                return Ok(Some(convert::csv_bytes_to_string(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_uuid(&self, header: &str, accessor: &mut DataAccessor) -> Result<Option<Uuid>, MatcherError> {
        if let Some(column) = accessor.schema().position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, accessor)? {
                return Ok(Some(convert::csv_bytes_to_uuid(bytes)?))
            }
        }
        Ok(None)
    }

    ///
    /// Get the value in the column as a displayable string - if no value is present an empty string is returned.
    ///
    pub fn get_as_string(&self, header: &str, accessor: &mut DataAccessor) -> Result<String, MatcherError> {
        match accessor.schema().data_type(header) {
            Some(data_type) => match data_type {
                DataType::UNKNOWN => Err(MatcherError::UnknownDataTypeForHeader { header: header.into() }),
                DataType::BOOLEAN => Ok(self.get_bool(header, accessor)?.map(|v|convert::bool_to_string(v)).unwrap_or(String::default())),
                DataType::DATETIME => Ok(self.get_datetime(header, accessor)?.map(|v|convert::datetime_to_string(v)).unwrap_or(String::default())),
                DataType::DECIMAL => Ok(self.get_decimal(header, accessor)?.map(|v|convert::decimal_to_string(v)).unwrap_or(String::default())),
                DataType::INTEGER => Ok(self.get_int(header, accessor)?.map(|v|convert::int_to_string(v)).unwrap_or(String::default())),
                DataType::STRING => Ok(self.get_string(header, accessor)?.unwrap_or(String::default())),
                DataType::UUID => Ok(self.get_uuid(header, accessor)?.map(|v|convert::uuid_to_string(v)).unwrap_or(String::default())),
            },
            None => Ok(String::default()),
        }
    }

    pub fn append_bool(&self, value: bool, accessor: &mut DataAccessor) {
        accessor.derived_accessor().append(convert::bool_to_string(value).into());
    }

    pub fn append_datetime(&self, value: u64, accessor: &mut DataAccessor) {
        accessor.derived_accessor().append(convert::datetime_to_string(value).into());
    }

    pub fn append_decimal(&self, value: Decimal, accessor: &mut DataAccessor) {
        accessor.derived_accessor().append(convert::decimal_to_string(value).into());
    }

    pub fn append_int(&self, value: i64, accessor: &mut DataAccessor) {
        accessor.derived_accessor().append(convert::int_to_string(value).into());
    }

    pub fn append_string(&self, value: &str, accessor: &mut DataAccessor) {
        accessor.derived_accessor().append(value.to_string().into());
    }

    pub fn append_uuid(&self, value: Uuid, accessor: &mut DataAccessor) {
        accessor.derived_accessor().append(convert::uuid_to_string(value).into());
    }

    pub fn flush(&self, accessor: &mut DataAccessor) -> Result<(), MatcherError> {
        accessor.derived_accessor().flush(self.file_idx)
    }

    ///
    /// Return all values for the record, padding empty cells.
    ///
    /// Data is returned in a displayable, string format.
    ///
    pub fn as_strings(&self, schema: &GridSchema, accessor: &mut DataAccessor) -> Vec<String> {
        schema.headers()
            .iter()
            .map(|header| self.get_as_string(header, accessor).unwrap_or_default())
            .collect()
    }

    ///
    /// If there is a value for this header, get the compacted byte format for it.
    ///
    pub fn get_compact_bytes(&self, header: &str, accessor: &mut DataAccessor)
        -> Result<Option<Bytes>, MatcherError> {

        match accessor.schema().position_in_record(header, self) {
            Some(col) => self.get_bytes(*col, accessor),
            None =>  Ok(None),
        }
    }

    ///
    /// Merge the first non-None value from the source columns into a new column.
    ///
    pub fn merge_col_from(&self, source: &[String], accessor: &mut DataAccessor)
        -> Result<(), MatcherError> {

        for header in source {
            let data_type = match accessor.schema().data_type(header) {
                Some(data_type) => data_type,
                None => return Err(MatcherError::UnknownDataTypeForHeader { header: header.into() }),
            };

            let value = match data_type {
                DataType::UNKNOWN => return Err(MatcherError::UnknownDataTypeForHeader { header: header.into() }),
                DataType::BOOLEAN => {
                    match self.get_bool(header, accessor)? {
                        Some(value) => convert::bool_to_string(value),
                        None => continue,
                    }
                },
                DataType::DATETIME => {
                    match self.get_datetime(header, accessor)? {
                        Some(value) => convert::datetime_to_string(value),
                        None => continue,
                    }
                },
                DataType::DECIMAL => {
                    match self.get_decimal(header, accessor)? {
                        Some(value) => convert::decimal_to_string(value),
                        None => continue,
                    }
                },
                DataType::INTEGER => {
                    match self.get_int(header, accessor)? {
                        Some(value) => convert::int_to_string(value),
                        None => continue,
                    }
                },
                DataType::STRING => {
                    match self.get_string(header, accessor)? {
                        Some(value) => value,
                        None => continue,
                    }
                },
                DataType::UUID => {
                    match self.get_uuid(header, accessor)? {
                        Some(value) => convert::uuid_to_string(value),
                        None => continue,
                    }
                },
            };

            accessor.derived_accessor().append(value.into());
            return Ok(())
        }

        // If none of the source columns has any data, we still need to 'pad' the buffer with a blank field.
        accessor.derived_accessor().append(Bytes::new());
        Ok(())
    }
}
