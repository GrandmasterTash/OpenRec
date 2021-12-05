use uuid::Uuid;
use rust_decimal::Decimal;
use super::data_type::DataType;
use csv::{Position, ByteRecord};
use std::{cell::Cell, fs::File};
use bytes::{BufMut, BytesMut, Bytes};
use crate::{model::{schema::GridSchema}, error::MatcherError, convert};

// TODO: Document the two forms of storing - compacted bytes and csv bytes...
// TODO: Document the derived vec and file reader approach to getting data...

pub struct Record {
    file_idx: u16,
    byte: u64, // Used to build a csv::Position.
    line: u64, // Used to build a csv::Position.
    derived: Vec<Bytes>, // Projected and merged column data. Retained. // TODO: Remove from record and pass in to append calls as part of 'DataAccessor'.
    matched: Cell<bool>,
}

impl Record {
    pub fn new(file_idx: u16, pos: &Position) -> Self {
        Self {
            byte: pos.byte(),
            line: pos.line(),
            file_idx,
            derived: Vec::new(),
            matched: Cell::new(false)
        }
    }

    pub fn row(&self) -> usize {
        self.line as usize
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
        self.derived
            .iter()
            .map(|field| field.len()).sum::<usize>() + std::mem::size_of::<Record>()
    }

    ///
    /// Build a csv::Position struct to seek this record in the file. The inflated struct is slightly
    /// larger than what is needed, hence we build-on-demand to keep the footprint of Record reduced.
    ///
    fn pos(&self) -> Position {
        let mut pos = Position::new();
        pos.set_byte(self.byte);
        pos.set_line(self.line);
        pos
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
    fn get_bytes(&self, col: isize, data_type: DataType, rdr: &mut csv::Reader<File>)
        -> Result<Option<Bytes>, MatcherError> {

        match col < 0 {
            true  => { // Derived column.
                match self.derived.get((col.abs() - 1) as usize) { // -1 -> 0, -2 -> 1, -3 -> 2, etc.
                    Some(bytes) if bytes.len() > 0 => {
                        let mut bm = BytesMut::new();
                        bm.put_slice(bytes);
                        Ok(Some(bm.freeze()))
                    },
                    Some(_) |
                    None    => Ok(None),
                }
            },
            false => { // File column.
                rdr.seek(self.pos())?;
                let mut buffer = csv::ByteRecord::new();
                rdr.read_byte_record(&mut buffer)?;

                match buffer.get(col as usize) {
                    Some(bytes) if bytes.len() > 0 => {
                        let mut bm = BytesMut::new();
                        bm.put(bytes);
                        let bytes = bm.freeze();

                        match data_type {
                            DataType::UNKNOWN => Err(MatcherError::UnknownDataTypeInColumn { column: col }),
                            DataType::BOOLEAN => Ok(Some(convert::bool_to_bytes(convert::csv_bytes_to_bool(bytes)?))),
                            DataType::BYTE => Ok(Some(convert::byte_to_bytes(convert::csv_bytes_to_byte(bytes)?))),
                            DataType::CHAR => Ok(Some(convert::char_to_bytes(convert::csv_bytes_to_char(bytes)?))),
                            DataType::DATE => Ok(Some(convert::date_to_bytes(convert::csv_bytes_to_date(bytes)?))),
                            DataType::DATETIME => Ok(Some(convert::datetime_to_bytes(convert::csv_bytes_to_datetime(bytes)?))),
                            DataType::DECIMAL => Ok(Some(convert::decimal_to_bytes(convert::csv_bytes_to_decimal(bytes)?))),
                            DataType::INTEGER => Ok(Some(convert::int_to_bytes(convert::csv_bytes_to_int(bytes)?))),
                            DataType::LONG => Ok(Some(convert::long_to_bytes(convert::csv_bytes_to_long(bytes)?))),
                            DataType::SHORT => Ok(Some(convert::short_to_bytes(convert::csv_bytes_to_short(bytes)?))),
                            DataType::STRING => Ok(Some(convert::string_to_bytes(&convert::csv_bytes_to_string(bytes)?))),
                            DataType::UUID => Ok(Some(convert::uuid_to_bytes(convert::csv_bytes_to_uuid(bytes)?))),
                        }
                    },
                    Some(_) |
                    None    => Ok(None),
                }
            },
        }
    }

    pub fn get_bool(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<bool>, MatcherError> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, DataType::BOOLEAN, rdr)? {
                return Ok(Some(convert::bytes_to_bool(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_byte(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<u8>, MatcherError> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, DataType::BYTE, rdr)? {
                return Ok(Some(convert::bytes_to_byte(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_char(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<char>, MatcherError> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, DataType::CHAR, rdr)? {
                return Ok(Some(convert::bytes_to_char(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_date(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<u64>, MatcherError> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, DataType::DATE, rdr)? {
                return Ok(Some(convert::bytes_to_date(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_datetime(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<u64>, MatcherError> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, DataType::DATETIME, rdr)? {
                return Ok(Some(convert::bytes_to_datetime(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_decimal(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<Decimal>, MatcherError> {
        if let Some(col) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*col, DataType::DECIMAL, rdr)? {
                return Ok(Some(convert::bytes_to_decimal(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_int(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<i32>, MatcherError> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, DataType::INTEGER, rdr)? {
                return Ok(Some(convert::bytes_to_int(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_long(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<i64>, MatcherError> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, DataType::LONG, rdr)? {
                return Ok(Some(convert::bytes_to_long(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_short(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<i16>, MatcherError> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, DataType::SHORT, rdr)? {
                return Ok(Some(convert::bytes_to_short(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_string(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<String>, MatcherError> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, DataType::STRING, rdr)? {
                return Ok(Some(convert::bytes_to_string(bytes)?))
            }
        }
        Ok(None)
    }

    pub fn get_uuid(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<Option<Uuid>, MatcherError> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.get_bytes(*column, DataType::UUID, rdr)? {
                return Ok(Some(convert::bytes_to_uuid(bytes)?))
            }
        }
        Ok(None)
    }

    ///
    /// Get the value in the column as a displayable string - if no value is present an empty string is returned.
    ///
    pub fn get_as_string(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Result<String, MatcherError> {
        match schema.data_type(header) {
            Some(data_type) => match data_type {
                DataType::UNKNOWN => Err(MatcherError::UnknownDataTypeForHeader { header: header.into() }),
                DataType::BOOLEAN => Ok(self.get_bool(header, schema, rdr)?.map(|v|convert::bool_to_string(v)).unwrap_or(String::default())),
                DataType::BYTE => Ok(self.get_byte(header, schema, rdr)?.map(|v|convert::byte_to_string(v)).unwrap_or(String::default())),
                DataType::CHAR => Ok(self.get_char(header, schema, rdr)?.map(|v|convert::char_to_string(v)).unwrap_or(String::default())),
                DataType::DATE => Ok(self.get_date(header, schema, rdr)?.map(|v|convert::date_to_string(v)).unwrap_or(String::default())),
                DataType::DATETIME => Ok(self.get_datetime(header, schema, rdr)?.map(|v|convert::datetime_to_string(v)).unwrap_or(String::default())),
                DataType::DECIMAL => Ok(self.get_decimal(header, schema, rdr)?.map(|v|convert::decimal_to_string(v)).unwrap_or(String::default())),
                DataType::INTEGER => Ok(self.get_int(header, schema, rdr)?.map(|v|convert::int_to_string(v)).unwrap_or(String::default())),
                DataType::LONG => Ok(self.get_long(header, schema, rdr)?.map(|v|convert::long_to_string(v)).unwrap_or(String::default())),
                DataType::SHORT => Ok(self.get_short(header, schema, rdr)?.map(|v|convert::short_to_string(v)).unwrap_or(String::default())),
                DataType::STRING => Ok(self.get_string(header, schema, rdr)?.unwrap_or(String::default())),
                DataType::UUID => Ok(self.get_uuid(header, schema, rdr)?.map(|v|convert::uuid_to_string(v)).unwrap_or(String::default())),
            },
            None => Ok(String::default()),
        }
    }

    pub fn append_bool(&mut self, value: bool) {
        self.derived.push(convert::bool_to_bytes(value));
    }

    pub fn append_byte(&mut self, value: u8) {
        self.derived.push(convert::byte_to_bytes(value));
    }

    pub fn append_char(&mut self, value: char) {
        self.derived.push(convert::char_to_bytes(value));
    }

    pub fn append_date(&mut self, value: u64) {
        self.derived.push(convert::date_to_bytes(value));
    }

    pub fn append_datetime(&mut self, value: u64) {
        self.derived.push(convert::datetime_to_bytes(value));
    }

    pub fn append_decimal(&mut self, value: Decimal) {
        self.derived.push(convert::decimal_to_bytes(value));
    }

    pub fn append_int(&mut self, value: i32) {
        self.derived.push(convert::int_to_bytes(value));
    }

    pub fn append_long(&mut self, value: i64) {
        self.derived.push(convert::long_to_bytes(value));
    }

    pub fn append_short(&mut self, value: i16) {
        self.derived.push(convert::short_to_bytes(value));
    }

    pub fn append_string(&mut self, value: &str) {
        self.derived.push(convert::string_to_bytes(value));
    }

    pub fn append_uuid(&mut self, value: Uuid) {
        self.derived.push(convert::uuid_to_bytes(value));
    }

    ///
    /// Return all values for the record, padding empty cells.
    ///
    /// Data is returned in a displayable, string format.
    ///
    pub fn as_strings(&self, schema: &GridSchema, rdr: &mut csv::Reader<File>) -> Vec<String> {
        schema.headers()
            .iter()
            .map(|header| self.get_as_string(header, schema, rdr).unwrap_or_default())
            .collect()
    }

    ///
    /// If there is a value for this header, get the compacted byte format for it.
    ///
    pub fn get_compact_bytes(&self, header: &str, schema: &GridSchema, rdr: &mut csv::Reader<File>)
        -> Result<Option<Bytes>, MatcherError> {

        match schema.position_in_record(header, self) {
            Some(col) => {
                match schema.data_type(header) {
                    Some(data_type) => self.get_bytes(*col, *data_type, rdr),
                    None => Ok(None),
                }
            },
            None => Ok(None),
        }
    }

    ///
    /// Merge the first non-None value from the source columns into a new column.
    ///
    pub fn merge_col_from(&mut self, source: &[String], schema: &GridSchema, rdr: &mut csv::Reader<File>)
        -> Result<(), MatcherError> {

        for header in source {
            match self.get_compact_bytes(header, schema, rdr)? {
                Some(raw) => {
                    self.derived.push(raw);
                    return Ok(())
                },
                None => continue,
            }
        }

        // If we've not found a value, we'll still need to put a blank in there column
        // to ensure the row has the correct number of columns.
        self.derived.push(Bytes::new());
        Ok(())
    }
}
