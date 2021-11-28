use uuid::Uuid;
use std::cell::Cell;
use rust_decimal::Decimal;
use bytes::{BufMut, BytesMut};
use crate::{data_type::{self, TRUE}, schema::GridSchema};

#[derive(Debug)]
pub struct Record {
    row: usize, // The line number in the file. 1 and 2 are headers, so the first row will always be 3.
    file_idx: usize,
    schema_idx: usize,
    inner: csv::ByteRecord,
    matched: Cell<bool>,
}

impl Record {
    pub fn new(file_idx: usize, schema_idx: usize, row: usize, inner: csv::ByteRecord) -> Self {
        Self { row, file_idx, schema_idx, inner, matched: Cell::new(false) }
    }

    pub fn row(&self) -> usize {
        self.row
    }

    pub fn schema_idx(&self) -> usize {
        self.schema_idx
    }

    pub fn file_idx(&self) -> usize {
        self.file_idx
    }

    pub fn inner(&self) -> &csv::ByteRecord {
        &self.inner
    }

    pub fn matched(&self) -> bool {
        self.matched.get()
    }

    pub fn set_matched(&self) {
        self.matched.set(true);
    }

    pub fn get_bool(&self, header: &str, schema: &GridSchema) -> Option<bool> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return Some(TRUE.as_bytes() == bytes)
            }
        }
        None
    }

    pub fn get_byte(&self, header: &str, schema: &GridSchema) -> Option<u8> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return Some(bytes[0])
            }
        }
        None
    }

    pub fn get_char(&self, header: &str, schema: &GridSchema) -> Option<char> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return String::from_utf8_lossy(bytes).chars().next()
            }
        }
        None
    }

    pub fn get_date(&self, header: &str, schema: &GridSchema) -> Option<u64> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return String::from_utf8_lossy(bytes).parse().ok()
            }
        }
        None
    }

    pub fn get_datetime(&self, header: &str, schema: &GridSchema) -> Option<u64> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return String::from_utf8_lossy(bytes).parse().ok()
            }
        }
        None
    }

    pub fn get_decimal(&self, header: &str, schema: &GridSchema) -> Option<Decimal> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return String::from_utf8_lossy(bytes).parse().ok()
            }
        }
        None
    }

    pub fn get_int(&self, header: &str, schema: &GridSchema) -> Option<i32> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return String::from_utf8_lossy(bytes).parse().ok()
            }
        }
        None
    }

    pub fn get_long(&self, header: &str, schema: &GridSchema) -> Option<i64> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return String::from_utf8_lossy(bytes).parse().ok()
            }
        }
        None
    }

    pub fn get_short(&self, header: &str, schema: &GridSchema) -> Option<i16> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return String::from_utf8_lossy(bytes).parse().ok()
            }
        }
        None
    }

    pub fn get_string(&self, header: &str, schema: &GridSchema) -> Option<&str> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return std::str::from_utf8(bytes).ok()
            }
        }
        None
    }

    pub fn get_uuid(&self, header: &str, schema: &GridSchema) -> Option<Uuid> {
        if let Some(column) = schema.position_in_record(header, self) {
            if let Some(bytes) = self.inner.get(*column) {
                return String::from_utf8_lossy(bytes).parse().ok()
            }
        }
        None
    }

    pub fn append_bool(&mut self, value: bool) {
        self.inner.push_field(format!("{}", match value {
            true  => data_type::TRUE,
            false => data_type::FALSE,
        }).as_bytes())
    }

    pub fn append_byte(&mut self, value: u8) {
        self.inner.push_field(format!("{}", value).as_bytes())
    }

    pub fn append_char(&mut self, value: char) {
        self.inner.push_field(format!("{}", value).as_bytes())
    }

    pub fn append_date(&mut self, value: u64) {
        self.inner.push_field(format!("{}", value).as_bytes())
    }

    pub fn append_datetime(&mut self, value: u64) {
        self.inner.push_field(format!("{}", value).as_bytes())
    }

    pub fn append_decimal(&mut self, value: Decimal) {
        self.inner.push_field(format!("{}", value).as_bytes())
    }

    pub fn append_int(&mut self, value: i32) {
        self.inner.push_field(format!("{}", value).as_bytes())
    }

    pub fn append_long(&mut self, value: i64) {
        self.inner.push_field(format!("{}", value).as_bytes())
    }

    pub fn append_short(&mut self, value: i16) {
        self.inner.push_field(format!("{}", value).as_bytes())
    }

    pub fn append_string(&mut self, value: &str) {
        self.inner.push_field(format!("{}", value).as_bytes())
    }

    pub fn append_uuid(&mut self, value: Uuid) {
        self.inner.push_field(format!("{}", value.to_hyphenated()).as_bytes())
    }

    ///
    /// Copies the raw byte value in the column specified - if there is any.
    ///
    pub fn get_bytes_copy(&self, header: &str, schema: &GridSchema) -> Option<Vec<u8>> {
        match schema.position_in_record(header, self) {
            Some(column) => {
                match self.inner.get(*column) {
                    Some(raw) if raw.len() > 0 => {
                        let mut buf = BytesMut::with_capacity(raw.len());
                        buf.put(raw);
                        // TODO: Use.freeze
                        Some(buf.to_vec())
                    },
                    Some(_) => None,
                    None => None,
                }
            },
            None => None,
        }
    }

    ///
    /// Merge the first non-None value from the source columns into a new column.
    ///
    pub fn merge_from(&mut self, source: &[String], schema: &GridSchema) {
        for header in source {
            match self.get_bytes_copy(header, schema) {
                Some(raw) => {
                    self.inner.push_field(&raw);
                    return
                },
                None => continue,
            }
        }

        // If we've not found a value, we'll still need to put a blank in there column
        // to ensure the row has the correct number of columns.
        self.append_string("");
    }
}
