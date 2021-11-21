use uuid::Uuid;
use rust_decimal::Decimal;
use crate::{data_type::{self, TRUE}, schema::GridSchema};

#[derive(Debug)]
pub struct Record {
    file_idx: usize,
    schema_idx: usize,
    inner: csv::ByteRecord,
}

impl Record {
    pub fn new(file_idx: usize, schema_idx: usize, inner: csv::ByteRecord) -> Self {
        Self { file_idx, schema_idx, inner }
    }

    pub fn schema_idx(&self) -> usize {
        self.schema_idx
    }

    pub fn inner(&self) -> &csv::ByteRecord {
        &self.inner
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
}

// TODO: Unit tests.