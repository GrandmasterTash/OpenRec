use serde::Deserialize;

///
/// Logical/business data-type for any given csv column.
///
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash)]
pub enum DataType {
    UNKNOWN,  // Unable to map short-code to a known value.
    BOOLEAN,  // 1,0 - uses byte.
    DATETIME, // 8-byte, long, millis-since epoch.
    DECIMAL,  // 8-byte (rust-decimal).
    INTEGER,  // 8-byte (-2^63 <-> 2^63-1).
    STRING,   // Null-terminated, UTF-8.
    UUID,     // 16-byte (UUID). A colum is added in memory if none is present in source file.
}

pub const TRUE: &str  = "1";
pub const FALSE: &str = "0";

impl DataType {
    pub fn to_str(&self) -> &str {
        self.into()
    }
}


impl From<&str> for DataType {
    fn from(value: &str) -> Self {
        match value {
            "BO" => DataType::BOOLEAN,
            "DT" => DataType::DATETIME,
            "DE" => DataType::DECIMAL,
            "IN" => DataType::INTEGER,
            "ST" => DataType::STRING,
            "ID" => DataType::UUID,
            _    => DataType::UNKNOWN
        }
    }
}

impl From<&DataType> for &str {
    fn from(dt: &DataType) -> Self {
        match dt {
            DataType::UNKNOWN  => "ER",
            DataType::BOOLEAN  => "BO",
            DataType::DATETIME => "DT",
            DataType::DECIMAL  => "DE",
            DataType::INTEGER  => "IN",
            DataType::STRING   => "ST",
            DataType::UUID     => "ID",
        }
    }
}
