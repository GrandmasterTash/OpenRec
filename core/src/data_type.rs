use serde::Deserialize;

///
/// Logical/business data-type for any given csv column.
///
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash)]
pub enum DataType {
    Unknown,  // Unable to map short-code to a known value.
    Boolean,  // 1,0 - uses byte.
    Datetime, // 8-byte, long, millis-since epoch.
    Decimal,  // 8-byte (rust-decimal).
    Integer,  // 8-byte (-2^63 <-> 2^63-1).
    String,   // Null-terminated, UTF-8.
    Uuid,     // 16-byte (UUID). A colum is added in memory if none is present in source file.
}

pub const TRUE: &str  = "1";
pub const FALSE: &str = "0";

impl DataType {
    pub fn as_str(&self) -> &str {
        self.into()
    }
}


impl From<&str> for DataType {
    fn from(value: &str) -> Self {
        match value {
            "BO" => DataType::Boolean,
            "DT" => DataType::Datetime,
            "DE" => DataType::Decimal,
            "IN" => DataType::Integer,
            "ST" => DataType::String,
            "ID" => DataType::Uuid,
            "??" => DataType::Unknown,
            eh => panic!("REALLY unknown data-type '{}'", eh)
        }
    }
}

impl From<&DataType> for &str {
    fn from(dt: &DataType) -> Self {
        match dt {
            DataType::Unknown  => "??",
            DataType::Boolean  => "BO",
            DataType::Datetime => "DT",
            DataType::Decimal  => "DE",
            DataType::Integer  => "IN",
            DataType::String   => "ST",
            DataType::Uuid     => "ID",
        }
    }
}
