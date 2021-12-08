use uuid::Uuid;
use bytes::{Bytes, Buf};
use rust_decimal::Decimal;
use chrono::{DateTime, Utc, TimeZone, SecondsFormat};
use crate::{model::data_type::{TRUE, DataType, FALSE}, error::MatcherError};



fn unparseable_csv_err(data_type: DataType, bytes: Bytes) -> MatcherError {
    MatcherError::UnparseableCsvField { data_type: data_type.to_str().into(), bytes: format!("{:?}", bytes) }
}

pub fn csv_bytes_to_bool(bytes: Bytes) -> Result<bool, MatcherError> {
    if bytes == TRUE.as_bytes() {
        return Ok(true)
    }
    if bytes == FALSE.as_bytes() {
        return Ok(false)
    }
    Err(unparseable_csv_err(DataType::BOOLEAN, bytes))
}

pub fn csv_bytes_to_byte(bytes: Bytes) -> Result<u8, MatcherError> {
    Ok(bytes.clone().get_u8())
}

pub fn csv_bytes_to_char(bytes: Bytes) -> Result<char, MatcherError> {
    String::from_utf8_lossy(&bytes)
        .chars()
        .next()
        .ok_or(unparseable_csv_err(DataType::CHAR, bytes))
}

pub fn csv_bytes_to_date(bytes: Bytes) -> Result<u64, MatcherError> {
    return String::from_utf8_lossy(&bytes).parse().map_err(|_| unparseable_csv_err(DataType::DATE, bytes))
}

pub fn csv_bytes_to_datetime(bytes: Bytes) -> Result<u64, MatcherError> {
    let raw = String::from_utf8_lossy(&bytes);
    return match DateTime::parse_from_rfc3339(&raw) {
        Ok(dt) => Ok(dt.timestamp_millis() as u64),
        Err(_) => Err(unparseable_csv_err(DataType::DATETIME, bytes))
    }
}

pub fn csv_bytes_to_decimal(bytes: Bytes) -> Result<Decimal, MatcherError> {
    match String::from_utf8_lossy(&bytes).parse() {
        Ok(dec) => Ok(dec),
        Err(_) => Err(unparseable_csv_err(DataType::DECIMAL, bytes)),
    }
}

pub fn csv_bytes_to_int(bytes: Bytes) -> Result<i32, MatcherError> {
    match String::from_utf8_lossy(&bytes).parse() {
        Ok(int) => Ok(int),
        Err(_) => Err(unparseable_csv_err(DataType::INTEGER, bytes)),
    }
}

pub fn csv_bytes_to_long(bytes: Bytes) -> Result<i64, MatcherError> {
    match String::from_utf8_lossy(&bytes).parse() {
        Ok(long) => Ok(long),
        Err(_) => Err(unparseable_csv_err(DataType::LONG, bytes)),
    }
}

pub fn csv_bytes_to_short(bytes: Bytes) -> Result<i16, MatcherError> {
    match String::from_utf8_lossy(&bytes).parse() {
        Ok(short) => Ok(short),
        Err(_) => Err(unparseable_csv_err(DataType::SHORT, bytes)),
    }
}

pub fn csv_bytes_to_string(bytes: Bytes) -> Result<String, MatcherError> {
    Ok(String::from_utf8_lossy(&bytes).into())
}

pub fn csv_bytes_to_uuid(bytes: Bytes) -> Result<Uuid, MatcherError> {
    match String::from_utf8_lossy(&bytes).parse() {
        Ok(uuid) => Ok(uuid),
        Err(_) => Err(unparseable_csv_err(DataType::UUID, bytes)),
    }
}



// fn unparseable_bytes_err(data_type: DataType, bytes: Bytes) -> MatcherError {
//     MatcherError::UnparseableInternalBytesField { data_type: data_type.to_str().into(), bytes: format!("{:?}", bytes) }
// }

// pub fn bytes_to_bool(bytes: Bytes) -> Result<bool, MatcherError> {
//     match bytes.clone().get_u8() {
//         0 => Ok(false),
//         1 => Ok(true),
//         _ => Err(unparseable_bytes_err(DataType::BOOLEAN, bytes))
//     }
// }

// pub fn bytes_to_byte(bytes: Bytes) -> Result<u8, MatcherError> {
//     Ok(bytes.clone().get_u8())
// }

// pub fn bytes_to_char(bytes: Bytes) -> Result<char, MatcherError> {
//     String::from_utf8_lossy(&bytes)
//         .chars()
//         .next()
//         .ok_or(unparseable_csv_err(DataType::CHAR, bytes))
// }

// pub fn bytes_to_date(bytes: Bytes) -> Result<u64, MatcherError> {
//     Ok(bytes.clone().get_u64())
// }

// pub fn bytes_to_datetime(bytes: Bytes) -> Result<u64, MatcherError> {
//     Ok(bytes.clone().get_u64())
// }

// pub fn bytes_to_decimal(bytes: Bytes) -> Result<Decimal, MatcherError> {
//     let array = match bytes.to_vec().try_into() {
//         Ok(raw) => raw,
//         Err(_) => return Err(unparseable_bytes_err(DataType::DECIMAL, bytes)),
//     };
//     Ok(Decimal::deserialize(array))
// }

// pub fn bytes_to_int(bytes: Bytes) -> Result<i32, MatcherError> {
//     Ok(bytes.clone().get_i32())
// }

// pub fn bytes_to_long(bytes: Bytes) -> Result<i64, MatcherError> {
//     Ok(bytes.clone().get_i64())
// }

// pub fn bytes_to_short(bytes: Bytes) -> Result<i16, MatcherError> {
//     Ok(bytes.clone().get_i16())
// }

// pub fn bytes_to_string(bytes: Bytes) -> Result<String, MatcherError> {
//     Ok(String::from_utf8_lossy(&bytes).into())
// }

// pub fn bytes_to_uuid(bytes: Bytes) -> Result<Uuid, MatcherError> {
//     let array = match bytes.to_vec().try_into() {
//         Ok(raw) => raw,
//         Err(_) => return Err(unparseable_bytes_err(DataType::UUID, bytes)),
//     };
//     Ok(Uuid::from_bytes(array))
// }



// pub fn bool_to_bytes(value: bool) -> Bytes {
//     match value {
//         true  => Bytes::from_static(&[0x01]),
//         false => Bytes::from_static(&[0x00]),
//     }
// }

// pub fn byte_to_bytes(value: u8) -> Bytes {
//     let mut bytes = BytesMut::new();
//     bytes.put_u8(value);
//     bytes.freeze()
// }

// pub fn char_to_bytes(value: char) -> Bytes {
//     Bytes::from(value.to_string())
// }

// pub fn date_to_bytes(value: u64) -> Bytes {
//     let mut bytes = BytesMut::new();
//     bytes.put_u64(value);
//     bytes.freeze()
// }

// pub fn datetime_to_bytes(value: u64) -> Bytes {
//     let mut bytes = BytesMut::new();
//     bytes.put_u64(value);
//     bytes.freeze()
// }

// pub fn decimal_to_bytes(value: Decimal) -> Bytes {
//     let mut bytes = BytesMut::new();
//     bytes.put_slice(&value.serialize());
//     bytes.freeze()
// }

// pub fn int_to_bytes(value: i32) -> Bytes {
//     let mut bytes = BytesMut::new();
//     bytes.put_i32(value);
//     bytes.freeze()
// }

// pub fn long_to_bytes(value: i64) -> Bytes {
//     let mut bytes = BytesMut::new();
//     bytes.put_i64(value);
//     bytes.freeze()
// }

// pub fn short_to_bytes(value: i16) -> Bytes {
//     let mut bytes = BytesMut::new();
//     bytes.put_i16(value);
//     bytes.freeze()
// }

// pub fn string_to_bytes(value: &str) -> Bytes {
//     let value: String = value.into();
//     let mut bytes = BytesMut::new();
//     bytes.put_slice(value.as_bytes());
//     bytes.freeze()
// }

// pub fn uuid_to_bytes(value: Uuid) -> Bytes {
//     let mut bytes = BytesMut::new();
//     bytes.put_slice(value.as_bytes());
//     bytes.freeze()
// }


pub fn bool_to_string(value: bool) -> String {
    format!("{}", value)
}

pub fn byte_to_string(value: u8) -> String {
    format!("{}", value)
}

pub fn char_to_string(value: char) -> String {
    format!("{}", value)
}

pub fn date_to_string(value: u64) -> String {
    let dt = Utc.timestamp(value as i64, 0);
    format!("{}", dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

pub fn datetime_to_string(value: u64) -> String {
    let dt = Utc.timestamp(value as i64 / 1000, (value % 1000) as u32 * 1000000);
    format!("{}", dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

pub fn decimal_to_string(value: Decimal) -> String {
    format!("{}", value)
}

pub fn int_to_string(value: i32) -> String {
    format!("{}", value)
}

pub fn long_to_string(value: i64) -> String {
    format!("{}", value)
}

pub fn short_to_string(value: i16) -> String {
    format!("{}", value)
}

pub fn uuid_to_string(value: Uuid) -> String {
    format!("{}", value.to_hyphenated().to_string())
}


// TODO: Unit tests to conert....
