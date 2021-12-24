use uuid::Uuid;
use bytes::Bytes;
use rust_decimal::Decimal;
use crate::error::MatcherError;
use core::data_type::{DataType, TRUE, FALSE};
use chrono::{DateTime, Utc, TimeZone, SecondsFormat};

fn unparseable_csv_err(data_type: DataType, bytes: Bytes) -> MatcherError {
    MatcherError::UnparseableCsvField { data_type: data_type.as_str().into(), bytes: format!("{:?}", bytes) }
}

pub fn csv_bytes_to_bool(bytes: Bytes) -> Result<bool, MatcherError> {
    if bytes == TRUE.as_bytes() {
        return Ok(true)
    }
    if bytes == FALSE.as_bytes() {
        return Ok(false)
    }
    Err(unparseable_csv_err(DataType::Boolean, bytes))
}

pub fn csv_bytes_to_datetime(bytes: Bytes) -> Result<u64, MatcherError> {
    let raw = String::from_utf8_lossy(&bytes);
    match DateTime::parse_from_rfc3339(&raw) {
        Ok(dt) => Ok(dt.timestamp_millis() as u64),
        Err(_) => Err(unparseable_csv_err(DataType::Datetime, bytes))
    }
}

pub fn csv_bytes_to_decimal(bytes: Bytes) -> Result<Decimal, MatcherError> {
    match String::from_utf8_lossy(&bytes).parse() {
        Ok(dec) => Ok(dec),
        Err(_) => Err(unparseable_csv_err(DataType::Decimal, bytes)),
    }
}

pub fn csv_bytes_to_int(bytes: Bytes) -> Result<i64, MatcherError> {
    match String::from_utf8_lossy(&bytes).parse() {
        Ok(int) => Ok(int),
        Err(_) => Err(unparseable_csv_err(DataType::Integer, bytes)),
    }
}

pub fn csv_bytes_to_string(bytes: Bytes) -> Result<String, MatcherError> {
    Ok(String::from_utf8_lossy(&bytes).into())
}

pub fn csv_bytes_to_uuid(bytes: Bytes) -> Result<Uuid, MatcherError> {
    match String::from_utf8_lossy(&bytes).parse() {
        Ok(uuid) => Ok(uuid),
        Err(_) => Err(unparseable_csv_err(DataType::Uuid, bytes)),
    }
}

pub fn bool_to_string(value: bool) -> String {
    format!("{}", value)
}

pub fn datetime_to_string(value: u64) -> String {
    let dt = Utc.timestamp(value as i64 / 1000, (value % 1000) as u32 * 1000000);
    dt.to_rfc3339_opts(SecondsFormat::Millis, true).to_string()
}

pub fn decimal_to_string(value: Decimal) -> String {
    format!("{}", value)
}

pub fn int_to_string(value: i64) -> String {
    format!("{}", value)
}

pub fn uuid_to_string(value: Uuid) -> String {
    value.to_hyphenated().to_string()
}


// TODO: Unit tests to convert....
