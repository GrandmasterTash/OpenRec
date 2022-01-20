use regex::Regex;
use bytes::Bytes;
use rlua::FromLuaMulti;
use rust_decimal::Decimal;
use lazy_static::lazy_static;
use crate::{error::JetwashError, analyser};
use chrono::{Utc, TimeZone, SecondsFormat};
use core::{data_type::DataType, lua::LuaDecimal, charter::ColumnMapping};

lazy_static! {
    static ref DATES: Vec<Regex> = vec!(
        Regex::new(r"^(\d{1,4})-(\d{1,4})-(\d{1,4})$").expect("bad regex d-m-y"),      // d-m-y
        Regex::new(r"^(\d{1,4})/(\d{1,4})/(\d{1,4})$").expect("bad regex d/m/y"),      // d/m/y
        Regex::new(r"^(\d{1,4})\\(\d{1,4})\\(\d{1,4})$").expect(r#"bad regex d\m\y"#), // d\m\y
        Regex::new(r"^(\d{1,4})\W(\d{1,4})\W(\d{1,4})$").expect("bad regex d m y"),    // d m y
    );
}

///
/// Use Lua to generate a new column on the incoming file.
///
pub fn eval_typed_lua(lua_ctx: &rlua::Context, lua: &str, as_a: DataType) -> Result<String, JetwashError> {
    let mapped = match as_a {
        DataType::Unknown => panic!("Can't eval if data-type is Unknown"),
        DataType::Boolean => bool_to_string(eval(lua_ctx, lua)?),
        DataType::Datetime => datetime_to_string(eval(lua_ctx, lua)?),
        DataType::Decimal => decimal_to_string(eval::<LuaDecimal>(lua_ctx, lua)?.0),
        DataType::Integer => int_to_string(eval(lua_ctx, lua)?),
        DataType::String => eval(lua_ctx, lua)?,
        DataType::Uuid => eval(lua_ctx, lua)?,
    };
    Ok(mapped)
}

fn bool_to_string(value: bool) -> String {
    format!("{}", value)
}

fn datetime_to_string(value: u64) -> String {
    let dt = Utc.timestamp(value as i64 / 1000, (value % 1000) as u32 * 1000000);
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn decimal_to_string(value: Decimal) -> String {
    format!("{}", value)
}

fn int_to_string(value: i64) -> String {
    format!("{}", value)
}

///
/// Perform a column mapping on the value specified.
///
/// Mappings could be raw Lua script or one of a preset help mappings, trim, dmy, etc.
///
pub fn map_field(lua_ctx: &rlua::Context, mapping: &ColumnMapping, original: Bytes) -> Result<Bytes, JetwashError> {
    // Provide the original value to the Lua script as a string variable called 'value'.
    let value = String::from_utf8_lossy(&original).to_string();

    let mapped: String = match mapping {
        ColumnMapping::Map { from, as_a, .. } => {
            // Perform some Lua to evaluate the mapped value to push into the new record.
            lua_ctx.globals().set("value", value)?;
            eval_typed_lua(lua_ctx, from, *as_a)?
        },

        ColumnMapping::Dmy( _column ) => {
            // If there's a value, try to parse as d/m/y, then d-m-y, then d\m\y, then d m y.
            match date_captures(&value) {
                Some(captures) => {
                    let dt = Utc.ymd(captures.2 as i32, captures.1, captures.0).and_hms_milli(0, 0, 0, 0);
                    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                },
                None => value,
            }
        },

        ColumnMapping::Mdy( _column ) => {
            // If there's a value, try to parse as m/d/y, then m-d-y, then m\d\y, then m d y.
            match date_captures(&value) {
                Some(captures) => {
                    let dt = Utc.ymd(captures.2 as i32, captures.0, captures.1).and_hms_milli(0, 0, 0, 0);
                    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                },
                None => value,
            }
        },

        ColumnMapping::Ymd( _column ) => {
            // If there's a value, try to parse as y/m/d, then y-m-d, then y/m/d, then y m d.
            match date_captures(&value) {
                Some(captures) => {
                    let dt = Utc.ymd(captures.0 as i32, captures.1, captures.2).and_hms_milli(0, 0, 0, 0);
                    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                },
                None => value,
            }
        },

        ColumnMapping::Trim( _column ) => value.trim().to_string(),

        ColumnMapping::AsBoolean( column )  => check_type(&value, column, DataType::Boolean)?.to_string(),
        ColumnMapping::AsDatetime( column ) => check_type(&value, column, DataType::Datetime)?.to_string(),
        ColumnMapping::AsDecimal( column )  => check_type(&value, column, DataType::Decimal)?.to_string(),
        ColumnMapping::AsInteger( column )  => check_type(&value, column, DataType::Integer)?.to_string(),
    };

    Ok(mapped.into())
}

///
/// If there's a value check it can be co-erced into the type.
///
fn check_type<'a>(value: &'a str, column: &str, data_type: DataType) -> Result<&'a str, JetwashError> {
    if !value.is_empty() && !analyser::is_type(value, DataType::Boolean) {
        return Err(JetwashError::SchemaViolation { column: column.to_string(), value: value.to_string(), data_type: data_type.as_str().to_string()})
    }
    Ok(value)
}

///
/// Iterate the date pattern combinations and if we get a match, return the three component/captures
///
fn date_captures(value: &str) -> Option<(u32, u32, u32)> {
    for pattern in &*DATES {
        match pattern.captures(value) {
            Some(captures) if captures.len() == 4 => {
                if let Ok(n1) = captures.get(1).expect("capture 1 missing").as_str().parse::<u32>() {
                    if let Ok(n2) = captures.get(2).expect("capture 2 missing").as_str().parse::<u32>() {
                        if let Ok(n3) = captures.get(3).expect("capture 3 missing").as_str().parse::<u32>() {
                            return Some((n1, n2, n3))
                        }
                    }
                }
            },
            Some(_capture) => {},
            None => {},
        }
    }
    None
}

///
/// Populate a Lua table of strings.
///
pub fn lua_record<'a>(lua_ctx: &rlua::Context<'a>, record: &csv::ByteRecord, header_record: &csv::ByteRecord)
    -> Result<rlua::Table<'a>, JetwashError> {

    // TODO: Perf. Consider scanning for only referenced columns.

    let lua_record = lua_ctx.create_table()?;

    for (header, value) in header_record.iter().zip(record.iter()) {
        let l_header: String = String::from_utf8_lossy(header).into();
        let l_value: String = String::from_utf8_lossy(value).into();
        lua_record.set(l_header, l_value)?;
    }

    Ok(lua_record)
}

///
/// Run the lua script provided. Reporting the failing script if it errors.
///
fn eval<'lua, R: FromLuaMulti<'lua>>(lua_ctx: &rlua::Context<'lua>, lua: &str)
    -> Result<R, rlua::Error> {

    match lua_ctx.load(lua).eval::<R>() {
        Ok(result) => Ok(result),
        Err(err) => {
            log::error!("Error in Lua script:\n{}\n\n{}", lua, err.to_string());
            Err(err)
        },
    }
}