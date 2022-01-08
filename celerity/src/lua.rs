use regex::Regex;
use rust_decimal::Decimal;
use rlua::{Context, Table};
use lazy_static::lazy_static;
use core::{data_type::DataType, lua::{LuaDecimal, eval}};
use crate::{model::{record::Record, schema::{Column, GridSchema}}, error::MatcherError, folders};

lazy_static! {
    static ref HEADER_REGEX: Regex = Regex::new(r#"record\["(.*?)"\]"#).expect("bad regex for HEADER_REGEX");
}

///
/// Plug-in aggregate Rust functions that can be called from Lua script inside matching group constraints.
///
pub fn create_aggregate_fns(lua_ctx: &rlua::Context) -> Result<(), rlua::Error> {
    let globals = lua_ctx.globals();

    // Provide a count(filter, records) function to the custom Lua script.
    let count = lua_ctx.create_function(|_, (filter, data): (rlua::Function, rlua::Table)| {
        let mut count = 0;
        for idx in 1..=data.len()? {
            let record: rlua::Table = data.get(idx)?;

            if filter.call::<_, bool>(record)? {
                count += 1;
            }
        }

        Ok(count)
    })?;

    // Provide a sum("field", filter, records) function to the custom Lua script.
    let sum = lua_ctx.create_function(|_, (field, filter, data): (String, rlua::Function, rlua::Table)| {
        let mut sum = Decimal::ZERO;

        for idx in 1..=data.len()? {
            let record: rlua::Table = data.get(idx)?;

            if filter.call::<_, bool>(record.clone())? {
                sum += record.get::<String, LuaDecimal>(field.clone())
                    .map_err(|source| MatcherError::CustomConstraintError { reason: format!("Field {} not found in record or not a DECIMAL. If you are trying to sum an INTEGER use sum_int() instead", field), source })?
                    .0;
            }
        }

        Ok(LuaDecimal(sum))
    })?;

    // Provide a sum_int("field", filter, records) function to the custom Lua script.
    let sum_int = lua_ctx.create_function(|_, (field, filter, data): (String, rlua::Function, rlua::Table)| {
        let mut sum = 0u64;

        for idx in 1..=data.len()? {
            let record: rlua::Table = data.get(idx)?;

            if filter.call::<_, bool>(record.clone())? {
                sum += record.get::<String, u64>(field.clone())
                    .map_err(|source| MatcherError::CustomConstraintError { reason: format!("Field {} not found in record or not an INTEGER.", field), source })?;
            }
        }

        Ok(sum)
    })?;

    // Provide a max("field", filter, records) function to the custom Lua script.
    let max = lua_ctx.create_function(|_, (field, filter, data): (String, rlua::Function, rlua::Table)| {
        let mut max = Decimal::MIN;

        for idx in 1..=data.len()? {
            let record: rlua::Table = data.get(idx)?;

            if filter.call::<_, bool>(record.clone())? {
                let value = record.get::<String, LuaDecimal>(field.clone())
                    .map_err(|source| MatcherError::CustomConstraintError { reason: format!("Field {} not found in record or not a DECIMAL. If you are trying to max an INTEGER use max_int() instead", field), source })?
                    .0;

                max = std::cmp::max(max, value);
            }
        }

        Ok(LuaDecimal(max))
    })?;

    // Provide a max_int("field", filter, records) function to the custom Lua script.
    let max_int = lua_ctx.create_function(|_, (field, filter, data): (String, rlua::Function, rlua::Table)| {
        let mut max = u64::MIN;

        for idx in 1..=data.len()? {
            let record: rlua::Table = data.get(idx)?;

            if filter.call::<_, bool>(record.clone())? {
                let value = record.get::<String, u64>(field.clone())
                    .map_err(|source| MatcherError::CustomConstraintError { reason: format!("Field {} not found in record or not a INTEGER.", field), source })?;

                max = std::cmp::max(max, value);
            }
        }

        Ok(max)
    })?;

    // Provide a min("field", filter, records) function to the custom Lua script.
    let min = lua_ctx.create_function(|_, (field, filter, data): (String, rlua::Function, rlua::Table)| {
        let mut min = Decimal::MAX;

        // println!("MIN-START: {}", min);

        for idx in 1..=data.len()? {
            let record: rlua::Table = data.get(idx)?;

            if filter.call::<_, bool>(record.clone())? {
                let value = record.get::<String, LuaDecimal>(field.clone())
                    .map_err(|source| MatcherError::CustomConstraintError { reason: format!("Field {} not found in record or not a DECIMAL. If you are trying to max an INTEGER use min_int() instead", field), source })?
                    .0;

                    // println!("MIN-CMP: {} / {}", min, value);
                    min = std::cmp::min(min, value);
                    // println!("MIN-NOW: {}", min);
            }
        }

        Ok(LuaDecimal(min))
    })?;

    // Provide a min_int("field", filter, records) function to the custom Lua script.
    let min_int = lua_ctx.create_function(|_, (field, filter, data): (String, rlua::Function, rlua::Table)| {
        let mut min = u64::MAX;

        for idx in 1..=data.len()? {
            let record: rlua::Table = data.get(idx)?;

            if filter.call::<_, bool>(record.clone())? {
                let value = record.get::<String, u64>(field.clone())
                    .map_err(|source| MatcherError::CustomConstraintError { reason: format!("Field {} not found in record or not a INTEGER.", field), source })?;

                min = std::cmp::min(min, value);
            }
        }

        Ok(min)
    })?;

    globals.set("count", count)?;
    globals.set("sum", sum)?;
    globals.set("sum_int", sum_int)?;
    globals.set("max", max)?;
    globals.set("max_int", max_int)?;
    globals.set("min", min)?;
    globals.set("min_int", min_int)?;
    Ok(())
}

///
/// Return all the columns referenced in the script specified.
///
pub fn referenced_columns(script: &str, schema: &GridSchema) -> Vec<Column> {
    let mut columns = Vec::new();

    for cap in HEADER_REGEX.captures_iter(script) {
        if let Some(col) = schema.column(&cap[1]) {
            columns.push(col.clone());
        }
    }

    columns
}

///
/// Convert all the specified column/fields of the record into a Lua table.
///
pub fn lua_record<'a>(
    record: &Record,
    avail_cols: &[Column],
    lua_ctx: &Context<'a>) -> Result<Table<'a>, MatcherError> {

    let lua_record = lua_ctx.create_table()?;

    for col in avail_cols {
        match col.data_type() {
            DataType::Unknown  => {},
            DataType::Boolean  => lua_record.set(col.header(), record.get_bool(col.header())?)?,
            DataType::Datetime => lua_record.set(col.header(), record.get_datetime(col.header())?)?,
            DataType::Decimal  => lua_record.set(col.header(), record.get_decimal(col.header())?.map(LuaDecimal))?,
            DataType::Integer  => lua_record.set(col.header(), record.get_int(col.header())?)?,
            DataType::String   => lua_record.set(col.header(), record.get_string(col.header())?)?,
            DataType::Uuid     => lua_record.set(col.header(), record.get_uuid(col.header())?.map(|i|i.to_string()))?,
        }
    }

    append_meta(record, &lua_record)?;

    Ok(lua_record)
}

///
/// Create some contextural information regarding the file that loaded a record.
///
fn append_meta(record: &Record, lua_record: &Table)
    -> Result<(), MatcherError> {

    let schema = record.schema();
    let file = &schema.files()[record.file_idx()];

    lua_record.set("META.filename", file.filename())?;

    let file_schema = &schema.file_schemas()[file.schema_idx()];

    if let Some(prefix) = file_schema.prefix() {
        lua_record.set("META.prefix", prefix)?;
    }

    if let Some(timestamp) = folders::unix_timestamp(file.timestamp()) {
        lua_record.set("META.timestamp", timestamp)?;
    }

    Ok(())
}

///
/// Filter the records using the Lua expression and return the filtered list (i.e. those matching the filter).
///
pub fn lua_filter<'a, 'b>(
    records: &[&'a Record],
    lua_script: &str,
    lua_ctx: &'b Context,
    schema: &GridSchema) -> Result<Vec<&'a Record>, MatcherError> {

    let mut results = vec!();
    let avail_cols = referenced_columns(lua_script, schema);
    let globals = lua_ctx.globals();

    for record in records {
        let lua_record = lua_record(record, &avail_cols, lua_ctx)?;
        globals.set("record", lua_record)?;

        if eval(lua_ctx, lua_script)? {
            results.push(*record);
        }
    }

    Ok(results)
}
