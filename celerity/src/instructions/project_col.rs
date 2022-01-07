use itertools::Itertools;
use core::data_type::DataType;
use crate::{error::MatcherError, model::{schema::{Column, GridSchema}, record::Record}, lua};

pub fn project_column(
    data_type: DataType,
    lua: &str,
    when: &Option<String>,
    record: &mut Record,
    avail_cols: &[Column],
    lua_ctx: &rlua::Context) -> Result<(), MatcherError> {

    let globals = lua_ctx.globals();

    let lua_record = lua::lua_record(record, avail_cols, lua_ctx)?;
    globals.set("record", lua_record)?;

    // Evalute the WHEN script to see if we should even evaluate the EVAL script. This allows us to skip
    // attempting to calulate values that are not relevant to the record without having to write verbose scripts.
    if when.is_none() || lua::eval(lua_ctx, when.as_ref().expect("weird"))? {
        // Now calculate the column value and append it to the underlying ByteRecord.
        match data_type {
            DataType::Unknown  => {},
            DataType::Boolean  => record.append_bool(lua::eval(lua_ctx, lua)?),
            DataType::Datetime => record.append_datetime(lua::eval(lua_ctx, lua)?),
            DataType::Decimal  => record.append_decimal(lua::eval::<lua::LuaDecimal>(lua_ctx, lua)?.0),
            DataType::Integer  => record.append_int(lua::eval(lua_ctx, lua)?),
            DataType::String   => record.append_string(&lua::eval::<String>(lua_ctx, lua)?),
            DataType::Uuid     => record.append_uuid(lua::eval::<String>(lua_ctx, lua).map(|s|s.parse().expect("Lua returned an invalid uuid"))?),
        };
    } else {
        // Put a blank value in the projected column if we're not evaluating it.
        record.append_string("");
    }

    Ok(())
}

///
/// Return the columns involved in any Lua script for this projection.
///
pub fn referenced_cols(eval: &str, when: Option<&str>, schema: &GridSchema) -> Vec<Column> {
    match when {
        Some(when) => vec!(lua::referenced_columns(eval, schema), lua::referenced_columns(when, schema)),
        None => vec!(lua::referenced_columns(eval, schema)),
    }.concat()
        .into_iter()
        .unique()
        .collect::<Vec<Column>>()
}