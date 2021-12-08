use itertools::Itertools;
use crate::{error::MatcherError, model::{data_type::DataType, schema::{Column, GridSchema}, data_accessor::DataAccessor, record::Record}, lua};

pub fn project_column_new(
    data_type: DataType,
    eval: &str,
    when: &Option<String>,
    record: &Record,
    accessor: &mut DataAccessor,
    script_cols: &[Column],
    lua_ctx: &rlua::Context) -> Result<(), MatcherError> {

    let globals = lua_ctx.globals();

    let lua_record = lua::lua_record(record, script_cols, accessor, lua_ctx)
        .map_err(rlua::Error::external)?;
    globals.set("record", lua_record)?;

    let lua_meta = lua::lua_meta(record, accessor.schema(), lua_ctx)
        .map_err(rlua::Error::external)?;
    globals.set("meta", lua_meta)?;

    // Evalute the WHEN script to see if we should even evaluate the EVAL script. This allows us to skip
    // attempting to calulate values that are not relevant to the record without having to write verbose scripts.
    if when.is_none() || lua_ctx.load(when.as_ref().unwrap()).eval::<bool>()? {
        // Now calculate the column value and append it to the underlying ByteRecord.
        match data_type {
            DataType::Unknown  => {},
            DataType::Boolean  => record.append_bool(lua_ctx.load(&eval).eval::<bool>()?, accessor),
            DataType::Datetime => record.append_datetime(lua_ctx.load(&eval).eval::<u64>()?, accessor),
            DataType::Decimal  => record.append_decimal(lua_ctx.load(&eval).eval::<lua::LuaDecimal>()?.0, accessor),
            DataType::Integer  => record.append_int(lua_ctx.load(&eval).eval::<i64>()?, accessor),
            DataType::String   => record.append_string(&lua_ctx.load(&eval).eval::<String>()?, accessor),
            DataType::Uuid     => record.append_uuid(lua_ctx.load(&eval).eval::<String>().map(|s|s.parse().expect("Lua returned an invalid uuid"))?, accessor),
        };
    } else {
        // Put a blank value in the projected column if we're not evaluating it.
        record.append_string("", accessor);
    }

    Ok(())
}

///
/// Return the columns involved in any Lua script for this projection.
///
pub fn script_cols(eval: &str, when: Option<&str>, schema: &GridSchema) -> Vec<Column> {
    match when {
        Some(when) => vec!(lua::script_columns(eval, schema), lua::script_columns(when, schema)),
        None => vec!(lua::script_columns(eval, schema)),
    }.concat()
        .into_iter()
        .unique()
        .collect::<Vec<Column>>()
}