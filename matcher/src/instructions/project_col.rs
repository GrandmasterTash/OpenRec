use itertools::Itertools;
use crate::{error::MatcherError, model::{data_type::DataType, schema::{Column, GridSchema}, data_accessor::DataAccessor, record::Record}, lua};

pub fn project_column_new(
    data_type: DataType,
    eval: &str,
    when: &Option<String>,
    record: &Record,
    accessor: &mut DataAccessor,
    script_cols: &Vec<Column>,
    lua_ctx: &rlua::Context) -> Result<(), MatcherError> {

    let globals = lua_ctx.globals();

    let lua_record = lua::lua_record(record, script_cols, accessor, lua_ctx)
        .map_err(|source| rlua::Error::external(source))?;
    globals.set("record", lua_record)?;

    let lua_meta = lua::lua_meta(record, accessor.schema(), lua_ctx)
        .map_err(|source| rlua::Error::external(source))?;
    globals.set("meta", lua_meta)?;

    // Evalute the WHEN script to see if we should even evaluate the EVAL script. This allows us to skip
    // attempting to calulate values that are not relevant to the record without having to write verbose scripts.
    if when.is_none() || lua_ctx.load(when.as_ref().unwrap()).eval::<bool>()? {
        // Now calculate the column value and append it to the underlying ByteRecord.
        match data_type {
            DataType::UNKNOWN  => {},
            DataType::BOOLEAN  => record.append_bool(lua_ctx.load(&eval).eval::<bool>()?, accessor),
            DataType::BYTE     => record.append_byte(lua_ctx.load(&eval).eval::<u8>()?, accessor),
            DataType::CHAR     => record.append_char(lua_ctx.load(&eval).eval::<String>().map(|s|s.chars().next().unwrap_or_default())?, accessor),
            DataType::DATE     => record.append_date(lua_ctx.load(&eval).eval::<u64>()?, accessor),
            DataType::DATETIME => record.append_datetime(lua_ctx.load(&eval).eval::<u64>()?, accessor),
            DataType::DECIMAL  => record.append_decimal(lua_ctx.load(&eval).eval::<lua::LuaDecimal>()?.0, accessor),
            DataType::INTEGER  => record.append_int(lua_ctx.load(&eval).eval::<i32>()?, accessor),
            DataType::LONG     => record.append_long(lua_ctx.load(&eval).eval::<i64>()?, accessor),
            DataType::SHORT    => record.append_short(lua_ctx.load(&eval).eval::<i16>()?, accessor),
            DataType::STRING   => record.append_string(&lua_ctx.load(&eval).eval::<String>()?, accessor),
            DataType::UUID     => record.append_uuid(lua_ctx.load(&eval).eval::<String>().map(|s|s.parse().expect("Lua returned an invalid uuid"))?, accessor),
        };
    } else {
        // Put a blank value in the projected column if we're not evaluating it.
        record.append_string("", accessor); // TODO: Create a 'pad' fn for this, to avoid dt confusion.
    }

    Ok(())
}

///
/// Return the columns involved in any Lua script for this projection.
///
pub fn script_cols(eval: &str, when: Option<&str>, schema: &GridSchema) -> Vec<Column> {
    match when {
        Some(when) => vec!(lua::script_columns(eval, &schema), lua::script_columns(when, &schema)),
        None => vec!(lua::script_columns(eval, &schema)),
    }.concat()
        .into_iter()
        .unique()
        .collect::<Vec<Column>>()
}