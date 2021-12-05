use regex::Regex;
use std::fs::File;
use rust_decimal::Decimal;
use rlua::{Context, Table};
use lazy_static::lazy_static;
use crate::{model::{data_type::DataType, record::Record, schema::{Column, GridSchema}}, error::MatcherError};

lazy_static! {
    static ref HEADER_REGEX: Regex = Regex::new(r#"record\["(.*?)"\]"#).unwrap();
}

///
/// Return all the columns referenced in the script specified.
///
pub fn script_columns<'a>(script: &str, schema: &'a GridSchema) -> Vec<&'a Column> {
    let mut columns = Vec::new();

    for cap in HEADER_REGEX.captures_iter(script) {
        if let Some(col) = schema.column(&cap[1]) {
            columns.push(col);
        } else {
            log::warn!("Record field [{}] was not found, potential typo in Lua script?\n{}", &cap[1], script);
        }
    }

    columns
}

///
/// Convert all the specified column/fields of the record into a Lua table.
///
pub fn lua_record<'a>(
    record: &Record,
    script_cols: &[&Column],
    schema: &GridSchema,
    rdr: &mut csv::Reader<File>,
    lua_ctx: &Context<'a>) -> Result<Table<'a>, MatcherError> {

    let lua_record = lua_ctx.create_table()?;

    for col in script_cols {
        match col.data_type() {
            DataType::UNKNOWN  => {},
            DataType::BOOLEAN  => lua_record.set(col.header(), record.get_bool(col.header(), &schema, rdr)?)?,
            DataType::BYTE     => lua_record.set(col.header(), record.get_byte(col.header(), &schema, rdr)?)?,
            DataType::CHAR     => lua_record.set(col.header(), record.get_char(col.header(), &schema, rdr)?.map(|c|c.to_string()))?,
            DataType::DATE     => lua_record.set(col.header(), record.get_date(col.header(), &schema, rdr)?)?,
            DataType::DATETIME => lua_record.set(col.header(), record.get_datetime(col.header(), &schema, rdr)?)?,
            DataType::DECIMAL  => lua_record.set(col.header(), record.get_decimal(col.header(), &schema, rdr)?.map(|d|LuaDecimal(d)))?,
            DataType::INTEGER  => lua_record.set(col.header(), record.get_int(col.header(), &schema, rdr)?)?,
            DataType::LONG     => lua_record.set(col.header(), record.get_long(col.header(), &schema, rdr)?)?,
            DataType::SHORT    => lua_record.set(col.header(), record.get_short(col.header(), &schema, rdr)?)?,
            DataType::STRING   => lua_record.set(col.header(), record.get_string(col.header(), &schema, rdr)?)?,
            DataType::UUID     => lua_record.set(col.header(), record.get_uuid(col.header(), &schema, rdr)?.map(|i|i.to_string()))?,
        }
    }

    Ok(lua_record)
}

///
/// Create some contextural information regarding the file that loaded a record.
///
pub fn lua_meta<'a>(record: &Record, schema: &GridSchema, lua_ctx: &Context<'a>)
    -> Result<Table<'a>, MatcherError> {

    let lua_meta = lua_ctx.create_table()?;

    let file = match schema.files().get(record.file_idx()) {
        Some(file) => file,
        None => return Err(MatcherError::MissingFileInSchema{ index: record.file_idx() }),
    };

    let file_schema = match schema.file_schemas().get(file.schema_idx()) {
        Some(file_schema) => file_schema,
        None => return Err(MatcherError::MissingSchemaInGrid{ index: file.schema_idx(), filename: file.filename().into() }),
    };

    lua_meta.set("prefix", file_schema.prefix())?;
    Ok(lua_meta)
}

///
/// Provide a wrapper around the custom Decimal type so we can use a precise data-type in Lua scripts.
///
#[derive(Clone)]
pub struct LuaDecimal (pub Decimal);

impl rlua::UserData for LuaDecimal {
    fn add_methods<'lua, T: rlua::UserDataMethods<'lua, Self>>(methods: &mut T) {
        // Decimal with Decimal.
        methods.add_meta_method(rlua::MetaMethod::Add, |_, this, other: LuaDecimal| { Ok(LuaDecimal(this.0 + other.0)) });
        methods.add_meta_method(rlua::MetaMethod::Sub, |_, this, other: LuaDecimal| { Ok(LuaDecimal(this.0 - other.0)) });
        methods.add_meta_method(rlua::MetaMethod::Mul, |_, this, other: LuaDecimal| { Ok(LuaDecimal(this.0 * other.0)) });
        methods.add_meta_method(rlua::MetaMethod::Div, |_, this, other: LuaDecimal| { Ok(LuaDecimal(this.0 / other.0)) });
        methods.add_meta_method(rlua::MetaMethod::Lt,  |_, this, other: LuaDecimal| { Ok(this.0 < other.0) });
        methods.add_meta_method(rlua::MetaMethod::Le,  |_, this, other: LuaDecimal| { Ok(this.0 <= other.0) });
        methods.add_meta_method(rlua::MetaMethod::Eq,  |_, this, other: LuaDecimal| { Ok(this.0 == other.0) });
        methods.add_meta_method(rlua::MetaMethod::Concat,   |_, this, other: String| { Ok(format!("{}{}", this.0, other)) });
        methods.add_meta_method(rlua::MetaMethod::ToString, |_, this, _: ()| { Ok(this.0.to_string()) });
        // Arithmetic operations between a Decimal and other types can go here...
    }
}