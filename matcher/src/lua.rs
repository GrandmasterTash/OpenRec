use regex::Regex;
use rust_decimal::Decimal;
use rlua::{Context, Table};
use lazy_static::lazy_static;
use crate::model::{data_type::DataType, record::Record, schema::{Column, GridSchema}};

lazy_static! {
    static ref HEADER_REGEX: Regex = Regex::new(r#"record\["(.*?)"\]"#).unwrap();
}

///
/// Return all the columns referenced in the script specified.
///
pub fn script_columns<'a>(script: &str, schema: &'a GridSchema) -> Vec<&'a Column> {
    let mut columns = Vec::new();

    for cap in HEADER_REGEX.captures_iter(script) {
        // if let Some(data_type) = schema.data_type(&cap[1]) {
        //     columns.pus
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
pub fn lua_record<'a>(record: &Record, script_cols: &[&Column], schema: &GridSchema, lua_ctx: &Context<'a>) -> Result<Table<'a>, rlua::Error> {
    let lua_record = lua_ctx.create_table()?;

    for col in script_cols {
        match col.data_type() {
            DataType::UNKNOWN  => {},
            DataType::BOOLEAN  => lua_record.set(col.header(), record.get_bool(col.header(), &schema))?,
            DataType::BYTE     => lua_record.set(col.header(), record.get_byte(col.header(), &schema))?,
            DataType::CHAR     => lua_record.set(col.header(), record.get_char(col.header(), &schema).map(|c|c.to_string()))?,
            DataType::DATE     => lua_record.set(col.header(), record.get_date(col.header(), &schema))?,
            DataType::DATETIME => lua_record.set(col.header(), record.get_datetime(col.header(), &schema))?,
            DataType::DECIMAL  => lua_record.set(col.header(), record.get_decimal(col.header(), &schema).map(|d|LuaDecimal(d)))?,
            DataType::INTEGER  => lua_record.set(col.header(), record.get_int(col.header(), &schema))?,
            DataType::LONG     => lua_record.set(col.header(), record.get_long(col.header(), &schema))?,
            DataType::SHORT    => lua_record.set(col.header(), record.get_short(col.header(), &schema))?,
            DataType::STRING   => lua_record.set(col.header(), record.get_string(col.header(), &schema))?,
            DataType::UUID     => lua_record.set(col.header(), record.get_uuid(col.header(), &schema).map(|i|i.to_string()))?,
        }
    }

    Ok(lua_record)
}

///
/// Create some contextural information regarding the file that loaded a record.
///
pub fn lua_meta<'a>(record: &Record, schema: &GridSchema, lua_ctx: &Context<'a>)
    -> Result<Table<'a>, rlua::Error> {

    let lua_meta = lua_ctx.create_table()?;

    match schema.file_schemas().get(record.schema_idx()) {
        Some(file) => lua_meta.set("prefix", file.prefix())?,
        None       => log::warn!("record file missing from grid schema"),
    }

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