use regex::Regex;
use lazy_static::lazy_static;
use rlua::{Context, Table, Number};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use crate::{model::{data_type::DataType, record::Record, schema::{Column, GridSchema}}, error::MatcherError, data_accessor::DataAccessor};

lazy_static! {
    static ref HEADER_REGEX: Regex = Regex::new(r#"record\["(.*?)"\]"#).unwrap();
}

///
/// Plug-in global Rust functions that can be called from Lua script.
///
pub fn init_context(lua_ctx: &rlua::Context) -> Result<(), rlua::Error> {
    let globals = lua_ctx.globals();

    // Create a decimal() function to convert a Lua number to a Rust Decimal data-type.
    let decimal = lua_ctx.create_function(|_, value: Number| {
        Ok(LuaDecimal(Decimal::from_f64(value).unwrap())) // TODO: Don't unwrap.
    })?;
    globals.set("decimal", decimal)?;

    Ok(())
}

///
/// Return all the columns referenced in the script specified.
///
pub fn script_columns(script: &str, schema: &GridSchema) -> Vec<Column> {
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
    script_cols: &[Column],
    accessor: &mut DataAccessor,
    lua_ctx: &Context<'a>) -> Result<Table<'a>, MatcherError> {

    let lua_record = lua_ctx.create_table()?;

    for col in script_cols {
        match col.data_type() {
            DataType::Unknown  => {},
            DataType::Boolean  => lua_record.set(col.header(), record.get_bool(col.header(), accessor)?)?,
            DataType::Datetime => lua_record.set(col.header(), record.get_datetime(col.header(), accessor)?)?,
            DataType::Decimal  => lua_record.set(col.header(), record.get_decimal(col.header(), accessor)?.map(LuaDecimal))?,
            DataType::Integer  => lua_record.set(col.header(), record.get_int(col.header(), accessor)?)?,
            DataType::String   => lua_record.set(col.header(), record.get_string(col.header(), accessor)?)?,
            DataType::Uuid     => lua_record.set(col.header(), record.get_uuid(col.header(), accessor)?.map(|i|i.to_string()))?,
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

    // TODO: Put filename in meta.
    // TODO: Put a derived unix timestamp in meta from the filename prefix.
    // TODO: Create a test to assert all the meta fields are set.

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
/// Filter the records using the Lua expression and return the filtered list (i.e. those matching the filter).
///
pub fn lua_filter<'a, 'b>(
    records: &[&'a Record],
    lua_script: &str,
    lua_ctx: &'b Context,
    accessor: &mut DataAccessor,
    schema: &GridSchema) -> Result<Vec<&'a Record>, MatcherError> {

    let mut results = vec!();
    let script_cols = script_columns(lua_script, schema);
    let globals = lua_ctx.globals();

    for record in records {
        let lua_record = lua_record(record, &script_cols, accessor, lua_ctx)?;
        globals.set("record", lua_record)?;

        let lua_meta = lua_meta(record, accessor.schema(), lua_ctx)?;
        globals.set("meta", lua_meta)?;

        if lua_ctx.load(&lua_script).eval::<bool>()? {
            results.push(*record);
        }
    }

    Ok(results)
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