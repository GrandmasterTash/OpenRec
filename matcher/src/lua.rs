use regex::Regex;
use lazy_static::lazy_static;
use rlua::{Context, Table, Number, FromLuaMulti};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use crate::{model::{data_type::DataType, record::Record, schema::{Column, GridSchema}}, error::MatcherError, data_accessor::DataAccessor, folders};

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
        Ok(LuaDecimal(Decimal::from_f64(value).expect("Unable to convert number from Lua into a Decimal type")))
    })?;

    globals.set("decimal", decimal)?;

    Ok(())
}

///
/// Run the lua script provided. Reporting the failing script if it errors.
///
pub fn eval<'lua, R: FromLuaMulti<'lua>>(lua_ctx: &rlua::Context<'lua>, lua: &str)
    -> Result<R, rlua::Error> {

    match lua_ctx.load(lua).eval::<R>() {
        Ok(result) => Ok(result),
        Err(err) => {
            log::error!("Error in Lua script:\n{}\n\n{}", lua, err.to_string());
            Err(err)
        },
    }
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

    append_meta(record, &lua_record, accessor.schema())?;

    Ok(lua_record)
}

///
/// Create some contextural information regarding the file that loaded a record.
///
// fn append_meta<'a>(record: &Record, schema: &GridSchema, lua_ctx: &Context<'a>)
//     -> Result<Table<'a>, MatcherError> {
fn append_meta<'a>(record: &Record, lua_record: &Table, schema: &GridSchema)
    -> Result<(), MatcherError> {

    // let lua_meta = lua_ctx.create_table()?;

    let file = match schema.files().get(record.file_idx()) {
        Some(file) => file,
        None => return Err(MatcherError::MissingFileInSchema{ index: record.file_idx() }),
    };

    lua_record.set("META.filename", file.filename())?;

    let file_schema = match schema.file_schemas().get(file.schema_idx()) {
        Some(file_schema) => file_schema,
        None => return Err(MatcherError::MissingSchemaInGrid{ index: file.schema_idx(), filename: file.filename().into() }),
    };

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
    accessor: &mut DataAccessor,
    schema: &GridSchema) -> Result<Vec<&'a Record>, MatcherError> {

    let mut results = vec!();
    let script_cols = script_columns(lua_script, schema);
    let globals = lua_ctx.globals();

    for record in records {
        let lua_record = lua_record(record, &script_cols, accessor, lua_ctx)?;
        globals.set("record", lua_record)?;

        // let lua_meta = lua_meta(record, accessor.schema(), lua_ctx)?;
        // globals.set("meta", lua_meta)?;

        if eval(lua_ctx, &lua_script)? {
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