use rlua::{Context, Table};
use crate::{data_type::{DataType, LuaDecimal}, error::MatcherError, grid::Grid, record::Record, schema::{Column, GridSchema}};

///
/// Use a script to calculate a value for a new column in each record.
///
/// The script can reference any other value in the same record.
///
/// Usful Ref: https://github.com/amethyst/rlua/blob/master/examples/guided_tour.rs
///
pub fn project_column(name: &str, data_type: DataType, eval: &str, when: &str, grid: &mut Grid, lua: &rlua::Lua) -> Result<(), MatcherError> {

    log::info!("Projecting column {name} as {data_type} with lua {eval} when {when}",
        name = name,
        data_type = data_type.to_str(),
        eval = eval,
        when = when);

    // Add the projected column to the schema.
    grid.schema_mut().add_projected_column(Column::new(name.into(), data_type))?;

    // Snapshot the schema so we can iterate mutable records in a mutable grid.
    let schema = grid.schema().clone();
    let mut row = 0;

    lua.context(|lua_ctx| {
        let globals = lua_ctx.globals();

        // Calculate the column value for every record.
        for record in grid.records_mut() {
            let lua_record = lua_record(record, &schema, &lua_ctx)?;
            globals.set("record", lua_record)?;

            let lua_meta = lua_meta(record, &schema, &lua_ctx)?;
            globals.set("meta", lua_meta)?;

            // Evalute the WHEN script to see if we should even evaluate the EVAL script. This allows us to skip
            // attempting to calulate values that are not relevant to the record without having to write verbose scripts.
            if lua_ctx.load(&when).eval::<bool>()? {
                // Now calculate the column value and append it to the underlying ByteRecord.
                match data_type {
                    DataType::UNKNOWN  => {},
                    DataType::BOOLEAN  => record.append_bool(lua_ctx.load(&eval).eval::<bool>()?),
                    DataType::BYTE     => record.append_byte(lua_ctx.load(&eval).eval::<u8>()?),
                    DataType::CHAR     => record.append_char(lua_ctx.load(&eval).eval::<String>().map(|s|s.chars().next().unwrap_or_default())?),
                    DataType::DATE     => record.append_date(lua_ctx.load(&eval).eval::<u64>()?),
                    DataType::DATETIME => record.append_datetime(lua_ctx.load(&eval).eval::<u64>()?),
                    DataType::DECIMAL  => record.append_decimal(lua_ctx.load(&eval).eval::<LuaDecimal>()?.0),
                    DataType::INTEGER  => record.append_int(lua_ctx.load(&eval).eval::<i32>()?),
                    DataType::LONG     => record.append_long(lua_ctx.load(&eval).eval::<i64>()?),
                    DataType::SHORT    => record.append_short(lua_ctx.load(&eval).eval::<i16>()?),
                    DataType::STRING   => record.append_string(&lua_ctx.load(&eval).eval::<String>()?),
                    DataType::UUID     => record.append_uuid(lua_ctx.load(&eval).eval::<String>().map(|s|s.parse().expect("Lua returned an invalid uuid"))?),
                };
            } else {
                // Put a blank value in the projected column if we're not evaluating it.
                record.append_string("");
            }

            row += 1;
        }
        Ok(())
    })
    .map_err(|source| MatcherError::ScriptError {
        eval: eval.into(),
        when: when.into(),
        data_type: data_type.to_str().into(),
        record: grid.record_as_string(row).unwrap_or("(no record)".into()),
        source
    })?;

    Ok(())
}

///
/// Convert all the fields of the record into a Lua table.
///
fn lua_record<'a>(record: &Record, schema: &GridSchema, lua_ctx: &Context<'a>) -> Result<Table<'a>, rlua::Error> {
    let lua_record = lua_ctx.create_table()?;

    // TODO: Performance - Could scan script once to build a list of required fields, rather than doing all.
    for header in schema.headers().iter().map(String::as_str) {
        if let Some(data_type) = schema.data_type(header) {
            match data_type { // TODO: Vec of columns would improve perf here - avoiding the map look-up.
                DataType::UNKNOWN  => {},
                DataType::BOOLEAN  => lua_record.set(header, record.get_bool(header, &schema))?,
                DataType::BYTE     => lua_record.set(header, record.get_byte(header, &schema))?,
                DataType::CHAR     => lua_record.set(header, record.get_char(header, &schema).map(|c|c.to_string()))?,
                DataType::DATE     => lua_record.set(header, record.get_date(header, &schema))?,
                DataType::DATETIME => lua_record.set(header, record.get_datetime(header, &schema))?,
                DataType::DECIMAL  => lua_record.set(header, record.get_decimal(header, &schema).map(|d|LuaDecimal(d)))?,
                DataType::INTEGER  => lua_record.set(header, record.get_int(header, &schema))?,
                DataType::LONG     => lua_record.set(header, record.get_long(header, &schema))?,
                DataType::SHORT    => lua_record.set(header, record.get_short(header, &schema))?,
                DataType::STRING   => lua_record.set(header, record.get_string(header, &schema))?,
                DataType::UUID     => lua_record.set(header, record.get_uuid(header, &schema).map(|i|i.to_string()))?,
            }
        }
    }

    Ok(lua_record)
}

///
/// Create some contextural information regarding the file that loaded a record.
///
fn lua_meta<'a>(record: &Record, schema: &GridSchema, lua_ctx: &Context<'a>) -> Result<Table<'a>, rlua::Error> {
    let lua_meta = lua_ctx.create_table()?;

    match schema.file_schemas().get(record.schema_idx()) {
        Some(file) => lua_meta.set("prefix", file.prefix())?,
        None       => log::warn!("record file missing from grid schema"),
    }

    Ok(lua_meta)
}