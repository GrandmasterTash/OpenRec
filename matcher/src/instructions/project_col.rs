use std::time::Instant;
use itertools::Itertools;

use crate::{data_type::DataType, error::MatcherError, formatted_duration_rate, grid::Grid, lua, schema::Column};

///
/// Use a script (eval) to calculate a value for a new column in each record.
///
/// The script can reference any other value in the same record.
///
pub fn project_column(
    name: &str,
    data_type: DataType,
    eval: &str,
    when: Option<&str>,
    grid: &mut Grid,
    lua: &rlua::Lua) -> Result<(), MatcherError> {

    let start = Instant::now();

    log::info!("Projecting column {} as {:?}", name, data_type);

    // Add the projected column to the schema.
    grid.schema_mut().add_projected_column(Column::new(name.into(), data_type))?;

    // Snapshot the schema so we can iterate mutable records in a mutable grid.
    let schema = grid.schema().clone();

    // Collect a unique list of all the columns we need to make available to the Lua script.
    let script_cols = match when {
        Some(when) => vec!(lua::script_columns(eval, &schema), lua::script_columns(when, &schema)),
        None => vec!(lua::script_columns(eval, &schema)),
    }.concat()
        .into_iter()
        .unique()
        .collect::<Vec<Column>>();
    let mut row = 0;

    // TODO: Consider rayon when we're streaming from files.

    lua.context(|lua_ctx| {
        let globals = lua_ctx.globals();

        // Calculate the column value for every record.
        for record in grid.records_mut() {
            let lua_record = lua::lua_record(record, &script_cols, &schema, &lua_ctx)?;
            globals.set("record", lua_record)?;

            let lua_meta = lua::lua_meta(record, &schema, &lua_ctx)?;
            globals.set("meta", lua_meta)?;

            // Evalute the WHEN script to see if we should even evaluate the EVAL script. This allows us to skip
            // attempting to calulate values that are not relevant to the record without having to write verbose scripts.
            if when.is_none() || lua_ctx.load(&when.unwrap()).eval::<bool>()? {
                // Now calculate the column value and append it to the underlying ByteRecord.
                match data_type {
                    DataType::UNKNOWN  => {},
                    DataType::BOOLEAN  => record.append_bool(lua_ctx.load(&eval).eval::<bool>()?),
                    DataType::BYTE     => record.append_byte(lua_ctx.load(&eval).eval::<u8>()?),
                    DataType::CHAR     => record.append_char(lua_ctx.load(&eval).eval::<String>().map(|s|s.chars().next().unwrap_or_default())?),
                    DataType::DATE     => record.append_date(lua_ctx.load(&eval).eval::<u64>()?),
                    DataType::DATETIME => record.append_datetime(lua_ctx.load(&eval).eval::<u64>()?),
                    DataType::DECIMAL  => record.append_decimal(lua_ctx.load(&eval).eval::<lua::LuaDecimal>()?.0),
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
    .map_err(|source| MatcherError::ProjectColScriptError {
        eval: eval.into(),
        when: when.unwrap_or("(no when script)").into(),
        data_type: data_type.to_str().into(),
        record: grid.record_as_string(row).unwrap_or("(no record)".into()),
        source
    })?;

    let (duration, rate) = formatted_duration_rate(row, start.elapsed());
    log::info!("Projection took {} for {} rows ({}/row)",
        duration,
        row,
        ansi_term::Colour::RGB(70, 130, 180).paint(rate));

    Ok(())
}