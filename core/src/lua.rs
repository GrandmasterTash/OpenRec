use std::path::Path;
use chrono::{Utc, TimeZone};
use rlua::{FromLuaMulti, Number};
use rust_decimal::{Decimal, prelude::FromPrimitive};

///
/// Plug-in global Rust functions that can be called from Lua script.
///
pub fn init_context(lua_ctx: &rlua::Context, global_lua: &Option<String>, lookup_path: &Path) -> Result<(), rlua::Error> {
    let globals = lua_ctx.globals();

    // Create a decimal() function to convert a Lua number to a Rust Decimal data-type.
    let decimal = lua_ctx.create_function(|_, value: Number| {
        Ok(LuaDecimal(Decimal::from_f64(value).expect("Unable to convert number from Lua into a Decimal type")))
    })?;

    globals.set("decimal", decimal)?;

    // Create an abs() function to specifically for a Rust Decimal data-type.
    let abs = lua_ctx.create_function(|_, value: LuaDecimal| {
        Ok(LuaDecimal(value.0.abs()))
    })?;

    globals.set("abs", abs)?;

    // Create a midnight() function to remove the time portion of a datetime value.
    let midnight = lua_ctx.create_function(|_, value: String| {
        let ts = value.parse::<i64>().unwrap_or_else(|_| panic!("midnight called with a non-numeric: {}", value));
        let dt = Utc.timestamp(ts / 1000, 0).date();
        Ok(dt.and_hms_milli(0,0,0,0).timestamp_millis())
    })?;

    globals.set("midnight", midnight)?;

    // Create a lookup(field, filename, filter_field, filter_string) function to find a value from another csv.
    let lookup_path = lookup_path.to_string_lossy().to_string();
    let lookup = lua_ctx.create_function(move |_, search: (
        /* find_field: */ String,
        /* file_name:  */ String,
        /* where_field: */ String,
        /* where_value: */ String)| {
        // Ok(lookup(&search.0, &search.1, &search.2, &search.3, lookup_path.clone())?)
        Ok(lookup(&search.0, &search.1, &search.2, &search.3, &lookup_path)
            .map_err(|err| rlua::Error::external(format!("{}", err)))?)
    })?;

    globals.set("lookup", lookup)?;

    // Run any global scripts.
    if let Some(global_lua) = global_lua {
        eval(lua_ctx, global_lua)?;
    }

    Ok(())
}

///
/// Run the lua script provided. Reporting the failing script if it errors.
///
pub fn eval<'lua, R: FromLuaMulti<'lua>>(lua_ctx: &rlua::Context<'lua>, lua: &str)
    -> Result<R, rlua::Error> {

    log::trace!("Running: {:?}", lua);

    match lua_ctx.load(lua).eval::<R>() {
        Ok(result) => Ok(result),
        Err(err) => {
            log::error!("Error in Lua script:\n{}\n\n{}", lua, err.to_string());
            Err(err)
        },
    }
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

///
/// Find a value from another csv file - or empty string if no match.
///
fn lookup(what_field: &str, file_name: &str, where_field: &str, is: &str, lookup_path: &str)
    -> Result<String, csv::Error> {

    // TODO: Prohibit lookup path escapes....

    // TODO: Perf. Once working consider creating a reader cache, with last 5 entries cached in memory - consider function memoization.
    let path = Path::new(lookup_path).join(file_name);
    if !path.exists() {
        panic!("Lookup file {} does not exist", path.to_string_lossy());
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .unwrap_or_else(|err| panic!("Failed to open {} : {}", lookup_path, err));

    // Get the column position of the where_field header.
    let where_col = match reader.headers()?.iter().position(|h| h == where_field) {
        Some(col) => col,
        None => panic!("Lookup 'where' field {} was not in the file {}", where_field, file_name),
    };

    // Get the column position of the what_field header.
    let what_col = match reader.headers()?.iter().position(|h| h == what_field) {
        Some(col) => col,
        None => panic!("Lookup 'what' field {} was not in the file {}", what_field, file_name),
    };

    for record in reader.records() {
        let record = record?;

        // Find a record where the where_field value == the is clause.
        match record.get(where_col) {
            Some(value) => {
                if value == is {
                    // Return the what_field.
                    match record.get(what_col) {
                        Some(what) => return Ok(what.to_string()),
                        None => return Ok(String::default()),
                    }
                }
            },
            None => continue,
        }
    }

    return Ok(String::default())
}