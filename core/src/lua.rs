use chrono::{Utc, TimeZone};
use rlua::{FromLuaMulti, Number};
use rust_decimal::{Decimal, prelude::FromPrimitive};

///
/// Plug-in global Rust functions that can be called from Lua script.
///
pub fn init_context(lua_ctx: &rlua::Context, global_lua: &Option<String>) -> Result<(), rlua::Error> {
    let globals = lua_ctx.globals();

    // Create a decimal() function to convert a Lua number to a Rust Decimal data-type.
    let decimal = lua_ctx.create_function(|_, value: Number| {
        Ok(LuaDecimal(Decimal::from_f64(value).expect("Unable to convert number from Lua into a Decimal type")))
    })?;

    globals.set("decimal", decimal)?;

    // Create a date_only() function to remove the time portion of a datetime value.
    let date_only = lua_ctx.create_function(|_, value: String| {
        let ts = value.parse::<i64>().unwrap_or_else(|_| panic!("date_only called with a non-numeric: {}", value));
        let dt = Utc.timestamp(ts / 1000, 0).date();
        Ok(dt.and_hms_milli(0,0,0,0).timestamp_millis())
    })?;

    globals.set("date_only", date_only)?;

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