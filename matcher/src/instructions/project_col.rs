use crate::{error::MatcherError, grid::Grid};


pub fn project_col(name: &str, script: &str, grid: &mut Grid) -> Result<(), MatcherError> {

    // TODO: Do this once for the match.
    let lua = rlua::Lua::new();

    // TODO: Ref: https://github.com/amethyst/rlua/blob/master/examples/guided_tour.rs


    let result = lua.context(|lua_ctx| {
        lua_ctx.load("return 1 + 2").eval::<i32>()
        // let globals = lua_ctx.globals();
        // assert_eq!(lua_ctx.load("1 + 1").eval::<i32>()?, 2);
        // assert_eq!(lua_ctx.load("false == false").eval::<bool>()?, true);
        // assert_eq!(lua_ctx.load("return 1 + 2").eval::<i32>()?, 3);
        // Ok(())
    }).expect("LUA FAILED");

    // println!("LUA: {}", result);

    // TODO: Add or replace the value/column in the ByteRecord
    Ok(())
}