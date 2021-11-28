use rlua::Context;
use rust_decimal::Decimal;
use crate::{charter::Constraint, data_type::DataType, error::MatcherError, lua, record::Record, schema::GridSchema};

impl Constraint {
    pub fn passes(&self, records: &[&Box<Record>], schema: &GridSchema, lua_ctx: &Context)
        -> Result<bool, rlua::Error> {

        match self {
            Constraint::NetsToZero { column, lhs, rhs, debug } => net_to_zero(column, lhs, rhs, debug, records, schema, lua_ctx),
        }
    }
}

///
/// NET to zero takes two sets of records and SUMs a column from both. Then subtracts the SUM of the first list from the second
/// and, if the result is zero returns true. There must be at least one record in each subset as well.
///
/// This allows you to match a list of, for exaple, payments to a list of invoices, as long as all the payments cover the cost
/// of the invoices.
///
fn net_to_zero(
    column: &String,
    lhs: &str,
    rhs: &str,
    debug: &Option<bool>,
    records: &[&Box<Record>],
    schema: &GridSchema,
    lua_ctx: &Context) -> Result<bool, rlua::Error> {

    // Validate NET column exists and is a DECIMAL (we can relax the type resiction if needed).
    if !schema.headers().contains(column) {
        return Err(rlua::Error::external(MatcherError::ConstraintColumnMissing{ column: column.into() }))
    }

    if *schema.data_type(column).unwrap_or(&DataType::UNKNOWN) != DataType::DECIMAL {
        return Err(rlua::Error::external(MatcherError::ConstraintColumnNotDecimal{ column: column.into() }))
    }

    // Collect records in the group which match lhs and rhs filters.
    let lhs_recs = lua_filter(records, lhs, lua_ctx, schema)?;
    let rhs_recs = lua_filter(records, rhs, lua_ctx, schema)?;

    // Sum the NETting column for records on both sides.
    let lhs_sum: Decimal = lhs_recs.iter().map(|r| r.get_decimal(column, schema).unwrap_or(Decimal::ZERO)).sum();
    let rhs_sum: Decimal = rhs_recs.iter().map(|r| r.get_decimal(column, schema).unwrap_or(Decimal::ZERO)).sum();

    // The constraint passes if the sides net to zero AND there is at least one record from each side.
    let net = (lhs_sum - rhs_sum) == Decimal::ZERO && (lhs_recs.len() > 0 && rhs_recs.len() > 0);

    // If the records don't net then, if we're debugging the constraint, output the group values.
    if !net && debug.unwrap_or(false) {
        let mut dbg = format!("{}\n", column);
        lhs_recs.iter().for_each(|r| dbg += &format!("{:<30}: {}\n", r.get_decimal(column, schema).unwrap_or(Decimal::ZERO), lhs) );
        dbg += &format!("{}: SUM\n", ansi_term::Colour::RGB(70, 130, 180).paint(format!("{:<30}", lhs_sum)));

        rhs_recs.iter().for_each(|r| dbg += &format!("{:<30}: {}\n", r.get_decimal(column, schema).unwrap_or(Decimal::ZERO), rhs) );
        dbg += &format!("{}: SUM", ansi_term::Colour::RGB(70, 130, 180).paint(format!("{:<30}", rhs_sum)));
        log::info!("NetToZero constraint failed for group\n{}", dbg);
    }

    // Do the records NET to zero?
    Ok(net)
}

///
/// Filter the records using the Lua expression and return the filtered list.
///
fn lua_filter<'a, 'b>(records: &[&'a Box<Record>], lua_script: &str, lua_ctx: &'b Context, schema: &GridSchema)
    -> Result<Vec<&'a Box<Record>>, rlua::Error> {

    let mut results = vec!();
    let script_cols = lua::script_columns(lua_script, &schema);
    let globals = lua_ctx.globals();

    for record in records {
        let lua_record = lua::lua_record(record, &script_cols, &schema, lua_ctx)?;
        globals.set("record", lua_record)?;

        let lua_meta = lua::lua_meta(record, &schema, lua_ctx)?;
        globals.set("meta", lua_meta)?;

        if lua_ctx.load(&lua_script).eval::<bool>()? {
            results.push(*record);
        }
    }

    Ok(results)
}
