use rlua::Context;
use rust_decimal::Decimal;
use crate::{model::{charter::{Constraint, ToleranceType}, data_type::DataType, record::Record, schema::{Column, GridSchema}}, error::MatcherError, lua, blue};

impl Constraint {
    pub fn passes(&self, records: &[&Box<Record>], schema: &GridSchema, lua_ctx: &Context)
        -> Result<bool, rlua::Error> {

        match self {
            Constraint::NetsToZero { column, lhs, rhs, debug } => {
                let sum_checker = |lhs_sum, rhs_sum| (lhs_sum - rhs_sum) == Decimal::ZERO;
                net(column, lhs, rhs, sum_checker, debug, records, schema, lua_ctx)
            },

            Constraint::NetsWithTolerance {column, lhs, rhs, tol_type, tolerance, debug } => {
                let sum_checker: Box<dyn Fn(Decimal, Decimal) -> bool> = match tol_type {
                    ToleranceType::Amount  => Box::new(|lhs_sum: Decimal, rhs_sum: Decimal| (lhs_sum - rhs_sum).abs() < *tolerance),
                    ToleranceType::Percent => Box::new(|lhs_sum: Decimal, rhs_sum: Decimal| {
                        let percent_tol = lhs_sum / (Decimal::ONE_HUNDRED / *tolerance);
                        // log::trace!("LHS_SUM {}, RHS_SUM {}, TOLERANCE {}, PERC_TOL {}, (lhs_sum - rhs_sum).abs() {}",
                        //     lhs_sum, rhs_sum, tolerance, percent_tol, (lhs_sum - rhs_sum).abs());
                        (lhs_sum - rhs_sum).abs() < percent_tol
                    }),
                };

                net(column, lhs, rhs, sum_checker, debug, records, schema, lua_ctx)
            },

            Constraint::Custom { script, fields } => custom_constraint(script, fields, records, schema, lua_ctx),
        }
    }
}

///
/// NETting takes two sets of records and SUMs a column from both. Then subtracts the SUM of the first list from the second
/// and, if the result is zero (or within a tolerance) it returns true. There must be at least one record in each subset as well.
///
/// This allows you to match a list of, for exaple, payments to a list of invoices, as long as all the payments cover the cost
/// of the invoices.
///
fn net<F>(
    column: &String,
    lhs: &str,
    rhs: &str,
    sum_checker: F,
    debug: &Option<bool>,
    records: &[&Box<Record>],
    schema: &GridSchema,
    lua_ctx: &Context) -> Result<bool, rlua::Error>

    where F: Fn(Decimal, Decimal) -> bool, {

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
    let net = sum_checker(lhs_sum, rhs_sum) && (lhs_recs.len() > 0 && rhs_recs.len() > 0);

    // If the records don't net then, if we're debugging the constraint, output the group values.
    if !net && debug.unwrap_or(false) {
        let mut dbg = format!("{}\n", column);
        lhs_recs.iter().for_each(|r| dbg += &format!("{:<30}: {}\n", r.get_decimal(column, schema).unwrap_or(Decimal::ZERO), lhs) );
        dbg += &format!("{}: SUM\n", blue(&format!("{:<30}", lhs_sum)));

        rhs_recs.iter().for_each(|r| dbg += &format!("{:<30}: {}\n", r.get_decimal(column, schema).unwrap_or(Decimal::ZERO), rhs) );
        dbg += &format!("{}: SUM", blue(&format!("{:<30}", rhs_sum)));
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

///
/// Allow entirely custom Lua script to be evaluated for a group constraint.
///
fn custom_constraint(script: &str, fields: &Option<Vec<String>>, records: &[&Box<Record>], schema: &GridSchema, lua_ctx: &Context) -> Result<bool, rlua::Error> {
    let script_cols = match fields {
        Some(fields) => {
            let fields = fields.iter().map(|f|f.as_str()).collect::<Vec<&str>>();

            schema.columns()
                .into_iter()
                .filter(|col| fields.contains(&col.header()))
                .collect::<Vec<&Column>>()
            },
        None => schema.columns(), // No restriction, provide all columns to the Lua script.
    };

    let globals = lua_ctx.globals();
    let lua_metas = lua_ctx.create_table()?;
    let lua_records = lua_ctx.create_table()?;

    for (idx, record) in records.iter().enumerate() {
        let lua_record = lua::lua_record(record, &script_cols, &schema, lua_ctx)?;
        lua_records.set(idx + 1, lua_record)?;

        let lua_meta = lua::lua_meta(record, &schema, lua_ctx)?;
        lua_metas.set(idx + 1, lua_meta)?;
    }

    globals.set("metas", lua_metas)?;
    globals.set("records", lua_records)?;
    lua_ctx.load(&script).eval::<bool>()
}