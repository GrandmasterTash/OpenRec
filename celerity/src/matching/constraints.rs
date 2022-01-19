use rlua::Context;
use rust_decimal::Decimal;
use core::{data_type::DataType, charter::{Constraint, ToleranceType}, lua::eval};
use crate::{model::{record::Record, schema::{Column, GridSchema}}, error::MatcherError, lua};

pub fn passes(
    constraint: &Constraint,
    records: &[&Record],
    schema: &GridSchema,
    lua_ctx: &Context) -> Result<bool, MatcherError> {

    match constraint {
        Constraint::NetsToZero { column, lhs, rhs } => {
            match schema.data_type(column).unwrap_or(&DataType::Unknown) {
                DataType::Decimal => net_to_zero(column, lhs, rhs, records, schema, lua_ctx),
                DataType::Integer => net_to_zero(column, lhs, rhs, records, schema, lua_ctx),
                col_type => return Err(MatcherError::CannotUseTypeForContstraint{ column: column.into(), col_type: format!("{:?}", col_type)})
            }
        },

        Constraint::NetsWithTolerance {column, lhs, rhs, tol_type, tolerance } => {
            match schema.data_type(column).unwrap_or(&DataType::Unknown) {
                DataType::Decimal => nets_with_tolerance(column, lhs, rhs, tol_type, *tolerance, records, schema, lua_ctx),
                DataType::Integer => nets_with_tolerance(column, lhs, rhs, tol_type, *tolerance, records, schema, lua_ctx),
                col_type => return Err(MatcherError::CannotUseTypeForContstraint{ column: column.into(), col_type: format!("{:?}", col_type)})
            }
        },

        Constraint::Custom { script, available_fields } => custom_constraint(script, available_fields, records, schema, lua_ctx),
    }
}

///
/// NETting takes two sets of records and SUMs a column from both. Then subtracts the SUM of the first list from the second
/// and, if the result is zero (or within a tolerance) it returns true. There must be at least one record in each subset as well.
///
/// This allows you to match a list of, for exaple, payments to a list of invoices, as long as all the payments cover the cost
/// of the invoices.
///
fn net_decimal<F>(
    column: &str,
    lhs: &str,
    rhs: &str,
    sum_checker: F,
    records: &[&Record],
    schema: &GridSchema,
    lua_ctx: &Context) -> Result<bool, MatcherError>

    where F: Fn(Decimal, Decimal) -> bool, {

    // Validate NET column exists and is a DECIMAL (we can relax the type resiction if needed).
    if !schema.headers().contains(&column.to_string()) {
        return Err(MatcherError::ConstraintColumnMissing{ column: column.into() })
    }

    // Collect records in the group which match lhs and rhs filters.
    let lhs_recs = lua::lua_filter(records, lhs, lua_ctx, schema)?;
    let rhs_recs = lua::lua_filter(records, rhs, lua_ctx, schema)?;

    // Sum the NETting column for records on both sides.
    let lhs_sum: Decimal = lhs_recs.iter().map(|r| r.get_decimal(column).unwrap_or(Some(Decimal::ZERO)).unwrap_or(Decimal::ZERO)).sum();
    let rhs_sum: Decimal = rhs_recs.iter().map(|r| r.get_decimal(column).unwrap_or(Some(Decimal::ZERO)).unwrap_or(Decimal::ZERO)).sum();

    // The constraint passes if the sides net to zero AND there is at least one record from each side.
    let net = sum_checker(lhs_sum, rhs_sum) && (!lhs_recs.is_empty() && !rhs_recs.is_empty());

    log::trace!("NET? {}, lhs.is_empty {}, rhs.is_empty {}", net, lhs_recs.is_empty(), rhs_recs.is_empty());

    // Do the records NET to zero?
    Ok(net)
}

///
/// Allow entirely custom Lua script to be evaluated for a group constraint.
///
fn custom_constraint(
    script: &str,
    available_fields: &Option<Vec<String>>,
    records: &[&Record],
    schema: &GridSchema,
    lua_ctx: &Context) -> Result<bool, MatcherError> {

    let avail_cols = match available_fields {
        Some(fields) => {
            let fields = fields.iter().map(|f|f.as_str()).collect::<Vec<&str>>();

            schema.columns()
                .into_iter()
                .filter(|col| fields.contains(&col.header()))
                .cloned()
                .collect::<Vec<Column>>()
            },
        None => schema.columns().into_iter().cloned().collect(), // No restriction, provide all columns to the Lua script.
    };

    let globals = lua_ctx.globals();
    let lua_records = lua_ctx.create_table()?;

    for (idx, record) in records.iter().enumerate() {
        let lua_record = lua::lua_record(record, &avail_cols, lua_ctx)?;
        lua_records.set(idx + 1, lua_record)?;
    }

    globals.set("records", lua_records)?;
    eval(lua_ctx, script)
        .map_err(|source| MatcherError::CustomConstraintError { reason: "Unknown".into(), source })
}


fn net_to_zero(
    column: &str,
    lhs: &str,
    rhs: &str,
    records: &[&Record],
    schema: &GridSchema,
    lua_ctx: &Context) -> Result<bool, MatcherError>
{
    // Create a closure to calculate the sum of lhs and rhs and pass it to the NET fn.
    let sum_checker = |lhs_sum: Decimal, rhs_sum: Decimal| {
        let result = (lhs_sum.abs() - rhs_sum.abs()).abs() == Decimal::ZERO;
        log::trace!("(lhs_sum.abs() - rhs_sum.abs()).abs() < 0 : ({}.abs() - {}.abs()).abs() < {} = {}", lhs_sum, rhs_sum, Decimal::ZERO, result);
        result
    };
    net_decimal(column, lhs, rhs, sum_checker, records, schema, lua_ctx)
}


fn nets_with_tolerance(
    column: &str,
    lhs: &str,
    rhs: &str,
    tol_type: &ToleranceType,
    tolerance: Decimal,
    records: &[&Record],
    schema: &GridSchema,
    lua_ctx: &Context) -> Result<bool, MatcherError>
{
    // Create a closure to calculate the sum of lhs and rhs and pass it to the NET fn.
    let sum_checker: Box<dyn Fn(Decimal, Decimal) -> bool> = match tol_type {
        ToleranceType::Amount => Box::new(|lhs_sum: Decimal, rhs_sum: Decimal| {
                let result = (lhs_sum.abs() - rhs_sum.abs()).abs() <= tolerance;
                log::trace!("(lhs_sum.abs() - rhs_sum.abs()).abs() <= tolerance : ({}.abs() - {}.abs()).abs() <= {} = {}", lhs_sum, rhs_sum, tolerance, result);
                result
            }),

        ToleranceType::Percent => Box::new(|lhs_sum: Decimal, rhs_sum: Decimal| {
                let percent_tol = lhs_sum / (Decimal::ONE_HUNDRED / tolerance);
                let result = (lhs_sum.abs() - rhs_sum.abs()).abs() <= percent_tol;
                log::trace!("(lhs_sum.abs() - rhs_sum.abs()).abs() <= percent_tol : ({}.abs() - {}.abs()).abs() <= {} = {}", lhs_sum, rhs_sum, percent_tol, result);
                result
            }),
    };

    net_decimal(column, lhs, rhs, sum_checker, records, schema, lua_ctx)
}
