use rlua::Context;
use rust_decimal::Decimal;
use core::{data_type::DataType, charter::{Constraint, ToleranceType}};
use crate::{model::{record::Record, schema::{Column, GridSchema}}, error::MatcherError, lua, data_accessor::DataAccessor};

pub fn passes(
    constraint: &Constraint,
    records: &[&Record],
    schema: &GridSchema,
    accessor: &mut DataAccessor,
    lua_ctx: &Context) -> Result<bool, MatcherError> {

    match constraint {
        // Create a closure to calculate the sum of lhs and rhs and pass it to the NET fn.
        Constraint::NetsToZero { column, lhs, rhs } => {
            let sum_checker = |lhs_sum: Decimal, rhs_sum: Decimal| {
                let result = (lhs_sum.abs() - rhs_sum.abs()).abs() == Decimal::ZERO;
                log::trace!("(lhs_sum.abs() - rhs_sum.abs()).abs() < 0 : ({}.abs() - {}.abs()).abs() < {} = {}", lhs_sum, rhs_sum, Decimal::ZERO, result);
                result
            };
            net(column, lhs, rhs, sum_checker, records, schema, accessor, lua_ctx)
        },

        // Create a closure to calculate the sum of lhs and rhs and pass it to the NET fn.
        Constraint::NetsWithTolerance {column, lhs, rhs, tol_type, tolerance } => {
            let sum_checker: Box<dyn Fn(Decimal, Decimal) -> bool> = match tol_type {
                ToleranceType::Amount => Box::new(|lhs_sum: Decimal, rhs_sum: Decimal| {
                        let result = (lhs_sum.abs() - rhs_sum.abs()).abs() < *tolerance;
                        log::trace!("(lhs_sum.abs() - rhs_sum.abs()).abs() < tolerance : ({}.abs() - {}.abs()).abs() < {} = {}", lhs_sum, rhs_sum, *tolerance, result);
                        result
                    }),

                ToleranceType::Percent => Box::new(|lhs_sum: Decimal, rhs_sum: Decimal| {
                        let percent_tol = lhs_sum / (Decimal::ONE_HUNDRED / *tolerance);
                        let result = (lhs_sum.abs() - rhs_sum.abs()).abs() < percent_tol;
                        log::trace!("(lhs_sum.abs() - rhs_sum.abs()).abs() < percent_tol : ({}.abs() - {}.abs()).abs() < {} = {}", lhs_sum, rhs_sum, percent_tol, result);
                        result
                    }),
            };

            net(column, lhs, rhs, sum_checker, records, schema, accessor, lua_ctx)
        },

        Constraint::Custom { script, fields } => custom_constraint(script, fields, records, schema, accessor, lua_ctx),
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
    records: &[&Record],
    schema: &GridSchema,
    accessor: &mut DataAccessor,
    lua_ctx: &Context) -> Result<bool, MatcherError>

    where F: Fn(Decimal, Decimal) -> bool, {

    // Validate NET column exists and is a DECIMAL (we can relax the type resiction if needed).
    if !accessor.schema().headers().contains(column) {
        return Err(MatcherError::ConstraintColumnMissing{ column: column.into() })
    }

    if *accessor.schema().data_type(column).unwrap_or(&DataType::Unknown) != DataType::Decimal {
        return Err(MatcherError::ConstraintColumnNotDecimal{ column: column.into() })
    }

    // Collect records in the group which match lhs and rhs filters.
    let lhs_recs = lua::lua_filter(records, lhs, lua_ctx, accessor, schema)?;
    let rhs_recs = lua::lua_filter(records, rhs, lua_ctx, accessor, schema)?;

    // Sum the NETting column for records on both sides.
    let lhs_sum: Decimal = lhs_recs.iter().map(|r| r.get_decimal(column, accessor).unwrap_or(Some(Decimal::ZERO)).unwrap_or(Decimal::ZERO)).sum();
    let rhs_sum: Decimal = rhs_recs.iter().map(|r| r.get_decimal(column, accessor).unwrap_or(Some(Decimal::ZERO)).unwrap_or(Decimal::ZERO)).sum();

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
    fields: &Option<Vec<String>>,
    records: &[&Record],
    schema: &GridSchema,
    accessor: &mut DataAccessor,
    lua_ctx: &Context) -> Result<bool, MatcherError> {

    let script_cols = match fields {
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
        let lua_record = lua::lua_record(record, &script_cols, accessor, lua_ctx)?;
        lua_records.set(idx + 1, lua_record)?;
    }

    globals.set("records", lua_records)?;
    lua::eval(lua_ctx, &script)
        .map_err(|source| MatcherError::CustomConstraintError { reason: "Unknown".into(), source })
}