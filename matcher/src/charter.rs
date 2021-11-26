use rlua::Context;
use std::time::Duration;
use rust_decimal::Decimal;
use humantime::format_duration;
use crate::{data_type::DataType, error::MatcherError, lua, record::Record, schema::GridSchema};

#[derive(Debug)]
pub struct Charter {
    name: String,
    version: u64, // Epoch millis at UTC.
    debug: bool,
    base_currency: String, // TODO: Is this required for matching?
    instructions: Vec<Instruction>,
    // TODO: Start at, end at
}

#[derive(Debug)]
pub enum Instruction {
    SourceData { filename: /* TODO: rename file_pattern and use array rather than multiple instructions */ String }, // Open a file of data by filename (wildcards allowed, eg. ('*_invoice.csv')
    ProjectColumn { name: String, data_type: DataType, eval: String, when: String }, // Create a derived column from one or more other columns.
    MergeColumns { name: String, source: Vec<String> }, // Merge the contents of columns together.
    MatchGroups { group_by: Vec<String>, constraints: Vec<Constraint> }, // Group the data by one or more columns (header-names)
    _Filter, // TODO: Apply a filter so only data matching the filter is currently available.
    _UnFilter, // TODO: Remove an applied filter.
}

// TODO: Push constraint into own file.
#[derive(Debug)]
pub enum Constraint {
    NetsToZero { column: String, lhs: String, rhs: String, debug: bool }
    // TODO: NETS_WITH_TOLERANCE
    // Custom Lua with access to Count, Sum and all records in the group (so table of tables): records[1]["invoices.blah"]
}

impl Charter {
    pub fn new(name: String, debug: bool, base_currency: String, version: u64, instructions: Vec<Instruction>) -> Self {
        Self { name, debug, base_currency, version, instructions }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn debug(&self) -> bool {
        self.debug
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn base_currency(&self) -> &str {
        &self.base_currency
    }

    pub fn instructions(&self) -> &[Instruction] {
        &self.instructions
    }
}

impl Constraint {
    pub fn passes(&self, records: &[&Box<Record>], schema: &GridSchema, lua_ctx: &Context)
        -> Result<bool, rlua::Error> {

        match self {
            // TODO: Push into fn.
            Constraint::NetsToZero { column, lhs, rhs, debug } => {
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
                let net = (lhs_sum - rhs_sum) == Decimal::ZERO;

                // If the records don't net then, if we're debugging the constraint, output the group values.
                if !net && *debug {
                    let mut dbg = format!("{}\n", column);
                    lhs_recs.iter().for_each(|r| dbg += &format!("{:<30}: {}\n", r.get_decimal(column, schema).unwrap_or(Decimal::ZERO), lhs) );
                    dbg += &format!("{}: SUM\n", ansi_term::Colour::RGB(70, 130, 180).paint(format!("{:<30}", lhs_sum)));

                    rhs_recs.iter().for_each(|r| dbg += &format!("{:<30}: {}\n", r.get_decimal(column, schema).unwrap_or(Decimal::ZERO), rhs) );
                    dbg += &format!("{}: SUM", ansi_term::Colour::RGB(70, 130, 180).paint(format!("{:<30}", rhs_sum)));
                    log::info!("NetToZero constraint failed for group\n{}", dbg);
                }

                // Do the records NET to zero?
                Ok(net)
            },
        }
    }
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
/// Provide a consistent formatting for durations and rates.
///
/// The format_duration will show micro and nano seconds but we typically only need to see ms.
///
pub fn formatted_duration_rate(amount: usize, elapsed: Duration) -> (String, String) {
    let duration = Duration::new(elapsed.as_secs(), elapsed.subsec_millis() * 1000000); // Keep precision to ms.
    let rate = (elapsed.as_millis() as f64 / amount as f64) as f64;
    (
        format_duration(duration).to_string(),
        format!("{:.3}ms", rate)
    )
}