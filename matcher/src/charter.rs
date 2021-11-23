use rlua::Context;
use rust_decimal::Decimal;
use crate::{data_type::DataType, error::MatcherError, lua, record::Record, schema::GridSchema};

#[derive(Debug)]
pub struct Charter {
    name: String,
    version: u64, // Epoch millis at UTC.
    preview: bool,
    base_currency: String,
    instructions: Vec<Instruction>,
    // TODO: Start at, end at
}

#[derive(Debug)]
pub enum Instruction {
    SourceData { filename: /* TODO: rename file_pattern*/ String }, // Open a file of data by filename (wildcards allowed, eg. ('*_invoice.csv')
    ProjectColumn { name: String, data_type: DataType, eval: String, when: String }, // Create a derived column from one or more other columns.
    MergeColumns { name: String, source: Vec<String> }, // Merge the contents of columns together.
    MatchGroups { group_by: Vec<String>, constraints: Vec<Constraint> }, // Group the data by one or more columns (header-names)
    _Filter, // Apply a filter so only data matching the filter is currently available.
    _UnFilter, // Remove an applied filter.
}

#[derive(Debug)]
pub enum Constraint {
    NetsToZero { column: String, lhs: String, rhs: String, debug: bool }
    // NETS_WITH_TOLERANCE
    // Custom Lua
}

impl Charter {
    pub fn new(name: String, preview: bool, base_currency: String, version: u64, instructions: Vec<Instruction>) -> Self {
        Self { name, preview, base_currency, version, instructions }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    // pub fn preview(&self) -> bool {
    //     self.preview
    // }

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
            Constraint::NetsToZero { column, lhs, rhs, debug } => {
                // Validate column exists and is a DECIMAL.
                if !schema.headers().contains(column) {
                    return Err(rlua::Error::external(MatcherError::ConstraintColumnMissing{ column: column.into() }))
                }

                if *schema.data_type(column).unwrap_or(&DataType::UNKNOWN) != DataType::DECIMAL {
                    return Err(rlua::Error::external(MatcherError::ConstraintColumnNotDecimal{ column: column.into() }))
                }

                // Collect records in the group which match lhs.
                let lhs_recs = lua_filter(records, lhs, lua_ctx, schema)?;
                let rhs_recs = lua_filter(records, rhs, lua_ctx, schema)?;

                // Sum the NETting column for records on both sides.
                let lhs_sum: Decimal = lhs_recs.iter().map(|r| r.get_decimal(column, schema).unwrap_or(Decimal::ZERO)).sum();
                let rhs_sum: Decimal = rhs_recs.iter().map(|r| r.get_decimal(column, schema).unwrap_or(Decimal::ZERO)).sum();
                let net = (lhs_sum - rhs_sum) == Decimal::ZERO;

                // If the records don't net, then output the group values.
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