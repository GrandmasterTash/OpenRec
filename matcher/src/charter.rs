use std::time;

#[derive(Debug)]
pub struct Charter {
    name: String,
    preview: bool,
    base_currency: String,
    version: time::Instant,
    instructions: Vec<Instruction>,
}

#[derive(Debug)]
pub enum Instruction {
    SOURCE_DATA { filename: String },                    // Open a file of data by filename (wildcards allowed, eg. ('*_invoice.csv')
    PROJECT_COLUMN { name: String, lua: String },        // Create a derived column from one or more other columns.
    MERGE_COLUMNS { name: String, source: Vec<String> }, // Merge the contents of columns together.
    // PROJECT_ROWS { projection: RowsProjection },      // Create one or more rows from other rows.
    GROUP_BY { columns: Vec<String> },                   // Group the data by one or more columns (header-names)
    MATCH_GROUPS { constraints: Vec<Constraint> },       // Create match groups from the curreny grouped data. Constraints can be provided to leave unmatched data behind.
    FILTER,     // Apply a filter so only data matching the filter is currently available.
    UN_FILTER,  // Remove an applied filter.
}

// TODO: fn to return a list of match-related columns - we can then ignore other columns.

#[derive(Debug)]
pub enum Constraint {
    NETS_TO_ZERO { column: String, lhs: String, rhs: String }
    // NETS_WITH_TOLERANCE
}

impl Charter {
    pub fn new(name: String, preview: bool, base_currency: String, version: time::Instant, instructions: Vec<Instruction>) -> Self {
        Self { name, preview, base_currency, version, instructions }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn preview(&self) -> bool {
        self.preview
    }

    pub fn version(&self) -> time::Instant {
        self.version
    }

    pub fn base_currency(&self) -> &str {
        &self.base_currency
    }

    pub fn instructions(&self) -> &[Instruction] {
        &self.instructions
    }
}