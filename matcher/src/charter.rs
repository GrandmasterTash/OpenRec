use crate::{data_type::DataType};

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
    SourceData { filename: /* TODO: rename file_pattern*/ String },                    // Open a file of data by filename (wildcards allowed, eg. ('*_invoice.csv')
    ProjectColumn { name: String, data_type: DataType, eval: String, when: String }, // Create a derived column from one or more other columns.
    MergeColumns { name: String, source: Vec<String> }, // Merge the contents of columns together.
    GroupBy { columns: Vec<String> },                   // Group the data by one or more columns (header-names)
    UnGroup,                                            // Remove any groupings on the data.
    MatchGroups { constraints: Vec<Constraint> },       // Create match groups from the curreny grouped data. Constraints can be provided to leave unmatched data behind.
    Filter,                                             // Apply a filter so only data matching the filter is currently available.
    UnFilter,                                           // Remove an applied filter.
}

#[derive(Debug)]
pub enum Constraint {
    NetsToZero { column: String, lhs: String, rhs: String }
    // NETS_WITH_TOLERANCE
}

impl Charter {
    pub fn new(name: String, preview: bool, base_currency: String, version: u64, instructions: Vec<Instruction>) -> Self {
        Self { name, preview, base_currency, version, instructions }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn preview(&self) -> bool {
        self.preview
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

    // pub fn matching_headers(&self) -> Vec<String> {
    //     // TODO: Return all the column headers involved in matching - basically anything in: -
    //     // MERGE_COLUMN.sources
    //     // GROUP_BY.columns
    //     // Anything that looks like 'xxx.yyy' in a any lua script.
    //     todo!()
    // }
}
