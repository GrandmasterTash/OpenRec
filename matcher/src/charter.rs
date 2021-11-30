use rust_decimal::Decimal;
use serde::Deserialize;
use std::io::BufReader;
use crate::{data_type::DataType, error::MatcherError};

#[derive(Debug, Deserialize)]
pub struct Charter {
    name: String,
    description: Option<String>,
    version: u64, // Epoch millis at UTC.
    debug: Option<bool>,
    file_patterns: Vec<String>,
    field_prefixes: Option<bool>,
    instructions: Vec<Instruction>,
    // TODO: Start at, end at, schema difference handling.
}


#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Instruction {
    Project { column: String, as_type: DataType, from: String, when: Option<String> }, // Create a derived column from one or more other columns.
    MergeColumns { into: String, from: Vec<String> }, // Merge the contents of columns together.
    MatchGroups { group_by: Vec<String>, constraints: Vec<Constraint> }, // Group the data by one or more columns (header-names)
    _Filter, // TODO: Apply a filter so only data matching the filter is currently available.
    _UnFilter, // TODO: Remove an applied filter.
}

#[derive(Debug, Deserialize)]
pub enum ToleranceType {
    Amount,
    Percent
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Constraint {
    NetsToZero { column: String, lhs: String, rhs: String, debug: Option<bool> },
    NetsWithTolerance { column: String, lhs: String, rhs: String, tol_type: ToleranceType, tolerance: Decimal, debug: Option<bool> },
    Custom { script: String, fields: Option<Vec<String>> }
    // TODO: Count, Sum, Min, Max, Avg is required!
    // Custom Lua with access to Count, Sum and all records in the group (so table of tables): records[1]["invoices.blah"]
}

impl Charter {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn debug(&self) -> bool {
        self.debug.unwrap_or(false)
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn file_patterns(&self) -> &[String] {
        &self.file_patterns
    }

    pub fn field_prefixes(&self) -> bool {
        self.field_prefixes.unwrap_or(true)
    }

    pub fn instructions(&self) -> &[Instruction] {
        &self.instructions
    }

    pub fn load(path: &str) -> Result<Self, MatcherError> {
        let rdr = BufReader::new(std::fs::File::open(path)
            .map_err(|source| MatcherError::CharterFileNotFound { path: path.into(), source })?);

        Ok(serde_yaml::from_reader(rdr)
            .map_err(|source| MatcherError::InvalidCharter { path: path.into(), source })?)
    }
}
