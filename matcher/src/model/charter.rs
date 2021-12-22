use serde::Deserialize;
use std::io::BufReader;
use rust_decimal::Decimal;
use crate::{model::data_type::DataType, error::MatcherError};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Charter {
    name: String,
    description: Option<String>,
    version: u64, // Epoch millis at UTC.
    debug: Option<bool>,
    file_patterns: Vec<String>,
    field_aliases: Option<Vec<String>>,
    use_field_prefixes: Option<bool>,
    instructions: Option<Vec<Instruction>>,
}


#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum Instruction {
    Project { column: String, as_type: DataType, from: String, when: Option<String> }, // Create a derived column from one or more other columns.
    MergeColumns { into: String, from: Vec<String> }, // Merge the contents of columns together.
    MatchGroups { group_by: Vec<String>, constraints: Vec<Constraint> }, // Group the data by one or more columns (header-names)
}

#[derive(Debug, Deserialize)]
pub enum ToleranceType {
    Amount,
    Percent
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum Constraint {
    NetsToZero { column: String, lhs: String, rhs: String },
    NetsWithTolerance { column: String, lhs: String, rhs: String, tol_type: ToleranceType, tolerance: Decimal },
    Custom { script: String, fields: Option<Vec<String>> }
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

    pub fn field_aliases(&self) -> &Option<Vec<String>> {
        &self.field_aliases
    }

    pub fn use_field_prefixes(&self) -> bool {
        self.use_field_prefixes.unwrap_or(true)
    }

    pub fn instructions(&self) -> &[Instruction] {
        match &self.instructions {
            Some(instructions) => &instructions,
            None => &[],
        }
    }

    pub fn load(path: &str) -> Result<Self, MatcherError> {
        let rdr = BufReader::new(std::fs::File::open(path)
            .map_err(|source| MatcherError::CharterFileNotFound { path: path.into(), source })?);


        let charter: Self = serde_yaml::from_reader(rdr)
            .map_err(|source| MatcherError::InvalidCharter { path: path.into(), source })?;

        // If field_aliases are defined, there should be one for every file_pattern.
        if let Some(aliases) = charter.field_aliases() {
            if aliases.len() != charter.file_patterns().len() {
                return Err(MatcherError::CharterValidationError { reason: "If field_aliases are defined, there must be one for each defined file_pattern".into() })
            }
        }

        Ok(charter)
    }
}
