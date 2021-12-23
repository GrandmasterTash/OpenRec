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
    matching: Matching,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Matching {
    source_files: Vec<SourceFile>,
    use_field_prefixes: Option<bool>,
    instructions: Option<Vec<Instruction>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceFile {
    pattern: String,
    field_prefix: Option<String>
}

impl SourceFile {
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    pub fn field_prefix(&self) -> &Option<String> {
        &self.field_prefix
    }
}


#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum Instruction {
    Project { column: String, as_a: DataType, from: String, when: Option<String> }, // Create a derived column from one or more other columns.
    Merge { into: String, columns: Vec<String> }, // Merge the contents of columns together.
    Group { by: Vec<String>, match_when: Vec<Constraint> }, // Group the data by one or more columns (header-names)
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

    pub fn source_files(&self) -> &[SourceFile] {
        &self.matching.source_files
    }

    pub fn use_field_prefixes(&self) -> bool {
        self.matching.use_field_prefixes.unwrap_or(true)
    }

    pub fn instructions(&self) -> &[Instruction] {
        match &self.matching.instructions {
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
        let count_aliases = charter.source_files().iter().filter(|df| df.field_prefix.is_some() ).count();
        if count_aliases > 0 {
            if count_aliases != charter.source_files().len() {
                return Err(MatcherError::CharterValidationError { reason: "If field_aliases are defined, there must be one for each defined file_pattern".into() })
            }
        }

        Ok(charter)
    }
}
