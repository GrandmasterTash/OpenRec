use serde::Deserialize;
use rust_decimal::Decimal;
use std::{io::BufReader, path::Path};
use crate::{data_type::DataType, error::Error};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Charter {
    name: String,
    description: Option<String>,
    version: u64, // Epoch millis at UTC.
    debug: Option<bool>,
    matching: Matching,
    jetwash: Option<Jetwash>,
    global_lua: Option<String>,

    #[serde(default = "default_memory_limit")]
    memory_limit: usize, // The maximum number of bytes allowed for grouping and sorting data.
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Matching {
    source_files: Vec<MatchingSourceFile>,
    use_field_prefixes: Option<bool>,
    instructions: Option<Vec<Instruction>>,

    #[serde(default = "default_group_limit")]
    group_limit: usize, // The maximum number of records in a single group.
    // TODO: Rename group_size_limit
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename = "SourceFile")]
pub struct MatchingSourceFile {
    pattern: String,
    field_prefix: Option<String> // TODO: Prevent duplicate aliases.
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Jetwash {
    source_files: Vec<JetwashSourceFile>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename = "SourceFile")]
pub struct JetwashSourceFile {
    pattern: String,
    escape: Option<String>,
    quote: Option<String>,
    delimeter: Option<String>,
    headers: Option<Vec<String>>,
    column_mappings: Option<Vec<ColumnMapping>>,
    new_columns: Option<Vec<NewColumn>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewColumn {
    column: String,
    as_a: DataType,
    from: String
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ColumnMapping {
    Map { column: String, as_a: DataType, from: String  }, // Lua script creating a new column.
    Dmy ( String /* column */ ),  // Parse a day/month/year into a UTC Datetime
    Mdy ( String /* column */ ),  // Parse a month/day/year into a UTC Datetime
    Ymd ( String /* column */ ),  // Parse a year/month/day into a UTC Datetime
    Trim ( String /* column */ ), // Trim whitespace from the value.
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
    DatesWithRange { column: String, lhs: String, rhs: String, days: Option<u16>, hours: Option<u16>, minutes: Option<u16>, seconds: Option<u16> },
    Custom { script: String, fields: Option<Vec<String>> }
}

impl Jetwash {
    pub fn source_files(&self) -> &[JetwashSourceFile] {
        &self.source_files
    }
}

impl JetwashSourceFile {
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    pub fn escape(&self) -> &Option<String> {
        &self.escape
    }

    pub fn quote(&self) -> &Option<String> {
        &self.quote
    }

    pub fn delimeter(&self) -> &Option<String> {
        &self.delimeter
    }

    pub fn headers(&self) -> &Option<Vec<String>> {
        &self.headers
    }

    pub fn column_mappings(&self) -> &Option<Vec<ColumnMapping>> {
        &self.column_mappings
    }

    pub fn new_columns(&self) -> &Option<Vec<NewColumn>> {
        &self.new_columns
    }
}

impl MatchingSourceFile {
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    pub fn field_prefix(&self) -> &Option<String> {
        &self.field_prefix
    }
}

impl NewColumn {
    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn as_a(&self) -> DataType {
        self.as_a
    }

    pub fn from(&self) -> &str {
        &self.from
    }
}

impl ColumnMapping {
    pub fn column(&self) -> &str {
        match self {
            ColumnMapping::Map { column, .. } => column,
            ColumnMapping::Dmy( column )      => column,
            ColumnMapping::Mdy( column )      => column,
            ColumnMapping::Ymd( column )      => column,
            ColumnMapping::Trim( column )     => column,
        }
    }
}

impl Charter {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> &Option<String> {
        &self.description
    }

    pub fn debug(&self) -> bool {
        self.debug.unwrap_or(false)
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn memory_limit(&self) -> usize {
        self.memory_limit
    }

    pub fn group_limit(&self) -> usize {
        self.matching.group_limit
    }

    pub fn source_files(&self) -> &[MatchingSourceFile] {
        &self.matching.source_files
    }

    pub fn use_field_prefixes(&self) -> bool {
        self.matching.use_field_prefixes.unwrap_or(true)
    }

    pub fn global_lua(&self) -> &Option<String> {
        &self.global_lua
    }

    pub fn instructions(&self) -> &[Instruction] {
        match &self.matching.instructions {
            Some(instructions) => instructions,
            None => &[],
        }
    }

    pub fn jetwash(&self) -> &Option<Jetwash> {
        &self.jetwash
    }

    pub fn load(path: &Path) -> Result<Self, Error> {
        let rdr = BufReader::new(std::fs::File::open(&path)
            .map_err(|source| Error::CharterFileNotFound { path: path.to_string_lossy().into(), source })?);


        let charter: Self = serde_yaml::from_reader(rdr)
            .map_err(|source| Error::InvalidCharter { path: path.to_string_lossy().into(), source })?;

        // If field_aliases are defined, there should be one for every file_pattern.
        let count_aliases = charter.source_files().iter().filter(|df| df.field_prefix.is_some() ).count();
        if count_aliases > 0 && count_aliases != charter.source_files().len() {
            return Err(Error::CharterValidationError { reason: "If field_aliases are defined, there must be one for each defined file_pattern".into() })
        }

        // TODO 'META' is a reserved word and can't be an alias.

        Ok(charter)
    }
}

fn default_group_limit() -> usize {
    1000
}

fn default_memory_limit() -> usize {
    52428800 // 50MB, 50 * 1048576
}