use thiserror::Error;
use crate::data_type::DataType;

#[derive(Error, Debug)]
pub enum MatcherError {

    #[error("Unable to create directory {path}")]
    CannotCreateDir { path: String, source: std::io::Error },

    #[error("Unable to open file {path}")]
    CannotOpenCsv { path: String, source: csv::Error },

    #[error("Unable to read row from {path}")]
    CannotParseCsvRow { path: String, source: csv::Error },

    #[error("CSV file had no initial schema row")]
    NoSchemaRow { source: csv::Error },

    #[error("Cannot read CSV headers")]
    CannotReadHeaders { source: csv::Error },

    #[error("Unknown data type specified in column {column}")]
    UnknownDataTypeInColumn { column: usize },

    #[error("No data type specified for column {column}")]
    NoSchemaTypeForColumn { column: usize },

    #[error("Charter contained an invalid regular expression")]
    InvalidSourceFileRegEx { source: regex::Error },

    #[error("Schemas for {filename} must be the same, found these two schemas: -\n[{first}]\n[{second}]")]
    SchemaMismatch { filename: String, first: String, second: String },

    #[error("Projected column {header} already exists")]
    ProjectedColumnExists { header: String, },

    #[error("Merged column {header} already exists")]
    MergedColumnExists { header: String },

    #[error("Lua error in script\neval: {eval}\nreturn type: {data_type}\nwhen: {when}\nrecord: {record}")]
    ProjectColScriptError { eval: String, when: String, data_type: String, record: String, source: rlua::Error },

    #[error("Column {header} doesn't exist in the source data and cannot be used to merge")]
    MissingSourceColumn { header: String },

    #[error("There are no valid source columns defined for the merge")]
    NoValidSourceColumns {},

    #[error("The source column {header} has type {this_type:?} which wont merge with {other_type:?}")]
    InvalidSourceDataType { header: String, this_type: DataType, other_type: DataType},

    #[error("A script problem occured during the match")]
    MatchScriptError { source: rlua::Error },

    #[error("The constraint column {column} is not present")]
    ConstraintColumnMissing { column: String },

    #[error("The constraint column {column} is not a DECIMAL data-type")]
    ConstraintColumnNotDecimal { column: String },

    #[error("Failed to write the match job header {job_header} to {path}")]
    FailedToWriteJobHeader { job_header: String, path: String, source: serde_json::Error },

    #[error(transparent)]
    LuaError(#[from] rlua::Error),

    /// Represents all other cases of `std::io::Error`.
    #[error(transparent)]
    IOError(#[from] std::io::Error),

}