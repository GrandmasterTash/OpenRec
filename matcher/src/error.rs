use thiserror::Error;

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
    ProjectedColumnExists { header: String, /* schema: String,  script: String*/ },

    #[error("Lua error in script\neval: {eval}\nreturn type: {data_type}\nwhen: {when}\nrecord: {record}")]
    ScriptError { eval: String, when: String, data_type: String, record: String, source: rlua::Error },

    #[error(transparent)]
    LuaError(#[from] rlua::Error),

    /// Represents all other cases of `std::io::Error`.
    #[error(transparent)]
    IOError(#[from] std::io::Error),

}