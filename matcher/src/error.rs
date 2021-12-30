use thiserror::Error;
use core::data_type::DataType;

#[derive(Error, Debug)]
pub enum MatcherError {

    #[error("Charter {path} not found")]
    CharterFileNotFound { path: String, source: std::io::Error },

    #[error("Charter {path} contains invalid configuration")]
    InvalidCharter { path: String, source: serde_yaml::Error },

    #[error("Path {path} is not a file and has no filename")]
    PathNotAFile { path: String },

    #[error("Attempted to remove the .inprogress suffix from {path}")]
    FileNotInProgress { path: String },

    #[error("Unable to rename file {from} to {to}")]
    CannotRenameFile { from: String, to: String, source: std::io::Error },

    #[error("Unable to create directory {path}")]
    CannotCreateDir { path: String, source: std::io::Error },

    #[error("Unable to delete file {filename}")]
    CannotDeleteFile { filename: String, source: std::io::Error },

    #[error("Unable to create unmatched file {path}")]
    CannotCreateUnmatchedFile { path: String, source: csv::Error },

    #[error("A record indexed a file not in the schema/grid {index}")]
    MissingFileInSchema { index: usize },

    #[error("The schema for file {filename} {index} was not in the data grid")]
    MissingSchemaInGrid { filename: String, index: usize },

    #[error("The file {filename} doesn't have a valid timestamp prefix")]
    InvalidTimestampPrefix { filename: String },

    #[error("Unable to read a sourced data file {path} due to {description}")]
    BadSourceFile { path: String, description: String },

    #[error("Unable to open file {path}")]
    CannotOpenCsv { path: String, source: csv::Error },

    #[error("Unable to read row from {path}")]
    CannotParseCsvRow { path: String, source: csv::Error },

    #[error("Unable to read CSV {data_type} field from {bytes}")]
    UnparseableCsvField { data_type: String, bytes: String },

    #[error("Unable to parse type {data_type} field from {bytes}")]
    UnparseableInternalBytesField { data_type: String, bytes: String },

    #[error("CSV file had no initial schema row")]
    NoSchemaRow { source: csv::Error },

    #[error("Cannot read CSV headers")]
    CannotReadHeaders { source: csv::Error },

    #[error("Unable to write headers to {filename}")]
    CannotWriteHeaders { filename: String, source: csv::Error },

    #[error("Unable to write schema to {filename}")]
    CannotWriteSchema { filename: String, source: csv::Error },

    #[error("Unable to write {thing} to {filename}")]
    CannotWriteThing { thing: String, filename: String, source: std::io::Error },

    #[error("Unable to write matched record row to {filename}")]
    CannotWriteMatchedRecord { filename: String, source: serde_json::Error },

    #[error("Unable to write unmatched record row {row} to {filename}")]
    CannotWriteUnmatchedRecord { filename: String, row: usize, source: csv::Error },

    #[error("Unable to write matched file footer to {filename}")]
    CannotWriteFooter { filename: String, source: serde_json::Error },

    #[error("Cannot read derived data from record in row {row} for file {file_idx}, no derived index")]
    NoDerivedPosition { row: usize, file_idx: usize },

    #[error("The column {column} is not in the file {file}")]
    MissingColumn { column: String, file: String },

    #[error("Unknown data type specified in column {column}")]
    UnknownDataTypeInColumn { column: isize },

    #[error("Unknown data type specified for header {header}")]
    UnknownDataTypeForHeader { header: String },

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

    #[error("Instruction {instruction} was missing a set of script columms")]
    MissingScriptCols { instruction: usize },

    #[error("Lua error in script\neval: {eval}\nreturn type: {data_type}\nwhen: {when}\row: {row}")]
    ProjectColScriptError { eval: String, when: String, data_type: String, row: usize, source: rlua::Error },

    #[error("Error in custom Lua constraint: {reason}")]
    CustomConstraintError { reason: String, source: rlua::Error },

    #[error("Column {header} doesn't exist in the source data and cannot be used to merge")]
    MissingSourceColumn { header: String },

    #[error("There are no valid source columns defined for the merge")]
    NoValidSourceColumns {},

    #[error("The source column {header} has type {this_type:?} which wont merge with {other_type:?}")]
    InvalidSourceDataType { header: String, this_type: DataType, other_type: DataType},

    #[error("An error occured processing changeset {changeset} on record {row} from file {file}")]
    ChangeSetError { changeset: String, row: usize, file: String, source: rlua::Error },

    #[error("An error occured processing instruction {instruction} on record {row} from file {file}")]
    DeriveDataError { instruction: String, row: usize, file: String, source: rlua::Error },

    #[error("A problem occured during the match")]
    MatchGroupError { source: rlua::Error },

    #[error("The constraint column {column} is not present")]
    ConstraintColumnMissing { column: String },

    #[error("The constraint column {column} is not a DECIMAL data-type")]
    ConstraintColumnNotDecimal { column: String },

    #[error("Failed to write the match job header {job_header} to {path}")]
    FailedToWriteJobHeader { job_header: String, path: String, source: serde_json::Error },

    #[error("Unmatched record's file {file_idx} not found in grid")]
    UnmatchedFileNotInGrid { file_idx: usize },

    #[error("Unmatched file {filename} was not found in the unmatched handler")]
    UnmatchedFileNotInHandler { filename: String },

    #[error("Constraint {index} evaluation failed")]
    ConstraintError { index: usize, source: rlua::Error },

    #[error("Charter failed to load")]
    CharterLoadError ( #[from] core::error::Error ),

    #[error("Cannot create a derived file for {path}")]
    FileCantBeDerived { path: String },

    #[error("Unable to load changeset {path}")]
    UnableToParseChangset { path: String, source: serde_json::Error },

    #[error("An error occured sourcing data")]
    GridSourceError { source: rlua::Error },

    #[error(transparent)]
    LuaError(#[from] rlua::Error),

    #[error(transparent)]
    CSVError(#[from] csv::Error),

    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

///
/// This allows us to return MatcherErrors inside Lua contexts and have them wrapped
/// and exposed outside the context without having to map_err everywhere.
///
impl From<MatcherError> for rlua::Error {
    fn from(err: MatcherError) -> Self {
        rlua::Error::external(err)
    }
}