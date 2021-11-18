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

    /// Represents all other cases of `std::io::Error`.
    #[error(transparent)]
    IOError(#[from] std::io::Error), 

}