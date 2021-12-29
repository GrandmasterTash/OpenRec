use thiserror::Error;

#[derive(Error, Debug)]
pub enum JetwashError {

    #[error("Cannot start a new job as there are failed files from a previous job which cannot be read. Manual intervention is required.")]
    PreviousFailures,

    #[error("Unable to create directory {path}")]
    CannotCreateDir { path: String, source: std::io::Error },

    #[error("Unable to open file {path}")]
    CannotOpenCsv { path: String, source: csv::Error },

    #[error("Unable to read row from {path}")]
    CannotParseCsvRow { path: String, source: csv::Error },

    #[error("Unable to write schema to {filename}")]
    CannotWriteSchema { filename: String, source: csv::Error },

    #[error("Charter contained an invalid regular expression")]
    InvalidSourceFileRegEx { source: regex::Error },

    #[error("Unable to move file from {path} to {destination}")]
    CannotMoveFile { path: String, destination: String, source: std::io::Error },

    #[error("Encountered one or more errors in inbox files during data analysis - job aborted")]
    AnalysisErrors,

    #[error("Charter failed to load")]
    CharterLoadError ( #[from] core::error::Error ),

    #[error(transparent)]
    CSVError(#[from] csv::Error),

    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

// ///
// /// This allows us to return JetwashErrors inside Lua contexts and have them wrapped
// /// and exposed outside the context without having to map_err everywhere.
// ///
// impl From<JetwashError> for rlua::Error {
//     fn from(err: JetwashError) -> Self {
//         rlua::Error::external(err)
//     }
// }