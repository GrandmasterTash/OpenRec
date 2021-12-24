use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Charter {path} not found")]
    CharterFileNotFound { path: String, source: std::io::Error },

    #[error("Charter {path} contains invalid configuration")]
    InvalidCharter { path: String, source: serde_yaml::Error },

    #[error("Chart configuration is invalid - {reason}")]
    CharterValidationError { reason: String },
}