use thiserror::Error;

/// Canonical result for core.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Planning error: {0}")]
    Plan(String),

    #[error("Hashing error: {0}")]
    Hash(String),

    // The core crate does not do I/O, but higher layers may map their I/O
    // errors into this variant for convenience.
    #[error("I/O-like error (mapped into core): {0}")]
    IoLike(String),

    #[error("Internal invariant failed: {0}")]
    Invariant(String),
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Hash(e.to_string())
    }
}
