use thiserror::Error;

/// Result type local to emsqrt-mem.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("memory budget exceeded for tag '{tag}': requested {requested} bytes, capacity {capacity}, used {used}")]
    BudgetExceeded {
        tag: &'static str,
        requested: usize,
        capacity: usize,
        used: usize,
    },

    #[error("allocation failed for {bytes} bytes (tag '{tag}')")]
    AllocFailed { tag: &'static str, bytes: usize },

    #[error("memory budget error: {0}")]
    Budget(String),

    #[error("spill storage error: {0}")]
    Storage(String),

    #[error("unsupported codec: {0}")]
    CodecUnsupported(&'static str),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("checksum mismatch")]
    ChecksumMismatch,
}
