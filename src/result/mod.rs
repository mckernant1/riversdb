use thiserror::Error;

pub type Result<T> = std::result::Result<T, RiversError>;

#[derive(Debug, Error)]
pub enum RiversError {
    #[error("File Related Error")]
    FileErr(#[from] std::io::Error),
    #[error("Error Interpreting WAL {:?}", .msg)]
    WalErr {
        msg: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Key not found")]
    KeyNotFound,
}

impl RiversError {
    pub fn wal_err(msg: &str, e: std::io::Error) -> RiversError {
        RiversError::WalErr {
            msg: msg.to_string(),
            source: e,
        }
    }
}
