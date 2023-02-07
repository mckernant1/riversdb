use thiserror::Error;

pub type Result<T> = std::result::Result<T, RiversError>;

#[derive(Debug, Error)]
pub enum RiversError {
    #[error("File Related Error")]
    FileErr(#[from] std::io::Error),
}
