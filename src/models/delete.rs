use crate::result::RiversError;

use super::Key;

/// Delete Request
pub struct DeleteRequest {
    pub key: Key,
}

/// Delete Response
pub enum DeleteResponse {
    Success,
    Error { err: RiversError },
}
