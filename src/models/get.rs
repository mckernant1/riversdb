use crate::result::RiversError;

use super::{Entry, Key};

/// Get Request
pub struct GetRequest {
    pub key: Key,
}

/// Get Response
pub enum GetResponse {
    Success { entry: Entry },
    DoesNotExist,
    Error { err: RiversError },
}
