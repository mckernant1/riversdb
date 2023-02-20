use crate::result::RiversError;

use super::{Key, Value};

/// Put Request
pub struct PutRequest {
    pub key: Key,
    pub value: Value,
}

pub enum PutResponse {
    Success,
    Error { err: RiversError },
}
