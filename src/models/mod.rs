use bytes::Bytes;
use std::cmp::Ordering;

pub type Value = Bytes;
pub type Key = Bytes;
pub type Timestamp = u128;

#[derive(Debug, Eq, PartialEq, Ord)]
pub struct Entry {
    pub key: Key,
    pub value: Option<Value>,
    pub timestamp: Timestamp,
    pub deleted: bool,
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}
