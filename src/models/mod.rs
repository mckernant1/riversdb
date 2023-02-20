pub mod delete;
pub mod get;
pub mod put;

use bytes::Bytes;
use chrono::Utc;
use std::cmp::Ordering;

pub type Value = Bytes;
pub type Key = Bytes;
pub type Timestamp = u128;

#[derive(Clone, Debug, Eq, PartialEq, Ord)]
pub struct Entry {
    pub key: Key,
    pub value: Option<Value>,
    pub timestamp: Timestamp,
    pub deleted: bool,
}

impl Entry {
    pub fn new<T: Into<Bytes>>(key: T, value: T) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
            timestamp: Utc::now().timestamp_micros() as u128,
            deleted: false,
        }
    }

    pub fn deleted<T: Into<Bytes>>(key: T) -> Self {
        Self {
            key: key.into(),
            value: None,
            timestamp: Utc::now().timestamp_micros() as u128,
            deleted: true,
        }
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}
