use crate::models::{Key, Entry, Value};
use chrono::Utc;
use std::collections::BTreeSet;

pub struct MemTable {
    pub entries: BTreeSet<Entry>,
    pub size: usize,
}

impl MemTable {
    fn new() -> Self {
        Self {
            entries: Default::default(),
            size: 0,
        }
    }

    fn get(&self, key: &Key) -> Option<&Entry> {
        self.entries.iter().find(|it| it.key == key)
    }

    fn put(&mut self, key: Key, value: Value) {
        let entry = Entry {
            key: key.clone(),
            value: Some(value.clone()),
            timestamp: Utc::now().timestamp_micros() as u128,
            deleted: false,
        };

        self.size += match self.get(&key) {
            Some(existing) => {
                let existing_val_len = existing.value.as_ref().map(|it| it.len()).unwrap_or(0);
                value.len() - existing_val_len
            }
            None => entry.key.len() + entry.value.as_ref().map(|it| it.len()).unwrap_or(0),
        };

        self.entries.insert(entry);
    }

    fn delete(&mut self, key: Key) {
        let entry = Entry {
            key: key.clone(),
            value: None,
            timestamp: Utc::now().timestamp_micros() as u128,
            deleted: true,
        };

        self.size -= match self.get(&key) {
            Some(old) => old.value.as_ref().map(|it| it.len()).unwrap_or(0) + 16 + 1,
            None => 0,
        };
        self.entries.insert(entry);
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use super::MemTable;

    #[test]
    fn test_table() {
        let mut table = MemTable::new();

        let k1 = Bytes::from("k1");
        let k2 = Bytes::from("k2");
        let k3 = Bytes::from("k3");
        let v1 = Bytes::from("v1");
        let v2 = Bytes::from("v2");
        let v3 = Bytes::from("v3");

        table.put(k2.clone(), v2.clone());

        table.put(k1.clone(), v1);
        assert_eq!(k1, table.entries.first().unwrap().key);

        table.put(k3.clone(), v3);
        assert_eq!(k1, table.entries.first().unwrap().key);

        assert_eq!(v2, table.get(&k2).unwrap().value.clone().unwrap());
    }
}
