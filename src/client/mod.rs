use std::{fs::read_dir, path::PathBuf};

use itertools::Itertools;

use crate::{
    memtable::MemTable,
    models::{
        delete::{DeleteRequest, DeleteResponse},
        get::{GetRequest, GetResponse},
        put::{PutRequest, PutResponse},
        Entry, Key,
    },
    result::Result,
    wal::{WALIterator, WAL},
};

const RVR_EXT: &'static str = "rvr";

pub struct Rivers {
    storage_dir: PathBuf,
    memtable: MemTable,
    current_wal: WAL,
}

impl Rivers {
    pub fn new(storage_dir: PathBuf) -> Result<Self> {
        let current_wal = WAL::new(&storage_dir.to_path_buf())?;
        Ok(Self {
            storage_dir,
            memtable: MemTable::new(),
            current_wal,
        })
    }

    pub fn put(&mut self, put: PutRequest) -> PutResponse {
        let key = put.key;
        let value = put.value;
        let entry = Entry::new(key, value);

        if self.memtable.size > 2_000_000 {
            self.memtable.clear();
            if let Err(e) = self.current_wal.flush() {
                return PutResponse::Error { err: e };
            };
        }

        self.memtable
            .put(entry.key.clone(), entry.value.clone().unwrap());

        return match self.current_wal.put(entry) {
            Err(e) => PutResponse::Error { err: e },
            Ok(_) => PutResponse::Success,
        };
    }

    pub fn get(&self, get: GetRequest) -> GetResponse {
        let key = get.key;
        if let Some(entry) = self.memtable.get(&key) {
            return GetResponse::Success {
                entry: entry.clone(),
            };
        }

        let all_files = match self.get_all_files() {
            Ok(t) => t,
            Err(e) => return GetResponse::Error { err: e },
        };

        if let Err(e) = self.get_all_files() {
            return GetResponse::Error { err: e };
        }

        println!("Got {}", all_files.len());
        let key: Key = key.into();
        for file in all_files {
            println!("Looking at {}", file.to_str().unwrap());
            let mut iter = match WALIterator::new(&file) {
                Ok(t) => t,
                Err(e) => return GetResponse::Error { err: e },
            };
            if let Some(entry) = iter.find(|it| it.key == &key) {
                return GetResponse::Success { entry };
            }
        }

        GetResponse::DoesNotExist
    }

    pub fn delete(&mut self, del: DeleteRequest) -> DeleteResponse {
        let key = del.key;
        self.memtable.delete(key.clone());
        let entry = Entry::deleted(key);
        match self.current_wal.put(entry) {
            Ok(_) => DeleteResponse::Success,
            Err(e) => DeleteResponse::Error { err: e },
        }
    }

    fn get_all_files(&self) -> Result<Vec<PathBuf>> {
        Ok(read_dir(self.storage_dir.clone())?
            .filter_map(|it| it.ok())
            .sorted_by_key(|it| it.file_name())
            .map(|it| it.path())
            .filter(|it| it.extension().map(|it| it == RVR_EXT).unwrap())
            .collect())
    }
}
