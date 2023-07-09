use chrono::Utc;

use crate::{
    models::{Entry, Key, Timestamp, Value},
    result::{Result, RiversError},
};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    vec,
};

pub struct WAL {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl WAL {
    pub fn new(dir: &Path) -> Result<Self> {
        let ts = Utc::now().timestamp_micros();
        let path = Path::new(dir).join(format!("{}.rvr", ts));
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let writer = BufWriter::new(file);

        Ok(Self { path, writer })
    }

    pub fn from_path(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            path: path.to_owned(),
            writer,
        })
    }

    pub fn put(&mut self, entry: Entry) -> Result<()> {
        self.writer.write_all(&entry.key.len().to_le_bytes())?;
        self.writer
            .write_all(&(entry.deleted as u8).to_le_bytes())?;
        if !entry.deleted {
            self.writer.write_all(
                &entry
                    .value
                    .clone()
                    .map(|it| it.len())
                    .unwrap_or(0)
                    .to_le_bytes(),
            )?;
        }
        self.writer.write_all(&entry.key)?;
        if !entry.deleted {
            self.writer.write_all(&entry.value.unwrap_or_default())?;
        }
        self.writer.write_all(&entry.timestamp.to_le_bytes())?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        Ok(self.writer.flush()?)
    }
}

pub struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    pub fn new(path: &PathBuf) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(Self { reader })
    }
}

impl WALIterator {
    fn next_internal(&mut self) -> Result<Entry> {
        let mut len_buffer = [0; 8];

        self.reader
            .read_exact(&mut len_buffer)
            .map_err(|e| RiversError::wal_err("Failed to read key len", e))?;

        let key_len = usize::from_le_bytes(len_buffer);

        let mut bool_buffer = [0; 1];

        self.reader
            .read_exact(&mut bool_buffer)
            .map_err(|e| RiversError::wal_err("Failed to read deleted bool", e))?;

        let deleted = bool_buffer[0] != 0;
        let mut key = vec![0; key_len];
        let mut value = None;

        if deleted {
            self.reader
                .read_exact(&mut key)
                .map_err(|e| RiversError::wal_err("Failed to read deleted key", e))?;
        } else {
            self.reader
                .read_exact(&mut len_buffer)
                .map_err(|e| RiversError::wal_err("Failed to read value len", e))?;
            let value_len = usize::from_le_bytes(len_buffer);
            self.reader
                .read_exact(&mut key)
                .map_err(|e| RiversError::wal_err("Failed to read key", e))?;
            let mut value_buf = vec![0; value_len];
            self.reader
                .read_exact(&mut value_buf)
                .map_err(|e| RiversError::wal_err("Failed to read value", e))?;
            value = Some(Value::from(value_buf));
        }
        let key = Key::from(key);

        let mut ts_buffer = [0; 16];
        self.reader
            .read_exact(&mut ts_buffer)
            .map_err(|e| RiversError::wal_err("Failed to read timestamp", e))?;
        let timestamp = Timestamp::from_le_bytes(ts_buffer);

        Ok(Entry {
            key,
            value,
            deleted,
            timestamp,
        })
    }
}

impl Iterator for WALIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_internal() {
            Ok(t) => Some(t),
            Err(RiversError::WalErr { msg, source }) => {
                if source.kind() == std::io::ErrorKind::UnexpectedEof {
                    None
                } else {
                    panic!("Hit WalErr {}, {}", msg, source)
                }
            }
            Err(e) => panic!("Hit error in WALIterator {}", e),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs;

    const TMP_DIR: &'static str = "/tmp/rvr";

    #[test]
    fn test_wal_single() {
        fs::create_dir_all(TMP_DIR).unwrap();
        let path = Path::new(TMP_DIR);
        let mut wal = WAL::new(path).unwrap();

        let entry = Entry::new("key1", "key2");

        wal.put(entry.clone()).unwrap();
        wal.flush().unwrap();

        let mut wal_iter = WALIterator::new(&wal.path).unwrap();

        assert_eq!(entry, wal_iter.next().unwrap());
    }

    #[test]
    fn test_wal_multiple() {
        let path = Path::new(TMP_DIR);
        let mut wal = WAL::new(path).unwrap();

        let mut entries = vec![];
        for i in 0..100 {
            let entry = if i % 3 == 0 {
                Entry::new(format!("key{i}"), format!("key{i}"))
            } else {
                Entry::deleted(format!("key{i}"))
            };
            entries.push(entry.clone());
            wal.put(entry).unwrap();
            if i % 5 == 0 {
                wal.flush().unwrap();
            }
        }

        wal.flush().unwrap();

        let wal_iter = WALIterator::new(&wal.path).unwrap();

        for (wal_entry, entry) in wal_iter.zip(entries) {
            let wal_entry = wal_entry;
            assert_eq!(wal_entry, entry);
        }
    }
}
