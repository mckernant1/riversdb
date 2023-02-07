use bytes::Bytes;
use chrono::Utc;

use crate::{
    models::{Entry, Key, Timestamp, Value},
    result::Result,
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

    pub fn write(&mut self, entry: Entry) -> Result<()> {
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
    pub fn new(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(Self { reader })
    }
}

impl Iterator for WALIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_buffer = [0; 8];
        if self.reader.read_exact(&mut len_buffer).is_err() {
            return None;
        }

        let key_len = usize::from_le_bytes(len_buffer);

        let mut bool_buffer = [0; 1];

        if self.reader.read_exact(&mut bool_buffer).is_err() {
            return None;
        }

        let deleted = bool_buffer[0] != 0;
        let mut key = vec![0; key_len];
        let mut value = None;

        if deleted {
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
        } else {
            if self.reader.read_exact(&mut len_buffer).is_err() {
                return None;
            }
            let value_len = usize::from_le_bytes(len_buffer);
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
            let mut value_buf = vec![0; value_len];
            if self.reader.read_exact(&mut value_buf).is_err() {
                return None;
            }
            value = Some(Value::from(value_buf));
        }
        let key = Key::from(key);

        let mut ts_buffer = [0; 16];
        if self.reader.read_exact(&mut ts_buffer).is_err() {
            return None;
        }
        let timestamp = Timestamp::from_le_bytes(ts_buffer);

        Some(Entry {
            key,
            value,
            deleted,
            timestamp,
        })
    }
}
