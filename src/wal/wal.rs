// Copyright (c) 2025 Lichuang(codedump)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::base::Error;
use crate::base::KeySlice;
use crate::base::Result;
use bytes::BufMut;
use parking_lot::Mutex;
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
    path: String,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Wal> {
        let wal_path = path.as_ref().to_string_lossy().into_owned();
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .read(true)
                    .open(path)
                    .map_err(Error::io_error(format!(
                        "fail to open WAL file {:?}",
                        wal_path
                    )))?,
            ))),
            path: wal_path,
        })
    }

    pub fn write(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.write_batch(&[(key, value)])
    }

    pub fn write_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::<u8>::new();
        for (key, value) in data {
            Self::write_record(&mut buf, key, value);
        }
        let path = &self.path;
        file.write_all(&(buf.len() as u32).to_be_bytes())
            .map_err(Error::io_error(format!(
                "failed to write WAL file {} header",
                path
            )))?;
        file.write_all(&buf).map_err(Error::io_error(format!(
            "failed to write WAL file {} content",
            path
        )))?;
        file.write_all(&crc32fast::hash(&buf).to_be_bytes())
            .map_err(Error::io_error(format!(
                "failed to write WAL file {} hash",
                path
            )))?;
        Ok(())
    }

    fn write_record(buf: &mut Vec<u8>, key: &KeySlice, value: &[u8]) {
        buf.put_u16(key.key_len() as u16);
        buf.put_slice(key.key_ref());
        buf.put_u64(key.version());
        buf.put_u16(value.len() as u16);
        buf.put_slice(value);
    }
}
