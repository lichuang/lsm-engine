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

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Buf;
use bytes::BufMut;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;

use super::LsmEngineState;
use crate::compact::CompactionTask;

const MANIFEST: &str = "MANIFEST";

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn open(path: impl AsRef<Path>, state: &LsmEngineState) -> Result<Self> {
        let path = path.as_ref();
        let manifest_path = path.join(MANIFEST);

        if !manifest_path.exists() {
            let manifest = Manifest::create(path)?;
            manifest.add_record(ManifestRecord::NewMemtable(state.memtable.id()));
            Ok(manifest)
        } else {
            let (manifest, records) = Manifest::recover(path)?;
            Ok(manifest)
        }
    }

    fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)?,
            )),
        })
    }

    fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();
        let mut records = Vec::new();
        while buf_ptr.has_remaining() {
            let (record, buf) = ManifestRecord::decode(buf_ptr)?;
            records.push(record);
            buf_ptr = buf;
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let (buf_size, buf) = record.encode()?;

        file.write_all(&(buf_size as u64).to_be_bytes())?;
        file.write_all(&buf)?;
        file.sync_all();
        Ok(())
    }
}

impl ManifestRecord {
    // manifest record format:
    // buf len[u64] + json(record) + crc32 of json(record)
    pub(crate) fn encode(&self) -> Result<(usize, Vec<u8>)> {
        let mut buf = serde_json::to_vec(self)?;
        let hash = crc32fast::hash(&buf);
        let buf_size = buf.len();
        buf.put_u32(hash);

        Ok((buf_size, buf))
    }

    pub(crate) fn decode(mut buf: &[u8]) -> Result<(Self, &[u8])> {
        let buf_size = buf.get_u64();
        let slice = &buf[..buf_size as usize];
        let json = serde_json::from_slice::<ManifestRecord>(slice)?;
        buf.advance(buf_size as usize);
        let checksum = buf.get_u32();
        if checksum != crc32fast::hash(slice) {}

        Ok((json, buf))
    }
}
