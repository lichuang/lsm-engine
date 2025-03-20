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

use std::sync::Arc;

use crate::error::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::base::{KeyBytes, KeySlice};

pub struct Memtable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
        }
    }

    pub fn read(&self, key: KeySlice) -> Option<Bytes> {
        let key = KeyBytes::from_bytes_with_version(
            Bytes::from_static(unsafe { std::mem::transmute::<&[u8], &[u8]>(key.key_ref()) }),
            key.version(),
        );

        self.map.get(&key).map(|entry| entry.value().clone())
    }

    pub fn write(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.write_batch(&[(key, value)])
    }

    pub fn write_batch(&self, kvs: &[(KeySlice, &[u8])]) -> Result<()> {
        Ok(())
    }
}
