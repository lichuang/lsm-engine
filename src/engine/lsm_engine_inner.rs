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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::RwLock;

use super::LsmEngineState;
use super::LsmOptions;
use super::WriteBatchRecord;
use crate::base::Version;
use crate::mvcc::MvccInner;

pub struct LsmEngineInner {
    pub state: Arc<RwLock<Arc<LsmEngineState>>>,
    pub mvcc: MvccInner,
}

impl LsmEngineInner {
    // pub fn open(path: impl AsRef<Path>, options: LsmOptions) -> Result<Self> {
    // let state = LsmEngineState::create();
    //
    // Ok()
    // }

    pub fn mvcc(&self) -> &MvccInner {
        &self.mvcc
    }

    pub fn get_with_version(&self, key: &[u8], version: Version) -> Result<Option<Bytes>> {
        Ok(None)
    }

    // write batch records, return the committed version
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<Version> {
        Ok(0)
    }
}
