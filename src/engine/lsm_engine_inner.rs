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

use bytes::Bytes;

use crate::base::Result;
use crate::base::Version;
use crate::mvcc::MvccInner;

use super::WriteBatchRecord;

pub struct LsmEngineInner {
    pub mvcc: MvccInner,
}

impl LsmEngineInner {
    pub fn mvcc(&self) -> &MvccInner {
        &self.mvcc
    }

    pub fn get_with_version(&self, key: &[u8], version: Version) -> Result<Option<Bytes>> {
        Ok(None)
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<Version> {
        Ok(0)
    }
}
