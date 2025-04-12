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

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use parking_lot::Mutex;

use crate::base::Version;

use super::Watermark;

pub struct CommittedTxn {
    pub key_hashes: HashSet<u32>,
    pub read_version: Version,
    pub commit_version: Version,
}

pub struct MvccInner {
    pub write_lock: Mutex<()>,
    pub commit_lock: Mutex<()>,
    pub version: Arc<Mutex<(Version, Watermark)>>,
    pub committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxn>>>,
}

impl MvccInner {
    pub fn new(init_version: Version) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            version: Arc::new(Mutex::new((init_version, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// All version(strictly) below this version can be garbage collected.
    pub fn watermark(&self) -> Version {
        let version = self.version.lock();
        version.1.watermark().unwrap_or(version.0)
    }
}
