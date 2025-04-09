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

use crate::{base::Result, memtable::Memtable};

use super::{LsmEngineInner, LsmOptions};

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

pub struct LsmEngineState {
    // current memtable
    pub memtable: Memtable,
}

pub struct LsmEngine {
    state: LsmEngineState,
    inner: Arc<LsmEngineInner>,
}

impl LsmEngine {
    /*
    pub fn open(options: LsmOptions) -> Result<Self> {
        Ok(Self {})
    }
    */
}
