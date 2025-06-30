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
use std::path::Path;

use anyhow::Result;

use super::BlockMetaVec;
use crate::base::KeyVec;
use crate::base::Version;

pub type SsTableId = u64;

pub struct SsTableMeta {
    pub id: SsTableId,

    pub first_key: KeyVec,
    pub last_key: KeyVec,

    pub block_meta_vec: BlockMetaVec,
    pub block_meta_offset: usize,

    pub max_version: Version,
}

struct SsTableFile {}

impl SsTableFile {
    // pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
    // std::fs::write(path, &data)
    // .map_err(Error::io_error(format!("write to SsTable file {:?}", path)))?;
    // let file = File::options()
    // .read(true)
    // .write(false)
    // .open(path)
    // .map_err(Error::io_error(format!("open SsTable file {:?}", path)))?;
    // Ok(SsTableFile {
    // file: Some(file),
    // size: data.len(),
    // })
    // }
}

pub struct SsTable {
    pub meta: SsTableMeta,
    file: SsTableFile,
}

impl SsTable {
    // pub fn create(meta: SsTableMeta, path: &Path, data: Vec<u8>) -> Result<Self> {
    // Ok(Self {
    // meta,
    // file: SsTableFile::create(path, data)?,
    // })
    // }
}
