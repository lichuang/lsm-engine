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

use anyhow::Result;
use bytes::BufMut;
use tinysearch_cuckoofilter::CuckooFilter;

use super::BlockMetaVec;
use super::SsTableId;
use super::SsTableMeta;
use crate::base::KeySlice;
use crate::base::KeyVec;
use crate::base::VERSION_DEFAULT;
use crate::base::Version;
use crate::block::BlockBuilder;
use crate::table::BlockMeta;
use crate::table::SsTable;

pub struct SsTableBuilder {
    block_builder: BlockBuilder,
    filter: CuckooFilter<farmhash::FarmHasher>,

    first_key: KeyVec,
    last_key: KeyVec,

    data: Vec<u8>,

    block_meta_vec: BlockMetaVec,

    max_version: Version,

    block_size: usize,
}

impl SsTableBuilder {
    pub fn create(block_size: usize) -> Result<Self> {
        Ok(SsTableBuilder {
            block_builder: BlockBuilder::new(block_size),
            filter: CuckooFilter::<farmhash::FarmHasher>::with_capacity(10240),

            first_key: KeyVec::new(),
            last_key: KeyVec::new(),

            data: Vec::new(),
            block_meta_vec: BlockMetaVec::new(),

            max_version: VERSION_DEFAULT,
            block_size,
        })
    }

    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> Result<()> {
        if self.first_key.is_empty() {
            self.first_key = KeyVec::from_key_slice(&key);
        }
        self.max_version = std::cmp::max(self.max_version, key.version());

        self.filter.add(&farmhash::fingerprint32(key.key_ref()))?;

        // if the block is not full, `add` return true
        if self.block_builder.add(key, value) {
            self.last_key = KeyVec::from_key_slice(&key);
            return Ok(());
        }

        // else, the block is full
        // first finalize the block
        self.finalize();

        // then add data to the next block
        assert!(self.block_builder.add(key, value));
        self.first_key = KeyVec::from_key_slice(&key);
        self.last_key = KeyVec::from_key_slice(&key);

        Ok(())
    }

    // save [encoded block + block checksum(u32)] into data buffer
    fn finalize(&mut self) {
        let block_builder =
            std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
        let encoded_block = block_builder.finalize().encode();
        // save block meta
        self.block_meta_vec.push(BlockMeta {
            offset: self.data.len(),
            first_key: self.first_key.to_key_bytes(),
            last_key: self.last_key.to_key_bytes(),
        });
        let checksum = crc32fast::hash(&encoded_block);
        self.data.extend(encoded_block);
        self.data.put_u32(checksum);
    }

    pub fn build(mut self, id: SsTableId, path: impl AsRef<Path>) -> Result<SsTable> {
        self.finalize();
        let mut data = self.data;

        // save block meta vectors
        let block_meta_offset = data.len();
        self.block_meta_vec.encode(self.max_version, &mut data);
        data.put_u32(block_meta_offset as u32);

        // save filter data
        let export_filter = self.filter.export();
        let filter_data = bincode::serialize(&export_filter)?;
        data.extend(&filter_data);
        let filter_offset = data.len();
        data.put_u32(filter_offset as u32);

        // create sstable meta
        let table_meta = SsTableMeta {
            id,
            first_key: self.first_key.clone(),
            last_key: self.last_key.clone(),
            block_meta_vec: self.block_meta_vec.clone(),
            block_meta_offset,
            max_version: self.max_version,
        };

        SsTable::create(table_meta, path.as_ref(), data)
    }
}

#[cfg(test)]
mod tests {}
