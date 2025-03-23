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

use bytes::BufMut;

use crate::base::{KeySlice, KeyVec};

use super::block::{Block, SIZEOF_U16};

pub struct BlockBuilder {
    offsets: Vec<u16>,

    data: Vec<u8>,

    block_size: usize,

    first_key: KeyVec,
}

// return the first index that left[i] != rigth[i]
fn compute_overlap_index(left: KeySlice, right: KeySlice) -> usize {
    let mut i = 0;
    let left_len = left.key_len();
    let right_len = right.key_len();
    loop {
        if i >= left_len || i >= right_len {
            break;
        }
        if left.key_ref()[i] != right.key_ref()[i] {
            break;
        }
        i += 1;
    }
    i
}

impl BlockBuilder {
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::default(),
        }
    }

    fn estimated_size(&self) -> usize {
        SIZEOF_U16 + // number of key-value pairs
        self.offsets.len() * SIZEOF_U16 + // offsets
        self.data.len() // datas
    }

    fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    // add a key-value pair into the block, return false is block is full
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key MUST not be empty");

        if !self.is_empty() {
            let estimated_size =
                self.estimated_size() + key.raw_len() + value.len() + SIZEOF_U16 * 3; /* key_len, value_len and offset */
            if estimated_size > self.block_size {
                return false;
            }
        }
        let overlap_index = compute_overlap_index(self.first_key.to_key_slice(), key);

        // save offset of the data
        self.offsets.push(self.data.len() as u16);

        // key encoding format:
        // overlap index(u16) + left key len(u16) + left key content + version(u64)
        self.data.put_u16(overlap_index as u16);
        self.data.put_u16((key.key_len() - overlap_index) as u16);
        self.data.put(&key.key_ref()[overlap_index..]);
        self.data.put_u64(key.version());

        // value encoding format:
        // value len(u16) + value content
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        // save as first key if empty
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    pub fn finalize(self) -> Block {
        assert!(!self.is_empty(), "block MUST not be empty");
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
