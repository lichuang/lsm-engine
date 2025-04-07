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
use crate::base::Result;
use crate::base::{KeyBytes, Version};
use bytes::{Buf, BufMut};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlockMeta {
    // Offset of this block in Sstable
    pub offset: usize,

    // The first key of block
    pub first_key: KeyBytes,

    // The last key of block
    pub last_key: KeyBytes,
}

impl BlockMeta {
    pub fn estimated_size(&self) -> usize {
        let mut estimated_size = 0;
        // The size of offset
        estimated_size += std::mem::size_of::<u32>();
        // The size of first and last key length
        estimated_size += std::mem::size_of::<u16>() * 2;
        // The size of first key
        estimated_size += self.first_key.raw_len();
        // The size of last key
        estimated_size += self.last_key.raw_len();

        estimated_size
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u32(self.offset as u32);
        self.first_key.encode(buf);
        self.last_key.encode(buf);
    }

    pub fn decode(mut buf: &[u8]) -> (Self, &[u8]) {
        let offset = buf.get_u32() as usize;
        let (first_key, buf) = KeyBytes::decode(buf);
        let (last_key, buf) = KeyBytes::decode(buf);

        (
            Self {
                offset,
                first_key,
                last_key,
            },
            buf,
        )
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlockMetaVec(Vec<BlockMeta>);

impl BlockMetaVec {
    pub fn new() -> Self {
        BlockMetaVec(Vec::new())
    }

    pub fn with(metas: Vec<BlockMeta>) -> Self {
        BlockMetaVec(metas)
    }

    pub fn push(&mut self, meta: BlockMeta) {
        self.0.push(meta);
    }

    pub fn encode(&self, version: Version, buf: &mut Vec<u8>) {
        // number of blocks
        let mut estimated_size = std::mem::size_of::<u32>();
        for meta in &self.0 {
            estimated_size += meta.estimated_size();
        }
        // version
        estimated_size += std::mem::size_of::<u64>();
        // checksum
        estimated_size += std::mem::size_of::<u32>();

        buf.reserve(estimated_size);
        let original_len = buf.len();
        buf.put_u32(self.0.len() as u32);
        for meta in &self.0 {
            meta.encode(buf);
        }
        buf.put_u64(version);
        buf.put_u32(crc32fast::hash(&buf[original_len + 4..]));
        assert_eq!(estimated_size, buf.len() - original_len);
    }

    pub fn decode(mut buf: &[u8]) -> Result<(Version, BlockMetaVec)> {
        let mut meta_vec = Vec::new();
        // number of blocks
        let num = buf.get_u32() as usize;
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        for _ in 0..num {
            let (meta, ret_buf) = BlockMeta::decode(buf);
            buf = ret_buf;
            meta_vec.push(meta);
        }
        let version = buf.get_u64();
        if buf.get_u32() != checksum {
            Err(Error::block_meta_error("BlockMeta checksum mismatched"))
        } else {
            Ok((version, BlockMetaVec(meta_vec)))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::base::Result;
    use bytes::{Buf, Bytes};

    use crate::{base::KeyBytes, table::BlockMetaVec};

    use super::BlockMeta;

    #[test]
    fn test_encode_decode_block_meta() {
        let first_key = KeyBytes::new(Bytes::from("hello"), 1);
        let last_key = KeyBytes::new(Bytes::from("world"), 12);
        let offset = 100;

        let meta = BlockMeta {
            first_key,
            last_key,
            offset,
        };

        let estimated_size = meta.estimated_size();
        let mut buf = Vec::with_capacity(estimated_size);
        meta.encode(&mut buf);

        let (decode_meta, buf) = BlockMeta::decode(&buf);

        assert_eq!(meta, decode_meta);
        assert_eq!(buf.remaining(), 0);
    }

    #[test]
    fn test_encode_decode_block_meta_vector() -> Result<()> {
        let mut meta_vec = Vec::new();
        let key_tuples = vec![("first", "last"), ("hello", "world")];
        for (first_key, last_key) in key_tuples {
            let first_key = KeyBytes::new(Bytes::from(first_key), 1);
            let last_key = KeyBytes::new(Bytes::from(last_key), 12);
            let offset = 100;

            let meta = BlockMeta {
                first_key,
                last_key,
                offset,
            };
            meta_vec.push(meta);
        }
        let block_meta_vec = BlockMetaVec::with(meta_vec);
        let mut buf = Vec::new();
        let version = 101;
        block_meta_vec.encode(version, &mut buf);

        let (decode_version, decode_meta_vec) = BlockMetaVec::decode(&buf)?;

        assert_eq!(version, decode_version);
        assert_eq!(block_meta_vec, decode_meta_vec);

        Ok(())
    }
}
