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

use bytes::{Buf, BufMut, Bytes};

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

#[derive(PartialEq, Eq, Debug)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    // Block encode format:
    // key-value pairs array
    // offset array(u16 per element)
    // number of elements(u16)
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // encode number of elements
        buf.put_u16(self.offsets.len() as u16);

        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        let number_of_entry = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - number_of_entry * SIZEOF_U16;
        let offfsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        let offsets = offfsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let data = data[0..data_end].to_vec();

        Self { data, offsets }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{base::KeyBytes, block::BlockBuilder};

    use super::Block;

    #[test]
    fn test_encode_decode_block() {
        let mut builder = BlockBuilder::new(1024);
        builder.add(
            KeyBytes::new(Bytes::from("hello"), 1).to_key_slice(),
            Bytes::from("world").as_ref(),
        );

        builder.add(
            KeyBytes::new(Bytes::from("test"), 1).to_key_slice(),
            Bytes::from("case").as_ref(),
        );

        let block = builder.finalize();
        let encode_bytes = block.encode();
        let decode_block = Block::decode(encode_bytes.as_ref());
        assert_eq!(block, decode_block);
    }
}
