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

use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicUsize};

use crate::base::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::base::{KeyBytes, KeySlice};

pub struct Memtable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,

    id: usize,

    // since `SkipMap` has no function such as `size()` to
    // return the total size of the container, we need to accumulate estimate size when writing data
    approximate_size: Arc<AtomicUsize>,
}

impl Memtable {
    pub fn new(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn read(&self, key: KeySlice) -> Option<Bytes> {
        let key = KeyBytes::new(
            Bytes::from_static(unsafe { std::mem::transmute::<&[u8], &[u8]>(key.key_ref()) }),
            key.version(),
        );

        self.map.get(&key).map(|entry| entry.value().clone())
    }

    pub fn write(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.write_batch(&[(key, value)])
    }

    pub fn write_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut est_size = 0;
        for (k, v) in data {
            est_size += k.raw_len() + v.len();
            self.map.insert(k.to_key_bytes(), Bytes::copy_from_slice(v));
        }
        self.approximate_size.fetch_add(est_size, Ordering::Relaxed);
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::base::KeyBytes;

    use super::Memtable;

    #[test]
    fn test_write_and_read() {
        let table = Memtable::new(1);
        let key = KeyBytes::new(Bytes::from("hello"), 1);
        let value = Bytes::from("world");
        assert!(table.write(key.to_key_slice(), value.as_ref()).is_ok());
        let ret = table.read(key.to_key_slice());
        assert!(ret.is_some_and(|val| {
            assert_eq!(val, value);
            true
        }));

        assert_eq!(table.size(), key.raw_len() + value.len());
    }
}
