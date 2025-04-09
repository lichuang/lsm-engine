// Copyright (c) 2025 Lichuang
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

use std::fmt::Debug;

use bytes::{Buf, BufMut, Bytes};

pub type Version = u64;
pub const VERSION_DEFAULT: Version = 0;

pub struct Key<T: AsRef<[u8]>> {
    key: T,
    version: Version,
}

pub type KeyBytes = Key<Bytes>;
pub type KeySlice<'a> = Key<&'a [u8]>;
pub type KeyVec = Key<Vec<u8>>;

impl<T: AsRef<[u8]>> Key<T> {
    pub fn version(&self) -> Version {
        self.version
    }

    pub fn key_ref(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn key_len(&self) -> usize {
        self.key.as_ref().len()
    }

    pub fn raw_len(&self) -> usize {
        self.key.as_ref().len() + std::mem::size_of::<u64>()
    }

    pub fn is_empty(&self) -> bool {
        self.key.as_ref().is_empty()
    }
}

impl KeyBytes {
    pub fn new(key: Bytes, version: Version) -> Self {
        Self { key, version }
    }

    pub fn to_key_slice(&self) -> KeySlice {
        KeySlice {
            key: self.key.as_ref(),
            version: self.version,
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u16(self.key.as_ref().len() as u16);
        buf.put_slice(self.key.as_ref());
        buf.put_u64(self.version);
    }

    pub fn decode(mut buf: &[u8]) -> (Self, &[u8]) {
        let len = buf.get_u16() as usize;
        let key = buf.copy_to_bytes(len);
        let version = buf.get_u64();

        (Self { key, version }, buf)
    }
}

impl<'a> KeySlice<'a> {
    pub fn to_key_bytes(&self) -> KeyBytes {
        KeyBytes::new(self.key.to_vec().into(), self.version)
    }

    pub fn to_key_vec(&self) -> KeyVec {
        KeyVec {
            key: self.key.to_vec(),
            version: self.version,
        }
    }
}

impl KeyVec {
    pub fn new() -> Self {
        Self {
            key: Vec::new(),
            version: VERSION_DEFAULT,
        }
    }

    pub fn to_key_slice(&self) -> KeySlice {
        KeySlice {
            key: self.key.as_ref(),
            version: self.version,
        }
    }

    pub fn from_key_slice(slice: &KeySlice) -> Self {
        Self {
            key: slice.key.to_vec(),
            version: slice.version,
        }
    }

    pub fn to_key_bytes(&self) -> KeyBytes {
        KeyBytes {
            key: self.key.to_vec().into(),
            version: self.version,
        }
    }
}

impl<T: AsRef<[u8]> + Debug> Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.key.fmt(f)
    }
}

impl<T: AsRef<[u8]> + Default> Default for Key<T> {
    fn default() -> Self {
        Self {
            key: T::default(),
            version: VERSION_DEFAULT,
        }
    }
}

impl<T: AsRef<[u8]> + Clone> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            version: self.version,
        }
    }
}

impl<T: AsRef<[u8]> + Copy> Copy for Key<T> {}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.key_ref(), self.version()).eq(&(other.key_ref(), other.version()))
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.key_ref(), self.version()).partial_cmp(&(other.key_ref(), other.version()))
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.key_ref(), self.version()).cmp(&(other.key_ref(), other.version()))
    }
}

#[cfg(test)]
mod tests {
    use super::KeyBytes;
    use bytes::Bytes;

    #[test]
    fn test_compare_key() {
        let key_a = KeyBytes::new(Bytes::from("hello"), 1);
        let key_b = KeyBytes::new(Bytes::from("world"), 1);
        let key_c = KeyBytes::new(Bytes::from("hello"), 2);
        let key_d = KeyBytes::new(Bytes::from("hello"), 2);
        assert!(key_c > key_a);
        assert!(key_b > key_a);
        assert!(key_b > key_a);
        assert_eq!(key_c, key_d);
    }
}
