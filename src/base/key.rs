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

use std::cmp::Reverse;

use bytes::Bytes;

pub struct Key<T: AsRef<[u8]>> {
    key: T,
    version: u64,
}

pub type KeyBytes = Key<Bytes>;
pub type KeySlice<'a> = Key<&'a [u8]>;

impl<T: AsRef<[u8]>> Key<T> {
    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn key_ref(&self) -> &[u8] {
        self.key.as_ref()
    }
}

impl KeyBytes {
    pub fn from_bytes_with_version(key: Bytes, version: u64) -> Self {
        Self { key, version }
    }
}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.key_ref(), self.version()).eq(&(other.key_ref(), other.version()))
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.key_ref(), Reverse(self.version()))
            .partial_cmp(&(other.key_ref(), Reverse(other.version())))
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.key_ref(), Reverse(self.version())).cmp(&(other.key_ref(), Reverse(other.version())))
    }
}
