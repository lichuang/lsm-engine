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

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use super::mvcc_inner::CommittedTxn;
use crate::base::Version;
use crate::engine::LsmEngineInner;
use crate::engine::WriteBatchRecord;

const EMPTY_VALUE: Bytes = Bytes::new();

pub struct Transaction {
    pub read_version: Version,
    pub inner: Arc<LsmEngineInner>,
    pub storage: Arc<SkipMap<Bytes, Bytes>>,
    pub committed: Arc<AtomicBool>,
    // write set and read set
    pub key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn read(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            return Err(Error::txn_error("cannot operate on committed txn"));
        }
        if let Some(guard) = &self.key_hashes {
            let mut guard = guard.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }

        if let Some(entry) = self.storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }

        self.inner.get_with_version(key, self.read_version)
    }

    pub fn write(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.committed.load(Ordering::SeqCst) {
            return Err(Error::txn_error("cannot operate on committed txn"));
        }
        self.storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        if let Some(guard) = &self.key_hashes {
            let mut guard = guard.lock();
            let (write_set, _) = &mut *guard;
            write_set.insert(farmhash::hash32(key));
        }

        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write(key, &EMPTY_VALUE)
    }

    pub fn commit(&self) -> Result<()> {
        // mark `committed` automatically
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("cannot operate on committed txn");

        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        if let Some(guard) = &self.key_hashes {
            let guard = guard.lock();
            let (write_set, read_set) = &*guard;
            // do serializability check with txn after this transctions
            if !write_set.is_empty() {
                let committed_txns = self.inner.mvcc().committed_txns.lock();
                for (_, txn) in committed_txns.range((self.read_version + 1)..) {
                    for key_hash in read_set {
                        if txn.key_hashes.contains(key_hash) {
                            return Err(Error::txn_error("serializable check failed"));
                        }
                    }
                }
            }
        }
        // commit all the modifications
        let batch = self
            .storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<WriteBatchRecord<Bytes>>>();
        let commit_version = self.inner.write_batch(&batch)?;

        if self.key_hashes.is_none() {
            return Ok(());
        }

        // save write set of committed version
        let mut committed_txns = self.inner.mvcc().committed_txns.lock();
        // safe to unwrap
        let mut key_hashes = self.key_hashes.as_ref().unwrap().lock();
        let (write_set, _) = &mut *key_hashes;
        let old_data = committed_txns.insert(commit_version, CommittedTxn {
            key_hashes: std::mem::take(write_set),
            read_version: self.read_version,
            commit_version,
        });
        // assert there is no data of this version committed before
        assert!(old_data.is_none());

        // remove outage data
        let watermark = self.inner.mvcc().watermark();
        while let Some(entry) = committed_txns.first_entry() {
            if watermark >= *entry.key() {
                break;
            }

            entry.remove();
        }

        Ok(())
    }
}
