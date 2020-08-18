// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::key_handle::{KeyHandle, KeyHandleGuard};

use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{Arc, Weak},
};
use txn_types::{Key, Lock};

#[derive(Clone)]
pub struct LockTable(pub Arc<SkipMap<Key, Weak<KeyHandle>>>);

impl Default for LockTable {
    fn default() -> Self {
        LockTable(Arc::new(SkipMap::new()))
    }
}

impl LockTable {
    pub async fn lock_key(&self, key: &Key) -> KeyHandleGuard {
        loop {
            let handle = Arc::new(KeyHandle::new(key.clone(), self.clone()));
            let weak = Arc::downgrade(&handle);
            let weak2 = weak.clone();
            let guard = handle.lock().await;
            let entry = self.0.get_or_insert(key.clone(), weak);
            if entry.value().ptr_eq(&weak2) {
                return guard;
            } else {
                if let Some(handle) = entry.value().upgrade() {
                    return handle.lock().await;
                }
            }
        }
    }

    pub fn check_key<E>(
        &self,
        key: &Key,
        check_fn: impl FnOnce(&Lock) -> Result<(), E>,
    ) -> Result<(), E> {
        if let Some(lock_ref) = self.get(key) {
            return lock_ref.with_lock(|lock| {
                if let Some(lock) = &*lock {
                    return check_fn(lock);
                }
                Ok(())
            });
        }
        Ok(())
    }

    pub fn check_range<E>(
        &self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        mut check_fn: impl FnMut(&Key, &Lock) -> Result<(), E>,
    ) -> Result<(), E> {
        let e = self.find_first(start_key, end_key, |handle| {
            handle.with_lock(|lock| {
                lock.as_ref()
                    .and_then(|lock| check_fn(&handle.key, lock).err())
            })
        });
        if let Some(e) = e {
            Err(e)
        } else {
            Ok(())
        }
    }

    /// Gets the handle of the key.
    pub fn get<'m>(&'m self, key: &Key) -> Option<Arc<KeyHandle>> {
        self.0.get(key).and_then(|e| e.value().upgrade())
    }

    /// Finds the first handle in the given range that `pred` returns `Some`.
    /// The `Some` return value of `pred` will be returned by `find_first`.
    pub fn find_first<'m, T>(
        &'m self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        mut pred: impl FnMut(Arc<KeyHandle>) -> Option<T>,
    ) -> Option<T> {
        let lower_bound = start_key
            .map(|k| Bound::Included(k))
            .unwrap_or(Bound::Unbounded);
        let upper_bound = end_key
            .map(|k| Bound::Excluded(k))
            .unwrap_or(Bound::Unbounded);
        for e in self.0.range((lower_bound, upper_bound)) {
            let res = e.value().upgrade().and_then(&mut pred);
            if res.is_some() {
                return res;
            }
        }
        None
    }

    /// Removes the key and its key handle from the map.
    pub fn remove(&self, key: &Key) {
        self.0.remove(key);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::time::delay_for;
    use txn_types::LockType;

    #[tokio::test]
    async fn test_lock_key() {
        let lock_table = LockTable::default();

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let lock_table = lock_table.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let _guard = lock_table.lock_key(&Key::from_raw(b"k")).await;
                // Modify an atomic counter with a mutex guard. The value of the counter
                // should remain unchanged if the mutex works.
                let counter_val = counter.fetch_add(1, Ordering::SeqCst) + 1;
                delay_for(Duration::from_millis(1)).await;
                assert_eq!(counter.load(Ordering::SeqCst), counter_val);
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }

    fn ts_check(lock: &Lock, ts: u64) -> Result<(), Lock> {
        if lock.ts.into_inner() < ts {
            Err(lock.clone())
        } else {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_check_key() {
        let lock_table = LockTable::default();
        let key_k = Key::from_raw(b"k");

        // no lock found
        assert!(lock_table.check_key(&key_k, |_| Err(())).is_ok());

        let lock = Lock::new(
            LockType::Lock,
            b"k".to_vec(),
            10.into(),
            100,
            None,
            10.into(),
            1,
            10.into(),
        );
        let guard = lock_table.lock_key(&key_k).await;
        guard.with_lock(|l| {
            *l = Some(lock.clone());
        });

        // lock passes check_fn
        assert!(lock_table.check_key(&key_k, |l| ts_check(l, 5)).is_ok());

        // lock does not pass check_fn
        assert_eq!(lock_table.check_key(&key_k, |l| ts_check(l, 20)), Err(lock));
    }

    #[tokio::test]
    async fn test_check_range() {
        let lock_table = LockTable::default();

        let lock_k = Lock::new(
            LockType::Lock,
            b"k".to_vec(),
            20.into(),
            100,
            None,
            20.into(),
            1,
            20.into(),
        );
        let guard = lock_table.lock_key(&Key::from_raw(b"k")).await;
        guard.with_lock(|l| {
            *l = Some(lock_k.clone());
        });

        let lock_l = Lock::new(
            LockType::Lock,
            b"l".to_vec(),
            10.into(),
            100,
            None,
            10.into(),
            1,
            10.into(),
        );
        let guard = lock_table.lock_key(&Key::from_raw(b"l")).await;
        guard.with_lock(|l| {
            *l = Some(lock_l.clone());
        });

        // no lock found
        assert!(lock_table
            .check_range(
                Some(&Key::from_raw(b"m")),
                Some(&Key::from_raw(b"n")),
                |_, _| Err(())
            )
            .is_ok());

        // lock passes check_fn
        assert!(lock_table
            .check_range(None, Some(&Key::from_raw(b"z")), |_, l| ts_check(l, 5))
            .is_ok());

        // first lock does not pass check_fn
        assert_eq!(
            lock_table.check_range(Some(&Key::from_raw(b"a")), None, |_, l| ts_check(l, 25)),
            Err(lock_k)
        );

        // first lock passes check_fn but the second does not
        assert_eq!(
            lock_table.check_range(None, None, |_, l| ts_check(l, 15)),
            Err(lock_l)
        );
    }
}
