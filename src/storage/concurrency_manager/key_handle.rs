// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod key_mutex;
mod lock_store;

use self::{key_mutex::KeyMutex, lock_store::LockStore};
use super::handle_table::OrderedMap;

use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use txn_types::{Key, Lock};

const INIT_REF_COUNT: usize = usize::MAX;

/// An entry in the in-memory table providing functions related to a specific
/// key.
///
/// You should always use it with `KeyHandleRef` so useless `KeyHandle`s can
/// be removed from the table automatically.
pub struct KeyHandle {
    key: Key,
    ref_count: AtomicUsize,
    key_mutex: KeyMutex,
    lock_store: LockStore,
}

impl KeyHandle {
    pub fn new(key: Key) -> Self {
        KeyHandle {
            key,
            ref_count: AtomicUsize::new(INIT_REF_COUNT),
            key_mutex: KeyMutex::new(),
            lock_store: LockStore::new(),
        }
    }

    pub fn get_ref<'m, M: OrderedMap>(self: Arc<Self>, map: &'m M) -> Option<KeyHandleRef<'m, M>> {
        let mut ref_count = self.ref_count.load(Ordering::SeqCst);
        loop {
            // It is possible that the reference count has just decreased to zero and not
            // been removed from the map. In this case, we should not create a new reference
            // because the handle will be removed from the map immediately.
            if ref_count == 0 {
                return None;
            }
            let new_value = if ref_count == INIT_REF_COUNT {
                1
            } else {
                ref_count + 1
            };
            match self.ref_count.compare_exchange(
                ref_count,
                new_value,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Some(KeyHandleRef { handle: self, map });
                }
                Err(n) => ref_count = n,
            }
        }
    }
}

pub struct KeyHandleRef<'m, M: OrderedMap> {
    handle: Arc<KeyHandle>,
    map: &'m M,
}

impl<'m, M: OrderedMap> KeyHandleRef<'m, M> {
    pub fn key(&self) -> &Key {
        &self.key
    }

    pub async fn mutex_lock(self) -> KeyHandleMutexGuard<'m, M> {
        self.key_mutex.mutex_lock().await;
        KeyHandleMutexGuard(self)
    }

    pub fn with_lock<T>(&self, f: impl FnOnce(&Option<Lock>) -> T) -> T {
        self.lock_store.read(f)
    }
}

impl<'m, M: OrderedMap> Deref for KeyHandleRef<'m, M> {
    type Target = Arc<KeyHandle>;

    fn deref(&self) -> &Arc<KeyHandle> {
        &self.handle
    }
}

impl<'m, M: OrderedMap> Drop for KeyHandleRef<'m, M> {
    fn drop(&mut self) {
        if self.handle.ref_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.map.remove(&self.key);
        }
    }
}

/// A `KeyHandleRef` with its mutex locked.
pub struct KeyHandleMutexGuard<'m, M: OrderedMap>(KeyHandleRef<'m, M>);

impl<'m, M: OrderedMap> KeyHandleMutexGuard<'m, M> {
    pub fn key(&self) -> &Key {
        &self.0.key()
    }

    pub fn with_lock<T>(&self, f: impl FnOnce(&mut Option<Lock>) -> T) -> T {
        self.0.lock_store.write(f, &self.0.ref_count)
    }
}

impl<'m, M: OrderedMap> Drop for KeyHandleMutexGuard<'m, M> {
    fn drop(&mut self) {
        self.0.key_mutex.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::{collections::BTreeMap, time::Duration};
    use tokio::time::delay_for;
    use txn_types::LockType;

    #[tokio::test]
    async fn test_key_mutex() {
        let map = Arc::new(Mutex::new(BTreeMap::new()));
        let handle = Arc::new(KeyHandle::new(Key::from_raw(b"k")));
        map.insert_if_not_exist(Key::from_raw(b"k"), handle.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let map = map.clone();
            let handle = handle.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let lock_ref = handle.get_ref(&*map).unwrap();
                let _guard = lock_ref.mutex_lock().await;
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

    #[tokio::test]
    async fn test_ref_count() {
        let map = Mutex::new(BTreeMap::new());

        let k = Key::from_raw(b"k");

        // simple case
        map.insert_if_not_exist(k.clone(), Arc::new(KeyHandle::new(k.clone())));
        let lock_ref1 = map.get(&k).unwrap();
        let lock_ref2 = map.get(&k).unwrap();
        drop(lock_ref1);
        assert!(map.get(&k).is_some());
        drop(lock_ref2);
        assert!(map.get(&k).is_none());

        // should not removed it from the table if a lock is stored in it
        map.insert_if_not_exist(k.clone(), Arc::new(KeyHandle::new(k.clone())));
        let guard = map.get(&k).unwrap().mutex_lock().await;
        guard.with_lock(|lock| {
            *lock = Some(Lock::new(
                LockType::Lock,
                b"k".to_vec(),
                1.into(),
                100,
                None,
                1.into(),
                1,
                1.into(),
            ))
        });
        drop(guard);
        assert!(map.get(&k).is_some());

        // remove the lock stored in, then the handle should be removed from the table
        let guard = map.get(&k).unwrap().mutex_lock().await;
        guard.with_lock(|lock| *lock = None);
        drop(guard);
        assert!(map.get(&k).is_some());
    }
}