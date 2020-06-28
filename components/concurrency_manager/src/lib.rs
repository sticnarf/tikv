// TODO: upgrade rust toolchain and remove it
#![feature(atomic_min_max)]

pub mod errors;
mod mutex_btree;

use async_mutex::{Mutex as AsyncMutex, MutexGuard};
use errors::{Error, Result};
use kvproto::kvrpcpb::LockInfo;
use std::{
    collections::BTreeMap,
    mem,
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc, Mutex, Weak,
    },
};
use txn_types::TimeStamp;

pub struct ConcurrencyManager<M: ConcurrentMap> {
    max_read_ts: AtomicU64,
    map: M,
}

impl<M: ConcurrentMap> ConcurrencyManager<M> {
    pub fn new(latest_ts: TimeStamp) -> Self {
        Self {
            max_read_ts: AtomicU64::new(latest_ts.into_inner()),
            map: Default::default(),
        }
    }

    pub fn max_read_ts(&self) -> TimeStamp {
        self.max_read_ts.load(SeqCst).into()
    }

    pub fn read_key_check(
        &self,
        key: &[u8],
        ts: TimeStamp,
        mut check_fn: impl FnMut(&LockInfo, TimeStamp) -> bool,
    ) -> Result<()> {
        self.max_read_ts.fetch_max(ts.into_inner(), SeqCst);
        if let Some(lock) = self.map.get(key) {
            if let Lock::Prewrite(lock_info) = &*lock.value.lock().unwrap() {
                if !check_fn(lock_info, ts) {
                    return Err(Error::KeyIsLocked(lock_info.clone()));
                }
            }
        }
        Ok(())
    }

    pub fn read_range_check(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        ts: TimeStamp,
        mut check_fn: impl FnMut(&LockInfo, TimeStamp) -> bool,
    ) -> Result<()> {
        self.max_read_ts.fetch_max(ts.into_inner(), SeqCst);
        if let Some((key, lock)) = self.map.lower_bound(start_key) {
            if key.as_slice() < end_key {
                if let Lock::Prewrite(lock_info) = &*lock.value.lock().unwrap() {
                    if !check_fn(lock_info, ts) {
                        return Err(Error::KeyIsLocked(lock_info.clone()));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn lock_key<'a>(&'a self, key: &'a [u8]) -> KeyLockGuard<'a, M> {
        loop {
            if let Some(lock) = self.map.get(key) {
                match lock.mutex.upgrade() {
                    None => continue,
                    Some(mutex) => unsafe {
                        let static_mutex_ref: &'static AsyncMutex<_> = mem::transmute(&*mutex);
                        break KeyLockGuard {
                            map: &self.map,
                            key,
                            _mutex_guard: static_mutex_ref.lock().await,
                            mutex: Some(mutex.clone()),
                            value: lock.value.clone(),
                        };
                    },
                }
            } else {
                let mutex = Arc::new(AsyncMutex::new(()));
                let static_mutex_ref: &'static AsyncMutex<_> = unsafe { mem::transmute(&*mutex) };
                let mutex_guard = static_mutex_ref.lock().await;
                let value = Arc::new(Mutex::new(Lock::Empty));
                let key_lock = KeyLock {
                    mutex: Arc::downgrade(&mutex),
                    value: value.clone(),
                };
                let inserted = self.map.insert_if_not_exists(key, key_lock);
                if inserted {
                    break KeyLockGuard {
                        map: &self.map,
                        key,
                        _mutex_guard: mutex_guard,
                        mutex: Some(mutex),
                        value,
                    };
                }
            }
        }
    }
}

pub enum Lock {
    Empty,
    Pessimistic(LockInfo),
    Prewrite(LockInfo),
}

#[derive(Clone)]
pub struct KeyLock {
    mutex: Weak<AsyncMutex<()>>,
    value: Arc<Mutex<Lock>>,
}

pub struct KeyLockGuard<'a, M: ConcurrentMap> {
    map: &'a M,
    key: &'a [u8],
    _mutex_guard: MutexGuard<'a, ()>,
    mutex: Option<Arc<AsyncMutex<()>>>,
    value: Arc<Mutex<Lock>>,
}

impl<'a, M: ConcurrentMap> KeyLockGuard<'a, M> {
    pub fn update(&self, lock: Lock) {
        *self.value.lock().unwrap() = lock;
    }
}

impl<'a, M: ConcurrentMap> Drop for KeyLockGuard<'a, M> {
    fn drop(&mut self) {
        let mutex = self.mutex.take().unwrap();
        let weak = Arc::downgrade(&mutex);
        drop(mutex);
        if weak.strong_count() == 0 {
            self.map.remove(self.key);
        }
    }
}

pub trait ConcurrentMap: Default + Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> Option<KeyLock>;

    fn lower_bound(&self, key: &[u8]) -> Option<(Vec<u8>, KeyLock)>;

    // Returns false if the lock exists
    fn insert_if_not_exists(&self, key: &[u8], key_lock: KeyLock) -> bool;

    fn remove(&self, key: &[u8]);
}

pub type MutexBTreeConcurrencyManager = ConcurrencyManager<Mutex<BTreeMap<Vec<u8>, KeyLock>>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::{sync::mpsc, time::delay_for};

    #[tokio::test]
    async fn test_exclusive_lock() {
        let manager = Arc::new(MutexBTreeConcurrencyManager::new(1.into()));
        let counter = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for _ in 0..100 {
            let manager = manager.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let _guard = manager.lock_key(b"a").await;
                let init_value = counter.fetch_add(1, SeqCst) + 1;
                delay_for(Duration::from_millis(1)).await;
                // if the guard fail to block other threads, counter would change
                assert_eq!(counter.load(SeqCst), init_value);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(counter.load(SeqCst), 100);
    }

    #[test]
    fn test_max_read_ts() {
        let manager = MutexBTreeConcurrencyManager::new(1.into());
        assert_eq!(manager.max_read_ts(), 1.into());

        // read_check updates the max_read_ts
        manager
            .read_key_check(b"a", 10.into(), |_, _| true)
            .unwrap();
        assert_eq!(manager.max_read_ts(), 10.into());

        // max_read_ts does not decrease
        manager.read_key_check(b"a", 5.into(), |_, _| true).unwrap();
        assert_eq!(manager.max_read_ts(), 10.into());

        // same for read_range_check
        manager
            .read_range_check(b"a", b"b", 5.into(), |_, _| true)
            .unwrap();
        assert_eq!(manager.max_read_ts(), 10.into());
        manager
            .read_range_check(b"a", b"b", 15.into(), |_, _| true)
            .unwrap();
        assert_eq!(manager.max_read_ts(), 15.into());
    }

    #[tokio::test]
    async fn test_read_check_memory_lock() {
        let manager = Arc::new(MutexBTreeConcurrencyManager::new(1.into()));

        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();

        let manager2 = manager.clone();
        let handle = tokio::spawn(async move {
            rx1.recv().await;
            let guard = manager2.lock_key(b"b").await;

            tx2.send(()).unwrap();
            rx1.recv().await;

            let mut lock = LockInfo::default();
            lock.set_key(b"b".to_vec());
            lock.set_lock_version(20);
            guard.update(Lock::Prewrite(lock));

            tx2.send(()).unwrap();
            rx1.recv().await;
        });

        let check_fn =
            |lock_info: &LockInfo, ts: TimeStamp| ts.into_inner() <= lock_info.get_lock_version();

        // key is not locked
        assert!(manager.read_key_check(b"b", 2.into(), check_fn).is_ok());
        assert!(manager
            .read_range_check(b"a", b"c", 2.into(), check_fn)
            .is_ok());

        // wait for the lock being locked
        tx1.send(()).unwrap();
        rx2.recv().await;

        // no prewritten lock
        assert!(manager.read_key_check(b"b", 2.into(), check_fn).is_ok());
        assert!(manager
            .read_range_check(b"a", b"c", 2.into(), check_fn)
            .is_ok());

        // wait for prewrite
        tx1.send(()).unwrap();
        rx2.recv().await;

        // blocked by prewritten lock
        assert!(manager.read_key_check(b"b", 30.into(), check_fn).is_err());
        assert!(manager
            .read_range_check(b"a", b"c", 30.into(), check_fn)
            .is_err());

        // test check_fn
        assert!(manager.read_key_check(b"b", 10.into(), check_fn).is_ok());
        assert!(manager
            .read_range_check(b"a", b"c", 10.into(), check_fn)
            .is_ok());

        tx1.send(()).unwrap();
        handle.await.unwrap();

        // lock is released
        assert!(manager.read_key_check(b"b", 30.into(), check_fn).is_ok());
        assert!(manager
            .read_range_check(b"a", b"c", 30.into(), check_fn)
            .is_ok());
    }
}
