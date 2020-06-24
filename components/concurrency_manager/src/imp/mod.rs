mod mutex_btree;

use super::errors::{Error, Result};
use super::{ConcurrencyManager, Lock, LockGuard};

use async_mutex::{Mutex as AsyncMutex, MutexGuard};
use async_trait::async_trait;
use std::{
    mem,
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc, Mutex, Weak,
    },
};
use txn_types::TimeStamp;

pub struct ConcurrencyManagerImpl<M: ConcurrentMap> {
    max_read_ts: AtomicU64,
    map: M,
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

impl<'a, M: ConcurrentMap> LockGuard for KeyLockGuard<'a, M> {
    fn update(&self, lock: Lock) {
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

pub trait ConcurrentMap: Send + Sync + 'static {
    fn lower_bound(&self, key: &[u8]) -> Option<(Vec<u8>, KeyLock)>;

    // Returns false if the lock exists
    fn insert_if_not_exists(&self, key: &[u8], key_lock: KeyLock) -> bool;

    fn remove(&self, key: &[u8]);
}

#[async_trait]
impl<'a, M: ConcurrentMap> ConcurrencyManager<'a> for ConcurrencyManagerImpl<M> {
    type LockGuard = KeyLockGuard<'a, M>;

    fn max_read_ts(&'a self) -> TimeStamp {
        self.max_read_ts.load(SeqCst).into()
    }

    fn read_check(&'a self, start_key: &[u8], end_key: &[u8], ts: TimeStamp) -> Result<()> {
        self.max_read_ts.fetch_max(ts.into_inner(), SeqCst);
        if let Some((key, lock)) = self.map.lower_bound(start_key) {
            if key.as_slice() < end_key {
                if let Lock::Prewrite(lock_info) = &*lock.value.lock().unwrap() {
                    return Err(Error::KeyIsLocked(lock_info.clone()));
                }
            }
        }
        Ok(())
    }

    async fn lock_key(&'a self, key: &'a [u8]) -> Self::LockGuard {
        loop {
            if let Some((_, lock)) = self
                .map
                .lower_bound(key)
                .filter(|(key2, _)| key == key2.as_slice())
            {
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
