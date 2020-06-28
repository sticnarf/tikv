use super::{ConcurrentMap, KeyLock};

use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
    sync::Mutex,
};

struct StartRange<'a>(&'a [u8]);

impl<'a> RangeBounds<[u8]> for StartRange<'a> {
    fn start_bound(&self) -> Bound<&[u8]> {
        Bound::Included(self.0)
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }
}

impl ConcurrentMap for Mutex<BTreeMap<Vec<u8>, KeyLock>> {
    fn get(&self, key: &[u8]) -> Option<KeyLock> {
        let map = self.lock().unwrap();
        map.get(key).cloned()
    }

    fn lower_bound(&self, key: &[u8]) -> Option<(Vec<u8>, KeyLock)> {
        let map = self.lock().unwrap();
        map.range(StartRange(key))
            .next()
            .map(|(key, lock)| (key.to_owned(), lock.clone()))
    }

    fn insert_if_not_exists(&self, key: &[u8], key_lock: KeyLock) -> bool {
        let mut map = self.lock().unwrap();
        if map.contains_key(key) {
            false
        } else {
            map.insert(key.to_owned(), key_lock);
            true
        }
    }

    fn remove(&self, key: &[u8]) {
        let mut map = self.lock().unwrap();
        map.remove(key);
    }
}
