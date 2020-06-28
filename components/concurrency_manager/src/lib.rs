#![feature(atomic_min_max)]

pub mod errors;
mod imp;

use async_trait::async_trait;
use errors::Result;
use kvproto::kvrpcpb::LockInfo;
use txn_types::TimeStamp;

#[async_trait]
pub trait ConcurrencyManager<'a>: Send + Sync {
    type LockGuard;

    fn max_read_ts(&'a self) -> TimeStamp;

    fn read_key_check(
        &'a self,
        key: &[u8],
        ts: TimeStamp,
        check_fn: impl FnMut(&LockInfo, TimeStamp) -> bool,
    ) -> Result<()>;

    fn read_range_check(
        &'a self,
        start_key: &[u8],
        end_key: &[u8],
        ts: TimeStamp,
        check_fn: impl FnMut(&LockInfo, TimeStamp) -> bool,
    ) -> Result<()>;

    async fn lock_key(&'a self, key: &'a [u8]) -> Self::LockGuard;
}

pub trait LockGuard: Send + Sync + Drop {
    fn update(&self, lock: Lock);
}

pub enum Lock {
    Empty,
    Pessimistic(LockInfo),
    Prewrite(LockInfo),
}
