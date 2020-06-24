use kvproto::kvrpcpb::LockInfo;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("key is locked by {0:?}")]
    KeyIsLocked(LockInfo),
}

pub type Result<T> = std::result::Result<T, Error>;
