// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::iter::{self, FromIterator};
use std::marker::PhantomData;
use std::pin::Pin;

use kvproto::kvrpcpb::*;
use tikv_util::collections::HashMap;
use txn_types::{Key, Lock, Mutation, TimeStamp};

use crate::storage::concurrency_manager::{ConcurrencyManager, OrderedLockMap, TxnMutexGuard};
use crate::storage::lock_manager::WaitTimeout;
use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::types::{MvccInfo, PessimisticLockRes, StorageCallbackType, TxnStatus};
use crate::storage::Result;

/// Store Transaction scheduler commands.
///
/// Learn more about our transaction system at
/// [Deep Dive TiKV: Distributed Transactions](https://tikv.org/docs/deep-dive/distributed-transaction/introduction/)
///
/// These are typically scheduled and used through the [`Storage`](Storage) with functions like
/// [`Storage::prewrite`](Storage::prewrite) trait and are executed asynchronously.
// Logic related to these can be found in the `src/storage/txn/proccess.rs::process_write_impl` function.
pub struct Command {
    pub ctx: Context,
    pub kind: CommandKind,
}

pub struct TypedCommand<T> {
    pub cmd: Command,
    _pd: PhantomData<T>,
}

impl<T: StorageCallbackType> From<Command> for TypedCommand<T> {
    fn from(cmd: Command) -> TypedCommand<T> {
        TypedCommand {
            cmd,
            _pd: PhantomData,
        }
    }
}

impl<T> From<TypedCommand<T>> for Command {
    fn from(t: TypedCommand<T>) -> Command {
        t.cmd
    }
}

impl From<PrewriteRequest> for TypedCommand<Vec<Result<()>>> {
    fn from(mut req: PrewriteRequest) -> Self {
        let for_update_ts = req.get_for_update_ts();
        if for_update_ts == 0 {
            Prewrite::new(
                req.take_mutations().into_iter().map(Into::into).collect(),
                req.take_primary_lock(),
                req.get_start_version().into(),
                req.get_lock_ttl(),
                req.get_skip_constraint_check(),
                req.get_txn_size(),
                req.get_min_commit_ts().into(),
                req.take_context(),
            )
        } else {
            let is_pessimistic_lock = req.take_is_pessimistic_lock();
            let mutations = req
                .take_mutations()
                .into_iter()
                .map(Into::into)
                .zip(is_pessimistic_lock.into_iter())
                .collect();
            PrewritePessimistic::new(
                mutations,
                req.take_primary_lock(),
                req.get_start_version().into(),
                req.get_lock_ttl(),
                for_update_ts.into(),
                req.get_txn_size(),
                req.get_min_commit_ts().into(),
                req.take_context(),
            )
        }
    }
}

impl From<PessimisticLockRequest> for TypedCommand<Result<PessimisticLockRes>> {
    fn from(mut req: PessimisticLockRequest) -> Self {
        let keys = req
            .take_mutations()
            .into_iter()
            .map(|x| match x.get_op() {
                Op::PessimisticLock => (
                    Key::from_raw(x.get_key()),
                    x.get_assertion() == Assertion::NotExist,
                ),
                _ => panic!("mismatch Op in pessimistic lock mutations"),
            })
            .collect();

        AcquirePessimisticLock::new(
            keys,
            req.take_primary_lock(),
            req.get_start_version().into(),
            req.get_lock_ttl(),
            req.get_is_first_lock(),
            req.get_for_update_ts().into(),
            WaitTimeout::from_encoded(req.get_wait_timeout()),
            req.get_return_values(),
            req.get_min_commit_ts().into(),
            req.take_context(),
        )
    }
}

impl From<CommitRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: CommitRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

        Commit::new(
            keys,
            req.get_start_version().into(),
            req.get_commit_version().into(),
            req.take_context(),
        )
    }
}

impl From<CleanupRequest> for TypedCommand<()> {
    fn from(mut req: CleanupRequest) -> Self {
        Cleanup::new(
            Key::from_raw(req.get_key()),
            req.get_start_version().into(),
            req.get_current_ts().into(),
            req.take_context(),
        )
    }
}

impl From<BatchRollbackRequest> for TypedCommand<()> {
    fn from(mut req: BatchRollbackRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
        Rollback::new(keys, req.get_start_version().into(), req.take_context())
    }
}

impl From<PessimisticRollbackRequest> for TypedCommand<Vec<Result<()>>> {
    fn from(mut req: PessimisticRollbackRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

        PessimisticRollback::new(
            keys,
            req.get_start_version().into(),
            req.get_for_update_ts().into(),
            req.take_context(),
        )
    }
}

impl From<TxnHeartBeatRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: TxnHeartBeatRequest) -> Self {
        TxnHeartBeat::new(
            Key::from_raw(req.get_primary_lock()),
            req.get_start_version().into(),
            req.get_advise_lock_ttl(),
            req.take_context(),
        )
    }
}

impl From<CheckTxnStatusRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: CheckTxnStatusRequest) -> Self {
        CheckTxnStatus::new(
            Key::from_raw(req.get_primary_key()),
            req.get_lock_ts().into(),
            req.get_caller_start_ts().into(),
            req.get_current_ts().into(),
            req.get_rollback_if_not_exist(),
            req.take_context(),
        )
    }
}

impl From<ScanLockRequest> for TypedCommand<Vec<LockInfo>> {
    fn from(mut req: ScanLockRequest) -> Self {
        let start_key = if req.get_start_key().is_empty() {
            None
        } else {
            Some(Key::from_raw(req.get_start_key()))
        };

        ScanLock::new(
            req.get_max_version().into(),
            start_key,
            req.get_limit() as usize,
            req.take_context(),
        )
    }
}

impl From<ResolveLockRequest> for TypedCommand<()> {
    fn from(mut req: ResolveLockRequest) -> Self {
        let resolve_keys: Vec<Key> = req
            .get_keys()
            .iter()
            .map(|key| Key::from_raw(key))
            .collect();
        let txn_status = if req.get_start_version() > 0 {
            HashMap::from_iter(iter::once((
                req.get_start_version().into(),
                req.get_commit_version().into(),
            )))
        } else {
            HashMap::from_iter(
                req.take_txn_infos()
                    .into_iter()
                    .map(|info| (info.txn.into(), info.status.into())),
            )
        };

        if resolve_keys.is_empty() {
            ResolveLock::new(txn_status, None, vec![], req.take_context())
        } else {
            let start_ts: TimeStamp = req.get_start_version().into();
            assert!(!start_ts.is_zero());
            let commit_ts = req.get_commit_version().into();
            ResolveLockLite::new(start_ts, commit_ts, resolve_keys, req.take_context())
        }
    }
}

impl From<MvccGetByKeyRequest> for TypedCommand<MvccInfo> {
    fn from(mut req: MvccGetByKeyRequest) -> Self {
        MvccByKey::new(Key::from_raw(req.get_key()), req.take_context())
    }
}

impl From<MvccGetByStartTsRequest> for TypedCommand<Option<(Key, MvccInfo)>> {
    fn from(mut req: MvccGetByStartTsRequest) -> Self {
        MvccByStartTs::new(req.get_start_ts().into(), req.take_context())
    }
}

pub trait CommandExt {
    fn tag(&self) -> metrics::CommandKind;

    fn incr_cmd_metric(&self);

    fn ts(&self) -> TimeStamp {
        TimeStamp::zero()
    }

    fn readonly(&self) -> bool {
        false
    }

    fn is_sys_cmd(&self) -> bool {
        false
    }

    fn requires_pessimistic_txn(&self) -> bool {
        false
    }

    fn can_be_pipelined(&self) -> bool {
        false
    }

    fn write_bytes(&self) -> usize;

    fn gen_lock(&self, _latches: &Latches) -> latch::Lock;

    fn lock_keys<'cm>(
        &self,
        concurrency_manager: ConcurrencyManager,
    ) -> Pin<Box<dyn Future<Output = Vec<TxnMutexGuard<'cm, OrderedLockMap>>>>>;
}

macro_rules! command {
    (
        $(#[$outer_doc: meta])*
        $cmd: ident:
            cmd_ty => $cmd_ty: ty,
            content => {
                $($(#[$inner_doc:meta])* $arg: ident : $arg_ty: ty,)*
            }
    ) => {
        $(#[$outer_doc])*
        pub struct $cmd {
            $($(#[$inner_doc])* pub $arg: $arg_ty,)*
        }

        impl $cmd {
            pub fn new(
                $($arg: $arg_ty,)*
                ctx: Context,
            ) -> TypedCommand<$cmd_ty> {
                Command {
                    ctx,
                    kind: CommandKind::$cmd($cmd {
                        $($arg,)*
                    }),
                }
                .into()
            }
        }
    }
}

macro_rules! ts {
    ($ts:ident) => {
        fn ts(&self) -> TimeStamp {
            self.$ts
        }
    };
}

macro_rules! tag {
    ($tag:ident) => {
        fn tag(&self) -> metrics::CommandKind {
            metrics::CommandKind::$tag
        }

        fn incr_cmd_metric(&self) {
            KV_COMMAND_COUNTER_VEC_STATIC.$tag.inc();
        }
    };
}

macro_rules! write_bytes {
    ($field: ident) => {
        fn write_bytes(&self) -> usize {
            self.$field.as_encoded().len()
        }
    };
    ($field: ident: multiple) => {
        fn write_bytes(&self) -> usize {
            self.$field.iter().map(|x| x.as_encoded().len()).sum()
        }
    };
}

macro_rules! gen_lock {
    (empty) => {
        fn gen_lock(&self, _latches: &Latches) -> latch::Lock {
            latch::Lock::new(vec![])
        }

        fn lock_keys<'cm>(
            &self,
            concurrency_manager: ConcurrencyManager,
        ) -> Pin<Box<dyn Future<Output = Vec<TxnMutexGuard<'cm, OrderedLockMap>>>>> {
            Box::pin(async { Vec::new() })
        }
    };
    ($field: ident) => {
        fn gen_lock(&self, latches: &Latches) -> latch::Lock {
            latches.gen_lock(&[&self.$field])
        }

        fn lock_keys<'cm>(
            &self,
            concurrency_manager: ConcurrencyManager,
        ) -> Pin<Box<dyn Future<Output = Vec<TxnMutexGuard<'cm, OrderedLockMap>>>>> {
            Box::pin(async { vec![concurrency_manager.lock_key(&self.$field).await] })
        }
    };
    ($field: ident: multiple) => {
        fn gen_lock(&self, latches: &Latches) -> latch::Lock {
            latches.gen_lock(&self.$field)
        }
    };
    ($field: ident: multiple$transform: tt) => {
        fn gen_lock(&self, latches: &Latches) -> latch::Lock {
            #![allow(unused_parens)]
            let keys: Vec<&Key> = self.$field.iter().map($transform).collect();
            latches.gen_lock(&keys)
        }
    };
}

macro_rules! command_method {
    ($name:ident, $return_ty: ty, $value: expr) => {
        fn $name(&self) -> $return_ty {
            $value
        }
    };
}

command! {
    /// The prewrite phase of a transaction. The first phase of 2PC.
    ///
    /// This prepares the system to commit the transaction. Later a [`Commit`](CommandKind::Commit)
    /// or a [`Rollback`](CommandKind::Rollback) should follow.
    Prewrite:
        cmd_ty => Vec<Result<()>>,
        content => {
            /// The set of mutations to apply.
            mutations: Vec<Mutation>,
            /// The primary lock. Secondary locks (from `mutations`) will refer to the primary lock.
            primary: Vec<u8>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            lock_ttl: u64,
            skip_constraint_check: bool,
            /// How many keys this transaction involved.
            txn_size: u64,
            min_commit_ts: TimeStamp,
        }
}

impl CommandExt for Prewrite {
    tag!(prewrite);
    ts!(start_ts);

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for m in &self.mutations {
            match *m {
                Mutation::Put((ref key, ref value)) | Mutation::Insert((ref key, ref value)) => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                Mutation::Delete(ref key) | Mutation::Lock(ref key) => {
                    bytes += key.as_encoded().len();
                }
                Mutation::CheckNotExists(_) => (),
            }
        }
        bytes
    }

    gen_lock!(mutations: multiple(|x| x.key()));
}

impl Prewrite {
    #[cfg(test)]
    pub fn with_defaults(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
    ) -> TypedCommand<Vec<Result<()>>> {
        Prewrite::new(
            mutations,
            primary,
            start_ts,
            0,
            false,
            0,
            TimeStamp::default(),
            Context::default(),
        )
    }

    #[cfg(test)]
    pub fn with_lock_ttl(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        lock_ttl: u64,
    ) -> TypedCommand<Vec<Result<()>>> {
        Prewrite::new(
            mutations,
            primary,
            start_ts,
            lock_ttl,
            false,
            0,
            TimeStamp::default(),
            Context::default(),
        )
    }

    pub fn with_context(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        ctx: Context,
    ) -> TypedCommand<Vec<Result<()>>> {
        Prewrite::new(
            mutations,
            primary,
            start_ts,
            0,
            false,
            0,
            TimeStamp::default(),
            ctx,
        )
    }
}

command! {
    /// The prewrite phase of a transaction using pessimistic locking. The first phase of 2PC.
    ///
    /// This prepares the system to commit the transaction. Later a [`Commit`](CommandKind::Commit)
    /// or a [`Rollback`](CommandKind::Rollback) should follow.
    PrewritePessimistic:
        cmd_ty => Vec<Result<()>>,
        content => {
            /// The set of mutations to apply; the bool = is pessimistic lock.
            mutations: Vec<(Mutation, bool)>,
            /// The primary lock. Secondary locks (from `mutations`) will refer to the primary lock.
            primary: Vec<u8>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            lock_ttl: u64,
            for_update_ts: TimeStamp,
            /// How many keys this transaction involved.
            txn_size: u64,
            min_commit_ts: TimeStamp,
        }
}

impl CommandExt for PrewritePessimistic {
    tag!(prewrite);
    ts!(start_ts);
    command_method!(requires_pessimistic_txn, bool, true);

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for (m, _) in &self.mutations {
            match *m {
                Mutation::Put((ref key, ref value)) | Mutation::Insert((ref key, ref value)) => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                Mutation::Delete(ref key) | Mutation::Lock(ref key) => {
                    bytes += key.as_encoded().len();
                }
                Mutation::CheckNotExists(_) => (),
            }
        }
        bytes
    }

    gen_lock!(mutations: multiple(|(x, _)| x.key()));
}

command! {
    /// Acquire a Pessimistic lock on the keys.
    ///
    /// This can be rolled back with a [`PessimisticRollback`](CommandKind::PessimisticRollback) command.
    AcquirePessimisticLock:
        cmd_ty => Result<PessimisticLockRes>,
        content => {
            /// The set of keys to lock.
            keys: Vec<(Key, bool)>,
            /// The primary lock. Secondary locks (from `keys`) will refer to the primary lock.
            primary: Vec<u8>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            lock_ttl: u64,
            is_first_lock: bool,
            for_update_ts: TimeStamp,
            /// Time to wait for lock released in milliseconds when encountering locks.
            wait_timeout: Option<WaitTimeout>,
            /// If it is true, TiKV will return values of the keys if no error, so TiDB can cache the values for
            /// later read in the same transaction.
            return_values: bool,
            min_commit_ts: TimeStamp,
        }
}

impl CommandExt for AcquirePessimisticLock {
    tag!(acquire_pessimistic_lock);
    ts!(start_ts);
    command_method!(requires_pessimistic_txn, bool, true);
    command_method!(can_be_pipelined, bool, true);

    fn write_bytes(&self) -> usize {
        self.keys
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_lock!(keys: multiple(|x| &x.0));
}

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](CommandKind::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        content => {
            /// The keys affected.
            keys: Vec<Key>,
            /// The lock timestamp.
            lock_ts: TimeStamp,
            /// The commit timestamp.
            commit_ts: TimeStamp,
        }
}

impl CommandExt for Commit {
    tag!(commit);
    ts!(commit_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

command! {
    /// Rollback mutations on a single key.
    ///
    /// This should be following a [`Prewrite`](CommandKind::Prewrite) on the given key.
    Cleanup:
        cmd_ty => (),
        content => {
            key: Key,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The approximate current ts when cleanup request is invoked, which is used to check the
            /// lock's TTL. 0 means do not check TTL.
            current_ts: TimeStamp,
        }
}

impl CommandExt for Cleanup {
    tag!(cleanup);
    ts!(start_ts);
    write_bytes!(key);
    gen_lock!(key);
}

command! {
    /// Rollback from the transaction that was started at `start_ts`.
    ///
    /// This should be following a [`Prewrite`](CommandKind::Prewrite) on the given key.
    Rollback:
        cmd_ty => (),
        content => {
            keys: Vec<Key>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
        }
}

impl CommandExt for Rollback {
    tag!(rollback);
    ts!(start_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

command! {
    /// Rollback pessimistic locks identified by `start_ts` and `for_update_ts`.
    ///
    /// This can roll back an [`AcquirePessimisticLock`](CommandKind::AcquirePessimisticLock) command.
    PessimisticRollback:
        cmd_ty => Vec<Result<()>>,
        content => {
            /// The keys to be rolled back.
            keys: Vec<Key>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            for_update_ts: TimeStamp,
        }
}

impl CommandExt for PessimisticRollback {
    tag!(pessimistic_rollback);
    ts!(start_ts);
    command_method!(requires_pessimistic_txn, bool, true);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

command! {
    /// Heart beat of a transaction. It enlarges the primary lock's TTL.
    ///
    /// This is invoked on a transaction's primary lock. The lock may be generated by either
    /// [`AcquirePessimisticLock`](CommandKind::AcquirePessimisticLock) or
    /// [`Prewrite`](CommandKind::Prewrite).
    TxnHeartBeat:
        cmd_ty => TxnStatus,
        content => {
            /// The primary key of the transaction.
            primary_key: Key,
            /// The transaction's start_ts.
            start_ts: TimeStamp,
            /// The new TTL that will be used to update the lock's TTL. If the lock's TTL is already
            /// greater than `advise_ttl`, nothing will happen.
            advise_ttl: u64,
        }
}

impl CommandExt for TxnHeartBeat {
    tag!(txn_heart_beat);
    ts!(start_ts);
    write_bytes!(primary_key);
    gen_lock!(primary_key);
}

command! {
    /// Check the status of a transaction. This is usually invoked by a transaction that meets
    /// another transaction's lock. If the primary lock is expired, it will rollback the primary
    /// lock. If the primary lock exists but is not expired, it may update the transaction's
    /// `min_commit_ts`. Returns a [`TxnStatus`](TxnStatus) to represent the status.
    ///
    /// This is invoked on a transaction's primary lock. The lock may be generated by either
    /// [`AcquirePessimisticLock`](CommandKind::AcquirePessimisticLock) or
    /// [`Prewrite`](CommandKind::Prewrite).
    CheckTxnStatus:
        cmd_ty => TxnStatus,
        content => {
            /// The primary key of the transaction.
            primary_key: Key,
            /// The lock's ts, namely the transaction's start_ts.
            lock_ts: TimeStamp,
            /// The start_ts of the transaction that invokes this command.
            caller_start_ts: TimeStamp,
            /// The approximate current_ts when the command is invoked.
            current_ts: TimeStamp,
            /// Specifies the behavior when neither commit/rollback record nor lock is found. If true,
            /// rollbacks that transaction; otherwise returns an error.
            rollback_if_not_exist: bool,
        }
}

impl CommandExt for CheckTxnStatus {
    tag!(check_txn_status);
    ts!(lock_ts);
    write_bytes!(primary_key);
    gen_lock!(primary_key);
}

command! {
    /// Scan locks from `start_key`, and find all locks whose timestamp is before `max_ts`.
    ScanLock:
        cmd_ty => Vec<LockInfo>,
        content => {
            /// The maximum transaction timestamp to scan.
            max_ts: TimeStamp,
            /// The key to start from. (`None` means start from the very beginning.)
            start_key: Option<Key>,
            /// The result limit.
            limit: usize,
        }
}

impl CommandExt for ScanLock {
    tag!(scan_lock);
    ts!(max_ts);
    command_method!(readonly, bool, true);
    command_method!(is_sys_cmd, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

command! {
    /// Resolve locks according to `txn_status`.
    ///
    /// During the GC operation, this should be called to clean up stale locks whose timestamp is
    /// before safe point.
    ResolveLock:
        cmd_ty => (),
        content => {
            /// Maps lock_ts to commit_ts. If a transaction was rolled back, it is mapped to 0.
            ///
            /// For example, let `txn_status` be `{ 100: 101, 102: 0 }`, then it means that the transaction
            /// whose start_ts is 100 was committed with commit_ts `101`, and the transaction whose
            /// start_ts is 102 was rolled back. If there are these keys in the db:
            ///
            /// * "k1", lock_ts = 100
            /// * "k2", lock_ts = 102
            /// * "k3", lock_ts = 104
            /// * "k4", no lock
            ///
            /// Here `"k1"`, `"k2"` and `"k3"` each has a not-yet-committed version, because they have
            /// locks. After calling resolve_lock, `"k1"` will be committed with commit_ts = 101 and `"k2"`
            /// will be rolled back.  `"k3"` will not be affected, because its lock_ts is not contained in
            /// `txn_status`. `"k4"` will not be affected either, because it doesn't have a non-committed
            /// version.
            txn_status: HashMap<TimeStamp, TimeStamp>,
            scan_key: Option<Key>,
            key_locks: Vec<(Key, Lock)>,
        }
}

impl CommandExt for ResolveLock {
    tag!(resolve_lock);

    fn readonly(&self) -> bool {
        self.key_locks.is_empty()
    }

    command_method!(is_sys_cmd, bool, true);

    fn write_bytes(&self) -> usize {
        self.key_locks
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_lock!(key_locks: multiple(|(key, _)| key));
}

command! {
    /// Resolve locks on `resolve_keys` according to `start_ts` and `commit_ts`.
    ResolveLockLite:
        cmd_ty => (),
        content => {
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The transaction commit timestamp.
            commit_ts: TimeStamp,
            /// The keys to resolve.
            resolve_keys: Vec<Key>,
        }
}

impl CommandExt for ResolveLockLite {
    tag!(resolve_lock_lite);
    ts!(start_ts);
    command_method!(is_sys_cmd, bool, true);
    write_bytes!(resolve_keys: multiple);
    gen_lock!(resolve_keys: multiple);
}

command! {
    /// **Testing functionality:** Latch the given keys for given duration.
    ///
    /// This means other write operations that involve these keys will be blocked.
    Pause:
        cmd_ty => (),
        content => {
            /// The keys to hold latches on.
            keys: Vec<Key>,
            /// The amount of time in milliseconds to latch for.
            duration: u64,
        }
}

impl CommandExt for Pause {
    tag!(pause);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

command! {
    /// Retrieve MVCC information for the given key.
    MvccByKey:
        cmd_ty => MvccInfo,
        content => {
            key: Key,
        }
}

impl CommandExt for MvccByKey {
    tag!(key_mvcc);
    command_method!(readonly, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

command! {
    /// Retrieve MVCC info for the first committed key which `start_ts == ts`.
    MvccByStartTs:
        cmd_ty => Option<(Key, MvccInfo)>,
        content => {
            start_ts: TimeStamp,
        }
}

impl CommandExt for MvccByStartTs {
    tag!(start_ts_mvcc);
    ts!(start_ts);
    command_method!(readonly, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

pub enum CommandKind {
    Prewrite(Prewrite),
    PrewritePessimistic(PrewritePessimistic),
    AcquirePessimisticLock(AcquirePessimisticLock),
    Commit(Commit),
    Cleanup(Cleanup),
    Rollback(Rollback),
    PessimisticRollback(PessimisticRollback),
    TxnHeartBeat(TxnHeartBeat),
    CheckTxnStatus(CheckTxnStatus),
    ScanLock(ScanLock),
    ResolveLock(ResolveLock),
    ResolveLockLite(ResolveLockLite),
    Pause(Pause),
    MvccByKey(MvccByKey),
    MvccByStartTs(MvccByStartTs),
}

impl Command {
    // This is for backward compatibility, after some other refactors are done
    // we can remove CommandKind totally and use `&dyn CommandExt` instead
    fn command_ext(&self) -> &dyn CommandExt {
        match &self.kind {
            CommandKind::Prewrite(t) => t,
            CommandKind::PrewritePessimistic(t) => t,
            CommandKind::AcquirePessimisticLock(t) => t,
            CommandKind::Commit(t) => t,
            CommandKind::Cleanup(t) => t,
            CommandKind::Rollback(t) => t,
            CommandKind::PessimisticRollback(t) => t,
            CommandKind::TxnHeartBeat(t) => t,
            CommandKind::CheckTxnStatus(t) => t,
            CommandKind::ScanLock(t) => t,
            CommandKind::ResolveLock(t) => t,
            CommandKind::ResolveLockLite(t) => t,
            CommandKind::Pause(t) => t,
            CommandKind::MvccByKey(t) => t,
            CommandKind::MvccByStartTs(t) => t,
        }
    }

    pub fn readonly(&self) -> bool {
        self.command_ext().readonly()
    }

    pub fn incr_cmd_metric(&self) {
        self.command_ext().incr_cmd_metric()
    }

    pub fn priority(&self) -> CommandPri {
        self.ctx.get_priority()
    }

    pub fn is_sys_cmd(&self) -> bool {
        self.command_ext().is_sys_cmd()
    }

    pub fn need_flow_control(&self) -> bool {
        !self.readonly() && self.priority() != CommandPri::High
    }

    pub fn tag(&self) -> metrics::CommandKind {
        self.command_ext().tag()
    }

    pub fn ts(&self) -> TimeStamp {
        self.command_ext().ts()
    }

    pub fn write_bytes(&self) -> usize {
        self.command_ext().write_bytes()
    }

    pub fn gen_lock(&self, latches: &Latches) -> latch::Lock {
        self.command_ext().gen_lock(latches)
    }

    pub fn requires_pessimistic_txn(&self) -> bool {
        self.command_ext().requires_pessimistic_txn()
    }

    pub fn can_be_pipelined(&self) -> bool {
        self.command_ext().can_be_pipelined()
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.kind {
            CommandKind::Prewrite(Prewrite {
                ref mutations,
                start_ts,
                ..
            }) => write!(
                f,
                "kv::command::prewrite mutations({}) @ {} | {:?}",
                mutations.len(),
                start_ts,
                self.ctx,
            ),
            CommandKind::PrewritePessimistic(PrewritePessimistic {
                ref mutations,
                start_ts,
                ..
            }) => write!(
                f,
                "kv::command::prewrite_pessimistic mutations({}) @ {} | {:?}",
                mutations.len(),
                start_ts,
                self.ctx,
            ),
            CommandKind::AcquirePessimisticLock(AcquirePessimisticLock {
                ref keys,
                start_ts,
                for_update_ts,
                ..
            }) => write!(
                f,
                "kv::command::acquirepessimisticlock keys({}) @ {} {} | {:?}",
                keys.len(),
                start_ts,
                for_update_ts,
                self.ctx,
            ),
            CommandKind::Commit(Commit {
                ref keys,
                lock_ts,
                commit_ts,
                ..
            }) => write!(
                f,
                "kv::command::commit {} {} -> {} | {:?}",
                keys.len(),
                lock_ts,
                commit_ts,
                self.ctx,
            ),
            CommandKind::Cleanup(Cleanup {
                ref key, start_ts, ..
            }) => write!(
                f,
                "kv::command::cleanup {} @ {} | {:?}",
                key, start_ts, self.ctx
            ),
            CommandKind::Rollback(Rollback {
                ref keys, start_ts, ..
            }) => write!(
                f,
                "kv::command::rollback keys({}) @ {} | {:?}",
                keys.len(),
                start_ts,
                self.ctx,
            ),
            CommandKind::PessimisticRollback(PessimisticRollback {
                ref keys,
                start_ts,
                for_update_ts,
            }) => write!(
                f,
                "kv::command::pessimistic_rollback keys({}) @ {} {} | {:?}",
                keys.len(),
                start_ts,
                for_update_ts,
                self.ctx,
            ),
            CommandKind::TxnHeartBeat(TxnHeartBeat {
                ref primary_key,
                start_ts,
                advise_ttl,
            }) => write!(
                f,
                "kv::command::txn_heart_beat {} @ {} ttl {} | {:?}",
                primary_key, start_ts, advise_ttl, self.ctx,
            ),
            CommandKind::CheckTxnStatus(CheckTxnStatus {
                ref primary_key,
                lock_ts,
                caller_start_ts,
                current_ts,
                ..
            }) => write!(
                f,
                "kv::command::check_txn_status {} @ {} curr({}, {}) | {:?}",
                primary_key, lock_ts, caller_start_ts, current_ts, self.ctx,
            ),
            CommandKind::ScanLock(ScanLock {
                max_ts,
                ref start_key,
                limit,
                ..
            }) => write!(
                f,
                "kv::scan_lock {:?} {} @ {} | {:?}",
                start_key, limit, max_ts, self.ctx,
            ),
            CommandKind::ResolveLock(_) => write!(f, "kv::resolve_lock"),
            CommandKind::ResolveLockLite(_) => write!(f, "kv::resolve_lock_lite"),
            CommandKind::Pause(Pause { ref keys, duration }) => write!(
                f,
                "kv::command::pause keys:({}) {} ms | {:?}",
                keys.len(),
                duration,
                self.ctx,
            ),
            CommandKind::MvccByKey(MvccByKey { ref key }) => {
                write!(f, "kv::command::mvccbykey {:?} | {:?}", key, self.ctx)
            }
            CommandKind::MvccByStartTs(MvccByStartTs { ref start_ts }) => write!(
                f,
                "kv::command::mvccbystartts {:?} | {:?}",
                start_ts, self.ctx
            ),
        }
    }
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
