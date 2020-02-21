// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

use kvproto::kvrpcpb::Context;

use tidb_query::codec::Datum;
use tikv::coprocessor::Endpoint;
use tikv::read_pool::{build_yatp_read_pool, tests::DummyReporter, ReadPool};
use tikv::server::Config;
use tikv::storage::kv::RocksEngine;
use tikv::storage::{Engine, TestEngineBuilder};

#[derive(Clone)]
pub struct ProductTable(Table);

impl ProductTable {
    pub fn new() -> ProductTable {
        let id = ColumnBuilder::new()
            .col_type(TYPE_LONG)
            .primary_key(true)
            .build();
        let idx_id = next_id();
        let name = ColumnBuilder::new()
            .col_type(TYPE_VAR_CHAR)
            .index_key(idx_id)
            .build();
        let count = ColumnBuilder::new()
            .col_type(TYPE_LONG)
            .index_key(idx_id)
            .build();
        let table = TableBuilder::new()
            .add_col("id", id)
            .add_col("name", name)
            .add_col("count", count)
            .build();
        ProductTable(table)
    }
}

impl std::ops::Deref for ProductTable {
    type Target = Table;

    fn deref(&self) -> &Table {
        &self.0
    }
}

pub fn init_data_with_engine_and_commit<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (Store<E>, Endpoint<E>, ReadPool) {
    init_data_with_details(ctx, engine, tbl, vals, commit, &Config::default())
}

pub fn init_data_with_details<E: Engine>(
    ctx: Context,
    engine: E,
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
    cfg: &Config,
) -> (Store<E>, Endpoint<E>, ReadPool) {
    let mut store = Store::from_engine(engine.clone());

    store.begin();
    for &(id, name, count) in vals {
        store
            .insert_into(&tbl)
            .set(&tbl["id"], Datum::I64(id))
            .set(&tbl["name"], name.map(str::as_bytes).into())
            .set(&tbl["count"], Datum::I64(count))
            .execute_with_ctx(ctx.clone());
    }
    if commit {
        store.commit_with_ctx(ctx);
    }

    let pool = build_yatp_read_pool(
        &tikv::config::UnifiedReadPoolConfig::default_for_test(),
        DummyReporter,
        engine,
    );
    let cop = Endpoint::new(cfg, pool.handle());
    (store, cop, pool)
}

pub fn init_data_with_commit(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    commit: bool,
) -> (Store<RocksEngine>, Endpoint<RocksEngine>, ReadPool) {
    let engine = TestEngineBuilder::new().build().unwrap();
    init_data_with_engine_and_commit(Context::default(), engine, tbl, vals, commit)
}

// This function will create a Product table and initialize with the specified data.
pub fn init_with_data(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
) -> (Store<RocksEngine>, Endpoint<RocksEngine>, ReadPool) {
    init_data_with_commit(tbl, vals, true)
}
