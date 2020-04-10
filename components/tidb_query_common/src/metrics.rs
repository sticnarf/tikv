// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum ExecutorName {
        batch_table_scan,
        batch_index_scan,
        batch_selection,
        batch_simple_aggr,
        batch_fast_hash_aggr,
        batch_slow_hash_aggr,
        batch_stream_aggr,
        batch_limit,
        batch_top_n,
        table_scan,
        index_scan,
        selection,
        hash_aggr,
        stream_aggr,
        top_n,
        limit,
    }

    pub struct LocalCoprExecutorCount: LocalIntCounter {
        "type" => ExecutorName,
    }
}

lazy_static::lazy_static! {
    static ref COPR_EXECUTOR_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_executor_count",
        "Total number of each executor",
        &["type"]
    )
    .unwrap();
}

lazy_static::lazy_static! {
    pub static ref EXECUTOR_COUNT_METRICS: LocalCoprExecutorCount =
        auto_flush_from!(COPR_EXECUTOR_COUNT, LocalCoprExecutorCount);

    pub static ref COPR_BATCH_SIZE: Histogram = register_histogram!(
        "tikv_coprocessor_batch_size",
        "The batch size of coprocessor execution",
        exponential_buckets(32.0, 2.0, 10).unwrap()
    )
    .unwrap();
    pub static ref COPR_BATCH_DURATION: Histogram = register_histogram!(
        "tikv_coprocessor_batch_duration",
        "The batch duration of coprocessor execution",
        exponential_buckets(1e-6, 2.0, 20).unwrap()
    )
    .unwrap();
}
