// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub max_batch_size: usize,
    pub reschedule_duration: ReadableDuration,
    pub pool_size: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            max_batch_size: 256,
            reschedule_duration: ReadableDuration::secs(5),
            pool_size: 2,
        }
    }
}
