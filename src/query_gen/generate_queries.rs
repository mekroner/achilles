use std::path::PathBuf;

use crate::query_gen::{query_origin_filter, query_part_filter};
use crate::LancerConfig;
use nes_rust_client::prelude::*;

use super::test_case::{QueryProps, TestCase};

pub fn generate_test_cases(config: &LancerConfig) -> Vec<TestCase> {
    log::info!("Started generating queries:");
    let runs = (0..1)
        .map(|id| generate_test_case(id, config))
        .collect();
    log::info!("Done generating queries.");
    runs
}

fn generate_test_case(id: u32, config: &LancerConfig) -> TestCase {
    let origin_path = config
        .generated_files_path
        .join(format!("result-run{id}-origin.csv"));
    let origin_sink = Sink::csv_file(&origin_path, false);
    let origin = QueryProps::origin(query_origin_filter(origin_sink), PathBuf::from(origin_path));
    let others = (0..2)
        .map(|id| {
            let other_path = config
                .generated_files_path
                .join(format!("result-run{id}-other{id}.csv"));
            let other_sink = Sink::csv_file(&other_path, false);
            QueryProps::other(id, query_part_filter(other_sink), PathBuf::from(other_path))
        })
        .collect();

    TestCase {
        id,
        origin,
        others,
    }
}
