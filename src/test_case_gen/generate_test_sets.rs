use std::path::PathBuf;

use crate::stream_schema::StreamSchema;
use crate::test_case_gen::oracle::QueryGenFactory;
use crate::LancerConfig;
use nes_rust_client::prelude::*;

use super::{
    oracle::{QueryGen, QueryGenStrategy},
    test_case::{TestCase, TestSet},
};

pub fn generate_test_sets(
    test_run_id: u32,
    config: &LancerConfig,
    schema: &StreamSchema,
) -> Vec<TestSet> {
    let query_gen_factory = QueryGenFactory::new();
    log::info!("Started  generate_test_cases:");
    let test_cases = config
        .test_config
        .oracles
        .iter()
        .enumerate()
        .map(|(oracle_id, &strat)| {
            let mut cases = vec![];
            let reps = config.test_config.oracle_reps as usize;
            for rep_id in 0..reps {
                let test_set_id = (oracle_id * reps + rep_id) as u32;
                let query_gen = query_gen_factory.create_query_gen(&schema, strat);
                let case = generate_test_case(test_run_id, test_set_id, config, &*query_gen, strat);
                cases.push(case);
            }
            cases
        })
        .flatten()
        .collect();
    log::info!("Done generating queries.");
    test_cases
}

fn generate_test_case(
    test_run_id: u32,
    test_set_id: u32,
    config: &LancerConfig,
    query_gen: &dyn QueryGen,
    strategy: QueryGenStrategy,
) -> TestSet {
    let origin_path = config
        .path_config
        .result(test_run_id)
        .join(format!("test-set{test_set_id}-origin.csv"));
    let origin_sink = Sink::csv_file(&origin_path, false);
    let q_origin = query_gen.origin().sink(origin_sink);
    let origin = TestCase::origin(q_origin, PathBuf::from(origin_path));
    let others = (0..config.test_config.test_case_count)
        .map(|other_id| {
            let other_path = config
                .path_config
                .result(test_run_id)
                .join(format!("test-set{test_set_id}-other{other_id}.csv"));
            let other_sink = Sink::csv_file(&other_path, false);
            let q_other = query_gen.other().sink(other_sink);
            TestCase::other(other_id, q_other, PathBuf::from(other_path))
        })
        .collect();

    TestSet {
        id: test_set_id,
        strategy,
        origin,
        others,
    }
}
