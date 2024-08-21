use std::path::PathBuf;

use crate::query_gen::oracle::{QueryGenFactory, QueryGenStrategy};
use crate::stream_schema::StreamSchema;
use crate::LancerConfig;
use nes_rust_client::prelude::*;

use super::{
    oracle::QueryGen,
    test_case::{TestCase, TestSet},
};

pub struct TestConfig {
    oracles: Vec<QueryGenStrategy>,
}

pub fn generate_test_sets(config: &LancerConfig, schema: &StreamSchema) -> Vec<TestSet> {
    let query_gen_factory = QueryGenFactory::new();
    let oracle_reps = 1;
    let test_conf = TestConfig {
        oracles: vec![
            // QueryGenStrategy::Filter, 
            // QueryGenStrategy::Map,
            QueryGenStrategy::AggregationMin
        ],
    };
    log::info!("Started  generate_test_cases:");
    let test_cases = test_conf
        .oracles
        .iter()
        .enumerate()
        .map(|(oracle_id, &strat)| {
            let mut cases = vec![];
            for rep_id in 0..oracle_reps {
                let id = (oracle_id * oracle_reps + rep_id) as u32;
                let query_gen = query_gen_factory.create_query_gen(&schema, strat);
                let case = generate_test_case(id, config, &*query_gen);
                cases.push(case);
            }
            cases
        })
        .flatten()
        .collect();
    log::info!("Done generating queries.");
    test_cases
}

fn generate_test_case(id: u32, config: &LancerConfig, query_gen: &dyn QueryGen) -> TestSet {
    let origin_path = config
        .generated_files_path
        .join(format!("result-test-case{id}-origin.csv"));
    let origin_sink = Sink::csv_file(&origin_path, false);
    let q_origin = query_gen.origin().sink(origin_sink);
    let origin = TestCase::origin(q_origin, PathBuf::from(origin_path));
    let others = (0..1)
        .map(|other_id| {
            let other_path = config
                .generated_files_path
                .join(format!("result-test-case{id}-other{other_id}.csv"));
            let other_sink = Sink::csv_file(&other_path, false);
            let q_other = query_gen.other().sink(other_sink);
            TestCase::other(other_id, q_other, PathBuf::from(other_path))
        })
        .collect();

    TestSet { id, origin, others }
}
