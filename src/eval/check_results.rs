use std::fs;
use std::io::Write;

use super::evaluator::*;
use nes_rust_client::query::stringify::stringify_query;
use yaml_rust2::{yaml::Hash, Yaml, YamlEmitter};

use crate::{
    test_case_exec::{TestCaseExecStatus, TestSetExec},
    test_case_gen::query_id::TestCaseId,
    LancerConfig,
};

pub struct TestCaseResult {
    id: TestCaseId,
    relation: ResultRelation,
}

impl TestCaseResult {
    pub const fn new(id: TestCaseId, relation: ResultRelation) -> Self {
        Self { id, relation }
    }
}

impl Into<Yaml> for &TestCaseResult {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("id".into()), (&self.id).into());
        map.insert(Yaml::String("relation".into()), (&self.relation).into());
        Yaml::Hash(map)
    }
}

pub struct TestSetResult {
    id: u32,
    test_cases: Vec<TestCaseResult>,
}

impl Into<Yaml> for &TestSetResult {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("id".into()), Yaml::Integer(self.id.into()));
        let queries = self.test_cases.iter().map(|q| q.into()).collect();
        map.insert(Yaml::String("test_cases".into()), Yaml::Array(queries));
        Yaml::Hash(map)
    }
}

pub fn write_test_set_results_to_file(
    test_run_id: u32,
    config: &LancerConfig,
    test_set_results: &[TestSetResult],
) {
    let path = config.path_config.test_set_results(test_run_id);
    let yaml_test_sets: Vec<Yaml> = test_set_results
        .iter()
        .map(|test_set| test_set.into())
        .collect();
    let yaml_arr = Yaml::Array(yaml_test_sets);
    let mut out_str = String::new();
    let mut emitter = YamlEmitter::new(&mut out_str);
    emitter.dump(&yaml_arr).unwrap();
    let mut file = fs::File::create(path).expect("coordinator.yml has to be created!");
    write!(file, "{out_str}").unwrap();
}

pub fn check_test_sets(test_sets: &[TestSetExec]) -> Vec<TestSetResult> {
    log::info!("Checking results for equivalence:");
    test_sets
        .iter()
        .map(|test_set| {
            let test_cases = check_test_set(test_set);
            TestSetResult {
                id: test_set.id,
                test_cases,
            }
        })
        .collect()
}

pub fn check_test_set(test_set: &TestSetExec) -> Vec<TestCaseResult> {
    let mut test_case_results = Vec::new();
    for test_case in test_set.others.iter() {
        if test_case.status != TestCaseExecStatus::Success {
            log::warn!(
                "Skipping result check for test case {} because of execution state {:?}.",
                test_case.id(),
                test_case.status
            );
            continue;
        }
        log::debug!(
            "Check results of test case {}: {}.",
            test_case.id(),
            stringify_query(test_case.query())
        );
        let relation = match compare_files(test_set.origin.result_path(), test_case.result_path()) {
            Ok(ResultRelation::Equal) => {
                log::debug!("Result files are equal.");
                ResultRelation::Equal
            }
            Ok(ResultRelation::Reordered) => {
                log::debug!("Result files are reordered.");
                ResultRelation::Reordered
            }
            Ok(ResultRelation::Diff) => {
                log::warn!("Result files are not equal.");
                ResultRelation::Diff
            }
            Err(err) => {
                log::error!("{err}");
                continue;
            }
        };
        let test_case_result = TestCaseResult::new(test_case.id(), relation);
        test_case_results.push(test_case_result);
    }
    test_case_results
}
