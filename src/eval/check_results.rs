use std::fs;
use std::io::Write;

use super::evaluator::*;
use nes_rust_client::query::stringify::stringify_query;
use yaml_rust2::{yaml::Hash, Yaml, YamlEmitter, YamlLoader};

use crate::{
    test_case_exec::{TestCaseExecStatus, TestSetExec},
    test_case_gen::{oracle::QueryGenStrategy, query_id::TestCaseId},
    yaml_util::store_yaml_array,
    LancerConfig,
};

pub struct TestSetResult {
    pub id: u32,
    pub strategy: QueryGenStrategy,
    pub test_cases: Vec<TestCaseResult>,
}

pub struct TestCaseResult {
    pub id: TestCaseId,
    pub query_string: String,
    pub relation: ResultRelation,
}

// yaml

impl Into<Yaml> for &TestSetResult {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("id".into()), Yaml::Integer(self.id.into()));
        map.insert(Yaml::String("strategy".into()), (&self.strategy).into());
        let queries = self.test_cases.iter().map(|q| q.into()).collect();
        map.insert(Yaml::String("test_cases".into()), Yaml::Array(queries));
        Yaml::Hash(map)
    }
}

impl TryFrom<&Yaml> for TestSetResult {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Yaml::Integer(id) = value["id"] else {
            return Err("Failed to Parse TestCaseResult id.".to_string());
        };
        let strategy = (&value["strategy"]).try_into()?;
        let Yaml::Array(arr) = &value["test_cases"] else {
            return Err("Should be able to parse test_case field as Array.".to_string());
        };
        let test_cases = arr
            .iter()
            .map(|yaml_obj| TestCaseResult::try_from(yaml_obj))
            .collect::<Result<Vec<_>, Self::Error>>()?;
        Ok(TestSetResult {
            id: id as u32,
            strategy,
            test_cases,
        })
    }
}

impl Into<Yaml> for &TestCaseResult {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("id".into()), (&self.id).into());
        map.insert(Yaml::String("relation".into()), (&self.relation).into());
        map.insert(
            Yaml::String("query".into()),
            Yaml::String(self.query_string.clone()),
        );
        Yaml::Hash(map)
    }
}

impl TryFrom<&Yaml> for TestCaseResult {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let id = TestCaseId::try_from(&value["id"])?;
        let relation = ResultRelation::try_from(&value["relation"])?;
        let Yaml::String(query_string) = &value["query"] else {
            return Err("Unable to parse TestCaseResult: cannot read query.".into());
        };
        Ok(Self {
            id,
            relation,
            query_string: query_string.to_string(),
        })
    }
}

pub fn write_test_set_results_to_file(
    test_run_id: u32,
    config: &LancerConfig,
    test_set_results: &[TestSetResult],
) {
    let path = config.path_config.test_set_results(test_run_id);
    store_yaml_array(&path, test_set_results);
}

pub fn read_test_set_results_from_file(
    test_run_id: u32,
    config: &LancerConfig,
) -> Vec<TestSetResult> {
    let path = config.path_config.test_set_results(test_run_id);
    let content = fs::read_to_string(path).expect("Should have been able to read the file!");
    let docs = YamlLoader::load_from_str(&content).expect("Should have been able to parse Yaml.");
    let doc = &docs.first().expect("Should have one element");
    let Yaml::Array(arr) = doc else {
        panic!("Should have been able to parse Yaml::Array.")
    };
    arr.iter()
        .map(|yaml_obj| yaml_obj.try_into().expect("Should not fail to parse Yaml."))
        .collect()
}

pub fn check_test_sets(test_sets: &[TestSetExec]) -> Vec<TestSetResult> {
    log::info!("Checking results for equivalence:");
    test_sets
        .iter()
        .map(|test_set| {
            let test_cases = check_test_set(test_set);
            TestSetResult {
                id: test_set.id,
                strategy: test_set.strategy,
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
        let test_case_result = TestCaseResult {
            id: test_case.id(),
            relation,
            query_string: stringify_query(&test_case.query.query),
        };
        test_case_results.push(test_case_result);
    }
    test_case_results
}
