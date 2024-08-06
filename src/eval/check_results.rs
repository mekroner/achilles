use std::fs;
use std::io::Write;

use super::evaluator::*;
use nes_rust_client::query::stringify::stringify_query;
use yaml_rust2::{yaml::Hash, Yaml, YamlEmitter};

use crate::{
    query_gen::query_id::LancerQueryId,
    test_case_exec::{QueryExecStatus, TestCaseExec},
    LancerConfig,
};

pub struct QueryResult {
    id: LancerQueryId,
    relation: ResultRelation,
}

impl QueryResult {
    pub const fn new(id: LancerQueryId, relation: ResultRelation) -> Self {
        Self { id, relation }
    }
}

impl Into<Yaml> for &QueryResult {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("id".into()), (&self.id).into());
        map.insert(Yaml::String("relation".into()), (&self.relation).into());
        Yaml::Hash(map)
    }
}

pub struct TestCaseResult {
    id: u32,
    queries: Vec<QueryResult>,
}

impl Into<Yaml> for &TestCaseResult {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("id".into()), Yaml::Integer(self.id.into()));
        let queries = self.queries.iter().map(|q| q.into()).collect();
        map.insert(Yaml::String("queries".into()), Yaml::Array(queries));
        Yaml::Hash(map)
    }
}

pub fn write_test_case_result_to_file(config: &LancerConfig, test_case_results: &[TestCaseResult]) {
    let path = config.generated_files_path.join("test_case_results.yml");
    let yaml_test_cases: Vec<Yaml> = test_case_results
        .iter()
        .map(|test_case| test_case.into())
        .collect();
    let yaml_arr = Yaml::Array(yaml_test_cases);
    let mut out_str = String::new();
    let mut emitter = YamlEmitter::new(&mut out_str);
    emitter.dump(&yaml_arr).unwrap();
    let mut file = fs::File::create(path).expect("coordinator.yml has to be created!");
    write!(file, "{out_str}").unwrap();
}

pub fn check_test_cases(test_cases: &[TestCaseExec]) -> Vec<TestCaseResult> {
    log::info!("Checking results for equivalence:");
    test_cases.iter()
        .map(|test_case| {
            let queries = check_test_case(test_case);
            TestCaseResult {
                id: test_case.id,
                queries,
            }
        })
        .collect()
}

fn check_test_case(test_case: &TestCaseExec) -> Vec<QueryResult> {
    let mut queries = Vec::new();
    for props in test_case.others.iter() {
        if props.status != QueryExecStatus::Success {
            log::warn!(
                "Skipping result check for query {} because of execution state {:?}.",
                props.id(),
                props.status
            );
            continue;
        }
        log::debug!(
            "Check results of query {}: {}.",
            props.id(),
            stringify_query(props.query())
        );
        let relation = match compare_files(test_case.origin.result_path(), props.result_path()) {
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
        let query = QueryResult::new(props.id(), relation);
        queries.push(query);
    }
    queries
}
