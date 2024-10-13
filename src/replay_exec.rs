use std::fs;

use nes_rust_client::query::sink::Sink;

use crate::{
    eval::check_results::check_test_set,
    process_test_case::process_test_sets::{process_single_test_case, process_test_set},
    test_case_gen::{
        query_id::TestCaseId,
        test_case::{TestCase, TestSet},
    },
    yaml_util::load_yaml_array,
    LancerConfig,
};

#[derive(Clone)]
pub enum ReplayExec {
    TestSet(TestSetLocation),
    TestCase(TestCaseLocation),
}

#[derive(Clone)]
pub struct TestSetLocation {
    run_id: u32,
    test_set_id: u32,
}

#[derive(Clone)]
pub struct TestCaseLocation {
    run_id: u32,
    test_set_id: u32,
    test_case_id: TestCaseId,
}

impl ReplayExec {
    pub fn test_set(run_id: u32, test_set_id: u32) -> Self {
        Self::TestSet(TestSetLocation {
            run_id,
            test_set_id,
        })
    }

    pub fn test_case(run_id: u32, test_set_id: u32, test_case_id: TestCaseId) -> Self {
        Self::TestCase(TestCaseLocation {
            run_id,
            test_set_id,
            test_case_id,
        })
    }
}

pub async fn replay_exec(replay_exec: &ReplayExec, config: &LancerConfig) {
    match replay_exec {
        ReplayExec::TestSet(location) => replay_exec_test_set(location, config).await,
        ReplayExec::TestCase(location) => replay_exec_test_case(location, config).await,
    }
}

fn create_replay_dir<T>(location: T, config: &LancerConfig) {}

async fn replay_exec_test_set(location: &TestSetLocation, config: &LancerConfig) {
    let Ok(test_sets) = load_yaml_array::<TestSet>(&config.path_config.test_sets(location.run_id))
    else {
        panic!("Unable to load test sets for run {}", location.run_id);
    };
    let Some(test_set) = test_sets
        .into_iter()
        .find(|set| set.id == location.test_set_id)
    else {
        panic!("Unable to load test set {}", location.test_set_id);
    };
    log::info!(
        "Loaded test set {} in run {}.",
        location.test_set_id,
        location.run_id
    );

    // create replay folder
    let replay_results_path = config
        .path_config
        .test_run(location.run_id)
        .join("replay_results");
    if replay_results_path.exists() {
        log::info!("Deleting existing files in path: {:?}", replay_results_path);
        fs::remove_dir_all(&replay_results_path).unwrap();
    }
    fs::create_dir(&replay_results_path).unwrap();
    // update test set
    let origin_result_path = replay_results_path.join(format!("result-origin.csv"));
    let new_sink = Sink::csv_file(&origin_result_path, false);
    let mut query = test_set.origin.query.clone();
    query.set_sink(new_sink);
    let origin = TestCase {
        id: test_set.origin.id(),
        query,
        result_path: origin_result_path,
    };

    let mut others = Vec::new();
    for (index, test_case) in test_set.others.into_iter().enumerate() {
        let result_path = replay_results_path.join(format!("result-other{index}.csv"));
        let new_sink = Sink::csv_file(&result_path, false);
        let mut query = test_case.query.clone();
        query.set_sink(new_sink);
        let updated_test_case = TestCase {
            id: test_case.id(),
            query,
            result_path,
        };
        others.push(updated_test_case);
    }

    let updated_test_set = TestSet {
        id: test_set.id,
        strategy: test_set.strategy,
        origin,
        others,
    };
    let test_set_exec = process_test_set(location.run_id, updated_test_set, config).await;
    check_test_set(&test_set_exec);
}

async fn replay_exec_test_case(location: &TestCaseLocation, config: &LancerConfig) {
    let Ok(test_sets) = load_yaml_array::<TestSet>(&config.path_config.test_sets(location.run_id))
    else {
        panic!("Unable to load test sets for run {}", location.run_id);
    };
    let Some(test_set) = test_sets
        .into_iter()
        .find(|set| set.id == location.test_set_id)
    else {
        panic!("Unable to load test set {}", location.test_set_id);
    };
    let Some(test_case) = test_set.test_case(location.test_case_id) else {
        panic!("Unable to load test case {}", location.test_case_id);
    };
    log::info!(
        "Loaded test set {} in run {}.",
        location.test_set_id,
        location.run_id
    );
    // create new dir
    let replay_results_path = config
        .path_config
        .test_run(location.run_id)
        .join("replay_results");
    if replay_results_path.exists() {
        log::info!("Deleting existing files in path: {:?}", replay_results_path);
        fs::remove_dir_all(&replay_results_path).unwrap();
    }
    fs::create_dir(&replay_results_path).unwrap();

    let result_path = replay_results_path.join(format!("result.csv"));

    let new_sink = Sink::csv_file(&result_path, false);
    let mut query = test_case.query.clone();
    query.set_sink(new_sink);
    let updated_test_case = TestCase {
        id: test_case.id(),
        query,
        result_path,
    };
    let test_case_exec = process_single_test_case(location.run_id, updated_test_case, config).await;
    log::info!("{:?}", test_case_exec);
}
