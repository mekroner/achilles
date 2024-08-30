use std::{
    os::unix::process,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    runner::{
        runner::Runner,
        runner_config::{self, RunnerConfig},
    },
    test_case_exec::{self, TestCaseExec, TestCaseExecStatus, TestSetExec},
    test_case_gen::test_case::{TestCase, TestSet},
};
use nes_rust_client::prelude::*;

use crate::LancerConfig;

pub async fn process_test_sets(
    test_run_id: u32,
    config: &LancerConfig,
    test_sets: Vec<TestSet>,
) -> Vec<TestSetExec> {
    let start_time = Instant::now();
    let mut results = Vec::new();
    for test_set in test_sets.into_iter() {
        log::debug!("Starting test set {}.", test_set.id);
        let result = process_test_set(test_run_id, test_set, config).await;
        results.push(result);
    }
    log::info!("All test sets done in {:?}.", start_time.elapsed());
    results
}

pub async fn process_test_set(
    test_run_id: u32,
    test_set: TestSet,
    config: &LancerConfig,
) -> TestSetExec {
    let mut runner_config = config.runner_config.clone();
    runner_config.coordinator_config_path =
        Some(config.path_config.coordinator_config(test_run_id));
    runner_config.worker_config_path = Some(config.path_config.worker_configs(test_run_id));
    let runner = Arc::new(Mutex::new(Runner::new(runner_config)));
    if let Err(err) = runner.lock().unwrap().start_all() {
        log::error!("Failed to start runner: {}", err);
        runner.lock().unwrap().stop_all();
        //TODO: Handle runner error
        panic!();
    }

    let retry_attempts = 5;
    let runtime = Arc::new(NebulaStreamRuntime::new("127.0.0.1", 8000));
    for i in 1..=retry_attempts {
        let is_connected = runtime.check_connection().await;
        if is_connected {
            log::debug!("Connection established for test set {}.", test_set.id);
            break;
        }
        if i == retry_attempts {
            log::error!("Connection to runtime failed for test set {}!", test_set.id);
            runner.lock().unwrap().stop_all();
            //TODO: Handle runtime error
            panic!();
        }
    }

    let runner_clone = runner.clone();
    let runtime_clone = runtime.clone();
    let timeout_duration = config.test_case_timeout.clone();
    let origin = test_set.origin;
    let origin_task = tokio::spawn(async move {
        process_test_case(&runtime_clone, runner_clone, origin, timeout_duration).await
    });

    let mut other_tasks = Vec::new();
    for test_case in test_set.others.into_iter() {
        let runner_clone = runner.clone();
        let runtime_clone = runtime.clone();
        let timeout_duration = config.test_case_timeout.clone();
        let handle = tokio::spawn(async move {
            process_test_case(&runtime_clone, runner_clone, test_case, timeout_duration).await
        });
        other_tasks.push(handle);
    }

    //TODO:: Await all tasks and collect the results
    let Ok(origin) = origin_task.await else {
        panic!("Failed to join");
    };

    let mut others = vec![];
    for task in other_tasks {
        if let Ok(props) = task.await {
            others.push(props);
        }
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    runner.lock().unwrap().stop_all();

    TestSetExec {
        id: test_set.id,
        origin,
        others,
    }
}

pub async fn process_single_test_case(
    run_id: u32,
    test_case: TestCase,
    config: &LancerConfig,
) -> TestCaseExec {
    let mut runner_config = config.runner_config.clone();
    runner_config.coordinator_config_path =
        Some(config.path_config.coordinator_config(run_id));
    runner_config.worker_config_path = Some(config.path_config.worker_configs(run_id));
    let runner = Arc::new(Mutex::new(Runner::new(runner_config)));
    if let Err(err) = runner.lock().unwrap().start_all() {
        log::error!("Failed to start runner: {}", err);
        runner.lock().unwrap().stop_all();
        //TODO: Handle runner error
        panic!();
    }

    let retry_attempts = 5;
    let runtime = Arc::new(NebulaStreamRuntime::new("127.0.0.1", 8000));
    for i in 1..=retry_attempts {
        let is_connected = runtime.check_connection().await;
        if is_connected {
            log::debug!("Connection established.");
            break;
        }
        if i == retry_attempts {
            log::error!("Connection to runtime failed!");
            runner.lock().unwrap().stop_all();
            //TODO: Handle runtime error
            panic!();
        }
    }

    let timeout_duration = config.test_case_timeout.clone();
    let test_case_exec = process_test_case(&runtime, runner.clone(), test_case, timeout_duration).await;

    tokio::time::sleep(Duration::from_secs(5)).await;
    runner.lock().unwrap().stop_all();
    test_case_exec
}

async fn process_test_case(
    runtime: &NebulaStreamRuntime,
    runner: Arc<Mutex<Runner>>,
    test_case: TestCase,
    timeout_duration: Duration,
) -> TestCaseExec {
    let response = runtime
        .execute_query(&test_case.query, PlacementStrategy::BottomUp)
        .await;
    let id = match response {
        Ok(id) => id,
        Err(err) => {
            log::warn!(
                "Failed to execute test case {}: Unable to register Query: {err}",
                test_case.id
            );
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::Failed);
        }
    };
    log::trace!(
        "Success registering query for test case {} with query id {id}.",
        test_case.id
    );

    let start_time = Instant::now();
    // Wait for query to stop
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        // first check if nes is still healthy
        // TODO: Improve Check if runner is healthy
        if !runner.lock().unwrap().health_check().unwrap().all_running() {
            log::warn!(
                "Failed to execute test_case {}: NebulaStream Crashed.",
                test_case.id
            );
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::Failed);
        }

        // then get query state
        log::trace!("Checking if test_case {} has stopped", test_case.id);
        let Ok(Some(status)) = runtime.query_status(id).await else {
            log::warn!(
                "Failed to execute test_case {}: Unable to get query state.",
                test_case.id
            );
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::Failed);
        };

        if status == QueryState::Stopped {
            log::info!("Executed test case {} successful.", test_case.id);
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::Success);
        }

        let is_timeout = start_time.elapsed() > timeout_duration;
        if is_timeout {
            log::warn!("Failed to execute test case {}: Timed out.", test_case.id);
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::TimedOut);
        }
    }
}
