use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    query_gen::test_case::{TestCase, TestSet},
    runner::runner::Runner,
    test_case_exec::{TestCaseExec, TestCaseExecStatus, TestSetExec},
};
use nes_rust_client::prelude::*;

use crate::LancerConfig;

pub async fn process_test_sets(config: &LancerConfig, test_sets: Vec<TestSet>) -> Vec<TestSetExec> {
    let start_time = Instant::now();
    let mut results = Vec::new();
    for test_set in test_sets.into_iter() {
        log::debug!("Starting test set {}.", test_set.id);
        let result = process_test_set(test_set, config).await;
        results.push(result);
    }
    log::info!("All test sets done in {:?}.", start_time.elapsed());
    results
}

async fn process_test_set(test_set: TestSet, config: &LancerConfig) -> TestSetExec {
    let runner = Arc::new(Mutex::new(Runner::new(config.runner_config.clone())));
    if let Err(err) = runner.lock().unwrap().start_all() {
        log::error!("Failed to start runner: {}", err);
        runner.lock().unwrap().stop_all();
        //TODO: Handle runner error
        panic!();
    }
    let runtime = Arc::new(NebulaStreamRuntime::new("127.0.0.1", 8000));
    if !runtime.check_connection().await {
        log::error!("Connection to runtime failed in run {}", test_set.id);
        runner.lock().unwrap().stop_all();
        //TODO: Handle runtime error
        panic!();
    }

    let runner_clone = runner.clone();
    let runtime_clone = runtime.clone();
    let timeout_duration = config.max_query_exec_duration.clone();
    let origin = test_set.origin;
    let origin_task = tokio::spawn(async move {
        process_test_case(&runtime_clone, runner_clone, origin, timeout_duration).await
    });

    let mut other_tasks = Vec::new();
    for test_case in test_set.others.into_iter() {
        let runner_clone = runner.clone();
        let runtime_clone = runtime.clone();
        let timeout_duration = config.max_query_exec_duration.clone();
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

    runner.lock().unwrap().stop_all();

    TestSetExec {
        id: test_set.id,
        origin,
        others,
    }
}

async fn process_test_case(
    runtime: &NebulaStreamRuntime,
    runner: Arc<Mutex<Runner>>,
    test_case: TestCase,
    timeout_duration: Duration,
) -> TestCaseExec {
    let Ok(id) = runtime
        .execute_query(&test_case.query, PlacementStrategy::BottomUp)
        .await
    else {
        log::warn!(
            "Failed to execute query {}: Unable to register Query",
            test_case.id
        );
        return TestCaseExec::from_with(test_case, TestCaseExecStatus::Failed);
    };
    log::trace!(
        "Success registering query {} with nes id {id}",
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
            log::warn!(
                "Failed to execute test case {}: Timed out.",
                test_case.id
            );
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::TimedOut);
        }
    }
}
