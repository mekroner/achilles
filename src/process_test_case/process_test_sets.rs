use std::{
    thread,
    time::{Duration, Instant},
};

use nes_rust_client::{
    query::time::Duration,
    runtime::{
        nebula_stream_runtime::{NebulaStreamRuntime, PlacementStrategy},
        query_state::QueryState,
    },
};

use crate::{
    process_test_case,
    runner::runner::Runner,
    test_case_exec::{TestCaseExec, TestCaseExecStatus, TestSetExec},
    test_case_gen::test_case::{TestCase, TestSet},
    LancerConfig,
};

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
    let sleep_duration = Duration::from_secs(2);

    //setup runner
    let mut runner_config = config.runner_config.clone();
    runner_config.coordinator_config_path =
        Some(config.path_config.coordinator_config(test_run_id));
    runner_config.worker_config_path = Some(config.path_config.worker_configs(test_run_id));
    let runner = Runner::new(runner_config);

    let runtime = connect_runtime();
    let origin = process_test_case_with_pre_check(
        &runtime,
        &mut runner,
        test_set.origin,
        &config.test_case_timeout,
    )
    .await;
    let mut others = Vec::new();
    for other in test_set.others.into_iter() {
        let other_exec = process_test_case_with_pre_check(
            &runtime,
            &mut runner,
            other,
            &config.test_case_timeout,
        )
        .await;
        others.push(other_exec);
        thread::sleep(sleep_duration);
    }
    runner.stop_all();
    TestSetExec {
        id: test_set.id,
        origin,
        others,
    }
}

/// returns false if the pre check encountered an error, that is if the runner encountered an error
/// or if the runtime was unable to connect
// FIXME: actually do something
async fn pre_check(runner: &mut Runner, runtime: &NebulaStreamRuntime) -> bool {
    todo!()
}

async fn process_test_case_with_pre_check(
    runtime: &NebulaStreamRuntime,
    runner: &mut Runner,
    test_case: TestCase,
    timeout_duration: &Duration,
) -> TestCaseExec {
    if !pre_check(runner, &runtime).await {
        return TestCaseExec::from_with(test_case, TestCaseExecStatus::Skipped);
    }
    process_test_case(&runtime, runner, test_case, &timeout_duration).await
}

async fn process_test_case(
    runtime: &NebulaStreamRuntime,
    runner: &mut Runner,
    test_case: TestCase,
    timeout_duration: &Duration,
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

        let is_timeout = start_time.elapsed() > *timeout_duration;
        if is_timeout {
            log::warn!("Failed to execute test case {}: Timed out.", test_case.id);
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::TimedOut);
        }
    }
}
