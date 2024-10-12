use std::{
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use nes_rust_client::runtime::{
    nebula_stream_runtime::{NebulaStreamRuntime, PlacementStrategy},
    query_state::QueryState,
};

use crate::{
    runner::runner::Runner,
    test_case_exec::{TestCaseExec, TestCaseExecStatus, TestSetExec},
    test_case_gen::test_case::{self, TestCase, TestSet},
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

fn files_in_dir(path: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if !path.is_dir() {
        log::warn!("{:?} is not a dir.", path);
        return files;
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        log::warn!("Failed to read {:?}", path);
        return files;
    };
    for entry in entries {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            }
        }
    }
    files
}

pub async fn process_single_test_case(
    test_run_id: u32,
    test_case: TestCase,
    config: &LancerConfig,
) -> TestCaseExec {
    let sleep_duration = Duration::from_secs(2);

    //setup runner
    // FIXME: This should be nicer
    let mut runner_config = config.runner_config.clone();
    runner_config.coordinator_config_path =
        Some(config.path_config.coordinator_config(test_run_id));
    runner_config.worker_config_path =
        files_in_dir(&config.path_config.worker_configs(test_run_id));
    let mut runner = Runner::new(runner_config);
    runner.start_all();

    //setup runtime
    let runtime = NebulaStreamRuntime::new(
        config.net_config.coord_ip.to_string(),
        config.net_config.coord_rest_port.into(),
    );

    let test_case_exec = process_test_case_with_pre_check(
        &runtime,
        &mut runner,
        test_case,
        &config.test_case_timeout,
    )
    .await;
    runner.stop_all();
    test_case_exec
}

pub async fn process_test_set(
    test_run_id: u32,
    test_set: TestSet,
    config: &LancerConfig,
) -> TestSetExec {
    let sleep_duration = Duration::from_secs(2);

    //setup runner
    // FIXME: This should be nicer
    let mut runner_config = config.runner_config.clone();
    runner_config.coordinator_config_path =
        Some(config.path_config.coordinator_config(test_run_id));
    runner_config.worker_config_path =
        files_in_dir(&config.path_config.worker_configs(test_run_id));
    let mut runner = Runner::new(runner_config);
    runner.start_all();

    //setup runtime
    let runtime = NebulaStreamRuntime::new("127.0.0.1", 8000);

    // run test cases
    let origin = process_test_case_with_pre_check(
        &runtime,
        &mut runner,
        test_set.origin,
        &config.test_case_timeout,
    )
    .await;
    post_check_restart(&origin, &mut runner).await;
    let mut others = Vec::new();
    for other in test_set.others.into_iter() {
        let other_exec = process_test_case_with_pre_check(
            &runtime,
            &mut runner,
            other,
            &config.test_case_timeout,
        )
        .await;
        post_check_restart(&other_exec, &mut runner).await;
        others.push(other_exec);
        thread::sleep(sleep_duration);
    }
    // clean up
    runner.stop_all();
    TestSetExec {
        id: test_set.id,
        origin,
        others,
    }
}

/// returns false if the pre check encountered an error, that is if the runner encountered an error
/// or if the runtime was unable to connect. If everything is working as expected this function
/// returns true
// FIXME: actually do something
async fn pre_check(runner: &mut Runner, runtime: &NebulaStreamRuntime) -> bool {
    if !runner.health_check().unwrap().all_running() {
        log::warn!("NebulaStream crashed.");
        return false;
    }
    if !runtime.check_connection().await {
        log::warn!("Unable to connect to NebulaStream.");
        return false;
    }
    true
}

/// check if the exec was correct, else restart the runner.
async fn post_check_restart(exec: &TestCaseExec, runner: &mut Runner) {
    if exec.status == TestCaseExecStatus::Success {
        return;
    }
    runner.stop_all();
    runner.start_all();
}

async fn process_test_case_with_pre_check(
    runtime: &NebulaStreamRuntime,
    runner: &mut Runner,
    test_case: TestCase,
    timeout_duration: &Duration,
) -> TestCaseExec {
    if !pre_check(runner, &runtime).await {
        log::warn!("Skipping test case.");
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
            let error_str = format!("Unable to register query: {err}");
            log::warn!("Failed to execute test case {}: {error_str}", test_case.id);
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::Failed(error_str));
        }
    };
    log::trace!(
        "Success registering query for test case {} with query id {id}.",
        test_case.id
    );

    let start_time = Instant::now();
    // Wait for query to stop
    loop {
        thread::sleep(Duration::from_secs(2));
        // first check if nes is still healthy
        // TODO: Improve Check if runner is healthy
        let runner_status = runner.health_check().unwrap();
        if !runner_status.all_running() {
            log::warn!(
                "Failed to execute test_case {}: NebulaStream Crashed.",
                test_case.id
            );
            let error_str = format!("NES crashed: {:?}", runner_status.collect_errors());
            log::warn!("RunnerStatus: {:?}", runner_status.collect_errors());
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::Failed(error_str));
        }

        // then get query state
        log::trace!("Checking if test_case {} has stopped", test_case.id);
        let Ok(Some(status)) = runtime.query_status(id).await else {
            log::warn!(
                "Failed to execute test_case {}: Unable to get query state.",
                test_case.id
            );
            return TestCaseExec::from_with(
                test_case,
                TestCaseExecStatus::Failed("Unable to get query state.".into()),
            );
        };

        if status == QueryState::Stopped {
            log::info!("Executed test case {} successful.", test_case.id);
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::Success);
        }

        let is_timeout = start_time.elapsed() > *timeout_duration;
        if is_timeout {
            log::warn!("Failed to execute test case {}: Timed out.", test_case.id);
            // To stop the query is apparently not effective because nebula stream is stuck
            // if let Err(err) = runtime.stop_query(id).await {
            //     log::error!(
            //         "Failed to stop test_case {} after time out: {err}",
            //         test_case.id
            //     );
            // }
            return TestCaseExec::from_with(test_case, TestCaseExecStatus::TimedOut);
        }
    }
}
