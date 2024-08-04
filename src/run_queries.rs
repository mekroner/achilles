use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    query_gen::test_case::{QueryProps, TestCase},
    runner::runner::Runner,
    test_case_exec::{QueryExecProps, QueryExecStatus, TestCaseExec},
};
use nes_rust_client::prelude::*;

use crate::LancerConfig;

pub async fn process_test_cases(config: &LancerConfig, cases: Vec<TestCase>) -> Vec<TestCaseExec> {
    let start_time = Instant::now();
    let mut results = Vec::new();
    for run in cases.into_iter() {
        log::debug!("Starting test case {}.", run.id);
        let result = process_test_run(run, config).await;
        results.push(result);
    }
    log::info!("All test runs done in {:?}.", start_time.elapsed());
    results
}

async fn process_test_run(run: TestCase, config: &LancerConfig) -> TestCaseExec {
    let runner = Arc::new(Mutex::new(Runner::new(config.runner_config.clone())));
    if let Err(err) = runner.lock().unwrap().start_all() {
        log::error!("Failed to start runner: {}", err);
        runner.lock().unwrap().stop_all();
        panic!();
        //TODO: Handle runner error
    }
    let runtime = Arc::new(NebulaStreamRuntime::new("127.0.0.1", 8000));
    if !runtime.check_connection().await {
        log::error!("Connection to runtime failed in run {}", run.id);
        runner.lock().unwrap().stop_all();
        panic!();
        //TODO: Handle runtime error
    }

    let runner_clone = runner.clone();
    let runtime_clone = runtime.clone();
    let timeout_duration = config.max_query_exec_duration.clone();
    let origin = run.origin;
    let origin_task = tokio::spawn(async move {
        process_query(&runtime_clone, runner_clone, origin, timeout_duration).await
    });

    let mut other_tasks = Vec::new();
    for props in run.others.into_iter() {
        let runner_clone = runner.clone();
        let runtime_clone = runtime.clone();
        let timeout_duration = config.max_query_exec_duration.clone();
        let handle = tokio::spawn(async move {
            process_query(&runtime_clone, runner_clone, props, timeout_duration).await
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

    TestCaseExec {
        id: run.id,
        origin,
        others,
    }
}

async fn process_query(
    runtime: &NebulaStreamRuntime,
    runner: Arc<Mutex<Runner>>,
    props: QueryProps,
    timeout_duration: Duration,
) -> QueryExecProps {
    let Ok(id) = runtime
        .execute_query(&props.query, PlacementStrategy::BottomUp)
        .await
    else {
        log::warn!(
            "Failed to execute query {}: Unable to register Query",
            props.lancer_query_id
        );
        return QueryExecProps::from_with(props, QueryExecStatus::Failed);
    };
    log::trace!(
        "Success registering query {} with nes id {id}",
        props.lancer_query_id
    );

    let start_time = Instant::now();
    // Wait for query to stop
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        log::trace!("Checking if query {} has stopped", props.lancer_query_id);
        let Ok(Some(status)) = runtime.query_status(id).await else {
            log::warn!(
                "Failed to execute query {}: Query was not registered.",
                props.lancer_query_id
            );
            return QueryExecProps::from_with(props, QueryExecStatus::Failed);
        };
        if status == QueryState::Stopped {
            log::info!("Executed query {} successful.", props.lancer_query_id);
            return QueryExecProps::from_with(props, QueryExecStatus::Success);
        }

        if runner.lock().unwrap().health_check().is_err() {
            log::warn!(
                "Failed to execute query {}: Nebula Stream Crashed.",
                props.lancer_query_id
            );
            return QueryExecProps::from_with(props, QueryExecStatus::Failed);
        }

        let is_timeout = start_time.elapsed() > timeout_duration;
        if is_timeout {
            log::warn!(
                "Failed to execute query {}: Timed out.",
                props.lancer_query_id
            );
            return QueryExecProps::from_with(props, QueryExecStatus::TimedOut);
        }
    }
}
