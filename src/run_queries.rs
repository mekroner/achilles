use std::{
    path::PathBuf,
    result, thread,
    time::{Duration, Instant},
};

use nes_runner::{
    runner::Runner,
    runner_config::{OutputIO, RunnerConfig},
};

use nes_rust_client::prelude::*;

use crate::{
    query_list::{QueryProps, QueryRun, QueryRunResult},
    LancerConfig, QueryList, QueryListEntry,
};

pub struct QueryExecProps {
    pub nes_query_id: i64,
    pub start_time: Instant,
}

pub async fn run_queries(query_runs: &[QueryRun], config: &LancerConfig) -> Vec<QueryRunResult> {
    let mut run_results = Vec::new();
    let mut runner = Runner::new(config.runner_config.clone());
    for run in query_runs {
        log::info!("Run queries in run {}.", run.run_id);
        let run_result = execute_query_run(run, &mut runner, config).await;
        run_results.push(run_result);
    }
    run_results
}

fn query_run_result_skipped_all(query_run: &QueryRun) -> QueryRunResult {
    todo!();
}

async fn execute_query_run(
    query_run: &QueryRun,
    runner: &mut Runner,
    config: &LancerConfig,
) -> QueryRunResult {
    //setup runner
    log::debug!("Start coordinator and workers.");
    if let Err(err) = runner.start_all() {
        log::error!("Failed to start runner due to {err}");
        runner.stop_all();
        return query_run_result_skipped_all(query_run);
    }

    //setup runtime
    let runtime = NebulaStreamRuntime::new("127.0.0.1", 8000);
    if !runtime.check_connection().await {
        log::error!("Runtime unable to connect to runner.");
        runner.stop_all();
        return query_run_result_skipped_all(query_run);
    };

    let mut exec_props = Vec::new();
    let queries = std::iter::once(&query_run.origin).chain(query_run.others.iter());
    for props in queries {
        let result = execute_single_query(&runtime, props).await;
        match result {
            Ok(exec_prop) => exec_props.push(exec_prop),
            Err(_) => todo!(),
        }
    }

    // wait for queries to finish execution
    while !all_queries_stopped(&runtime, &exec_props, &config).await {
        log::info!("Waiting for queries to stop execution...");
        thread::sleep(Duration::from_secs(5));
    }

    runner.stop_all();
    log::info!("Stopped coordinator and workers.");
}

enum SingleExecutionError {
    QueryExecutionFailed,
    ConnectionFailed,
    NesCrashed,
}

async fn execute_single_query(
    runtime: &NebulaStreamRuntime,
    props: &QueryProps,
) -> Result<QueryExecProps, SingleExecutionError> {
    let result = runtime
        .execute_query(&props.query, PlacementStrategy::BottomUp)
        .await;
    return match result {
        Ok(id) => Ok(QueryExecProps {
            nes_query_id: id,
            start_time: Instant::now(),
        }),
        Err(err) => {
            log::warn!("Could not execute Query: {err}");
            Err(SingleExecutionError::ConnectionFailed)
        }
    };

    // if let Err(err) = runner.health_check() {
    //     log::warn!("Stopping Runner due to {err}");
    //     runner.stop_all();
    //     return Err();
    // }
}

async fn all_queries_stopped(
    runtime: &NebulaStreamRuntime,
    exec_props: &[QueryExecProps],
    config: &LancerConfig,
) -> bool {
    let mut all_stopped = true;
    for props in exec_props {
        let Ok(Some(status)) = runtime.query_status(props.nes_query_id).await else {
            log::warn!(
                "Failed to check if query with id {} has stopped!",
                props.nes_query_id
            );
            continue;
        };
        log::debug!("Query with id {} has status {status}.", props.nes_query_id);
        if status != "STOPPED" {
            if is_timeout(props.start_time, config.max_query_exec_duration) {
                log::warn!(
                    "Query {} has timed out {:?}!",
                    props.nes_query_id,
                    props.start_time.elapsed()
                );
            } else {
                all_stopped = false;
            }
        }
    }
    return all_stopped;
}

fn is_timeout(start_time: Instant, duration: Duration) -> bool {
    duration < start_time.elapsed()
}
