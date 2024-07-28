use std::{
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

use nes_runner::{
    runner::Runner,
    runner_config::{OutputIO, RunnerConfig},
};

use nes_rust_client::prelude::*;

use crate::{LancerConfig, QueryList, QueryListEntry};

pub async fn run_queries(query_list: &mut QueryList, config: &LancerConfig) {
    let runner_config = RunnerConfig {
        coordinator_exec_path: "../../nebulastream/build/nes-coordinator/nesCoordinator".into(),
        worker_exec_path: "../../nebulastream/build/nes-worker/nesWorker".into(),
        coordinator_config_path: Some(
            PathBuf::new()
                .join(&config.generated_files_path)
                .join("coordinator.yml"),
        ),
        worker_config_path: Some(
            PathBuf::new()
                .join(&config.generated_files_path)
                .join("workers/worker-0.yml"),
        ),
        output_io: OutputIO::Null,
    };

    let mut runner = Runner::new(runner_config);
    for (run_id, entry) in query_list.entries.iter_mut().enumerate() {
        log::info!("Run queries in run {run_id}.");
        execute_query_run(entry, &mut runner, config).await;
    }
}

async fn execute_query_run(entry: &mut QueryListEntry, runner: &mut Runner, config: &LancerConfig) {
    log::debug!("Start coordinator and workers.");
    runner.start_coordinator();
    thread::sleep(Duration::from_secs(2));
    runner.start_worker();
    thread::sleep(Duration::from_secs(1));
    if let Err(err) = runner.health_check() {
        log::warn!("Stopping Runner due to {err}");
        runner.stop_all();
        return;
    }
    let runtime = NebulaStreamRuntime::new("127.0.0.1", 8000);
    let sources = runtime.logical_sources().await;
    log::info!("Logical Sources: {:?}", sources);

    let queries = std::iter::once(&mut entry.origin).chain(entry.others.iter_mut());
    for props in queries {
        let result = runtime
            .execute_query(&props.query, PlacementStrategy::BottomUp)
            .await;
        match result {
            Ok(id) => {
                props.query_id = Some(id);
                props.start_time = Some(Instant::now());
            }
            Err(err) => log::warn!("Could not execute Query: {err}"),
        }
        if let Err(err) = runner.health_check() {
            log::warn!("Stopping Runner due to {err}");
            runner.stop_all();
            return;
        }
    }

    while !are_all_queries_stopped(&runtime, &entry, &config).await {
        log::info!("Waiting for queries to stop execution...");
        thread::sleep(Duration::from_secs(5));
    }

    runner.stop_all();
    log::info!("Stopped coordinator and workers.");
}

async fn are_all_queries_stopped(
    runtime: &NebulaStreamRuntime,
    entry: &QueryListEntry,
    config: &LancerConfig,
) -> bool {
    let mut all_stopped = true;
    let queries = std::iter::once(&entry.origin).chain(entry.others.iter());
    for props in queries {
        let Some(id) = props.query_id else { continue };
        let Some(time) = props.start_time else {
            continue;
        };
        let Ok(Some(status)) = runtime.query_status(id).await else {
            log::warn!("Failed to check if query with id {id} has stopped!");
            continue;
        };
        log::debug!("Query with id {id } has status {status}.");
        if status != "STOPPED" {
            if timeout(time, config.max_query_exec_duration) {
                log::warn!("Query {id} has timed out {:?}!", time.elapsed());
            } else {
                all_stopped = false;
            }
        }
    }
    return all_stopped;
}

fn timeout(start_time: Instant, duration: Duration) -> bool {
    duration < start_time.elapsed()
}
