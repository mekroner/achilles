use std::{path::PathBuf, thread, time::Duration};

use nes_runner::{
    runner::Runner,
    runner_config::{OutputIO, RunnerConfig},
};

use nes_rust_client::prelude::*;

use crate::{LancerConfig, QueryList};

pub async fn run_queries(queries: QueryList, config: &LancerConfig) -> _ {
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

    log::info!("Start coordinator and workers.");
    let mut runner = Runner::new(runner_config);
    runner.start_coordinator();
    thread::sleep(Duration::from_secs(2));
    runner.start_worker();
    thread::sleep(Duration::from_secs(1));
    if runner.health_check().is_ok() {
        log::info!(" Still running");
    }
    let runtime = NebulaStreamRuntime::new("127.0.0.1", 8081);
    let sources = runtime.logical_sources().await;
    log::info!("Logical Sources: {:?}", sources);

    for i in queries.entries.iter() {
        log::info!("Generate {i} of {queries_to_gen} query pairs.");
        let (query0, query1) = query_filter();

        log::info!("Connection status: {:?}", runtime.check_connection().await);
        log::info!("Execute queries.");

        let result0 = runtime
            .execute_query(&query0, PlacementStrategy::BottomUp)
            .await;
        log::info!("Query0 response: {:?}", result0);

        let result1 = runtime
            .execute_query(&query1, PlacementStrategy::BottomUp)
            .await;
        log::info!("Query1 response: {:?}", result1);
        thread::sleep(Duration::from_secs(5));

        match (result0, result1) {
            (Ok(id0), Ok(id1)) => {
                current_queries.push(id0);
                current_queries.push(id1);
            }
            _ => {
                log::error!("Failed to execute query. Stopping nes runner.");
                runner.stop_all();
                return;
            }
        }
    }

    while !are_all_queries_stopped(&runtime, &current_queries).await {
        log::info!("Waiting for queries to stop execution...");
        thread::sleep(Duration::from_secs(5));
    }

    runner.stop_all();
    log::info!("Stopped coordinator and workers.");
}

async fn are_all_queries_stopped(runtime: &NebulaStreamRuntime, query_ids: &[i64]) -> bool {
    let mut all_stopped = true;
    for &id in query_ids {
        let Ok(Some(status)) = runtime.query_status(id).await else {
            panic!("Failed to check if query with id {id} has stopped!");
        };
        log::debug!("Query with id {id } has status {status}.");
        if status != "STOPPED" {
            all_stopped = false;
        }
    }
    return all_stopped;
}
