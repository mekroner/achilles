use std::path::PathBuf;

use crate::oracles::{query_origin_filter, query_part_filter};
use crate::query_list::{LancerQueryId, QueryExecStatus, QueryProps, TestRun};
use crate::LancerConfig;
use nes_rust_client::prelude::*;

pub fn generate_query_runs(config: &LancerConfig) -> Vec<TestRun> {
    log::info!("Started generating queries:");
    let runs = (0..2)
        .map(|run_id| generate_query_run(run_id, config))
        .collect();
    log::info!("Done generating queries.");
    runs
}

fn generate_query_run(run_id: u32, config: &LancerConfig) -> TestRun {
    let origin_path = config
        .generated_files_path
        .join(format!("result-run{run_id}-origin.csv"));
    let origin_sink = Sink::csv_file(&origin_path, false);
    let origin = QueryProps {
        lancer_query_id: LancerQueryId::Origin,
        query: query_origin_filter(origin_sink),
        result_path: PathBuf::from(origin_path),
        status: QueryExecStatus::Pending,
    };
    let others = (0..10)
        .map(|id| {
            let other_path = config
                .generated_files_path
                .join(format!("result-run{run_id}-other{id}.csv"));
            let other_sink = Sink::csv_file(&other_path, false);
            QueryProps {
                lancer_query_id: LancerQueryId::Other(id),
                query: query_part_filter(other_sink),
                result_path: PathBuf::from(other_path),
                status: QueryExecStatus::Pending,
            }
        })
        .collect();

    TestRun {
        run_id,
        origin,
        others,
    }
}
