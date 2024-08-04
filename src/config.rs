use std::{path::PathBuf, time::Duration};

use crate::{runner::runner_config::{OutputIO, RunnerConfig}, stages::Stages};

pub struct LancerConfig {
    pub generated_files_path: PathBuf,
    pub max_query_exec_duration: Duration,
    pub runner_config: RunnerConfig,
    pub skip_to_stage: Stages,
}

impl Default for LancerConfig {
    fn default() -> Self {
        let generated_files_path = PathBuf::from("./generated_files");
        let runner_config = RunnerConfig {
            coordinator_exec_path: "../../nebulastream/build/nes-coordinator/nesCoordinator".into(),
            worker_exec_path: "../../nebulastream/build/nes-worker/nesWorker".into(),
            coordinator_config_path: Some(
                PathBuf::new()
                    .join(&generated_files_path)
                    .join("coordinator.yml"),
            ),
            worker_config_path: Some(
                PathBuf::new()
                    .join(&generated_files_path)
                    .join("workers/worker-0.yml"),
            ),
            output_io: OutputIO::Null,
        };
        LancerConfig {
            generated_files_path,
            max_query_exec_duration: Duration::from_secs(10),
            runner_config,
            skip_to_stage: Stages::default(),
        }
    }
}
