use std::{
    net::Ipv4Addr,
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{
    nes_opt_config::NesOptConfig,
    nes_query_comp_config::NesQueryCompilerConfig,
    runner::runner_config::{OutputIO, RunnerConfig},
    stages::Stages,
    test_case_gen::oracle::QueryGenStrategy,
};

pub struct TestConfig {
    pub oracles: Vec<QueryGenStrategy>,
    pub test_run_count: u32,
    pub oracle_reps: u32,
    pub test_case_count: u32,
    pub field_count: u32,
    pub record_count: u32,
    pub physical_source_count: u32,
    pub predicate_depth: u32,
}

pub struct LancerConfig {
    pub path_config: FilePathConfig,
    pub test_case_timeout: Duration,
    pub runner_config: RunnerConfig,
    pub skip_to_stage: Stages,
    pub test_config: TestConfig,
    pub net_config: NetworkConfig,
    pub opt_config: NesOptConfig,
    pub query_comp_config: NesQueryCompilerConfig,
}

#[derive(Clone)]
pub struct NetworkConfig {
    pub coord_ip: Ipv4Addr,
    pub coord_rest_port: u16,
    pub coord_rpc_port: u16,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            coord_ip: Ipv4Addr::LOCALHOST,
            coord_rest_port: 8000,
            coord_rpc_port: 4000,
        }
    }
}

impl Default for TestConfig {
    fn default() -> Self {
        TestConfig {
            oracles: vec![
                // QueryGenStrategy::Filter,
                // QueryGenStrategy::Map,
                // QueryGenStrategy::AggMin,
                // QueryGenStrategy::AggMax,
                QueryGenStrategy::AggSum,
                // QueryGenStrategy::AggCount,
                // QueryGenStrategy::AggAvg,
                // QueryGenStrategy::KeyAggMin
                // QueryGenStrategy::KeyAggMax
                // QueryGenStrategy::KeyAggSum
                // QueryGenStrategy::KeyAggCount,
                // QueryGenStrategy::KeyAggAvg,
                // QueryGenStrategy::WinPartMin,
                // QueryGenStrategy::WinPartMax,
                // QueryGenStrategy::WinPartSum,
                // QueryGenStrategy::WinPartCount,
                // QueryGenStrategy::WinPartAvg,
            ],
            field_count: 10,
            record_count: 500,
            physical_source_count: 5,
            test_run_count: 1,
            oracle_reps: 2,
            test_case_count: 5,
            predicate_depth: 3,
        }
    }
}

impl Default for LancerConfig {
    fn default() -> Self {
        let runner_config = RunnerConfig {
            coordinator_exec_path: "../../nebulastream/build/nes-coordinator/nesCoordinator".into(),
            worker_exec_path: "../../nebulastream/build/nes-worker/nesWorker".into(),
            coordinator_config_path: None,
            worker_config_path: Vec::new(),
            output_io: OutputIO::Null,
        };

        LancerConfig {
            // generated_files_path,
            path_config: FilePathConfig::default(),
            test_case_timeout: Duration::from_secs(20),
            runner_config,
            skip_to_stage: Stages::default(),
            test_config: TestConfig::default(),
            net_config: NetworkConfig::default(),
            opt_config: NesOptConfig::default(),
            query_comp_config: NesQueryCompilerConfig::default(),
        }
    }
}

pub struct FilePathConfig {
    pub base: PathBuf,
    pub test_run: PathBuf,
    pub stream_config: PathBuf,
    pub results: PathBuf,
    pub coordinator_config_file: PathBuf,
    pub worker_configs: PathBuf,
    pub test_sets_file: PathBuf,
    pub test_set_execs_file: PathBuf,
    pub test_set_results_file: PathBuf,
}

impl Default for FilePathConfig {
    fn default() -> Self {
        Self {
            base: PathBuf::from("generated-files"),
            test_run: PathBuf::from("test-run"),
            stream_config: PathBuf::from("stream-config"),
            results: PathBuf::from("results"),
            coordinator_config_file: PathBuf::from("coordinator.yml"),
            worker_configs: PathBuf::from("workers"),
            test_sets_file: PathBuf::from("test_sets.yml"),
            test_set_execs_file: PathBuf::from("test_set_execs.yml"),
            test_set_results_file: PathBuf::from("test_set_results.yml"),
        }
    }
}

impl FilePathConfig {
    pub fn base(&self) -> &Path {
        &self.base
    }

    pub fn test_run(&self, test_run_id: u32) -> PathBuf {
        let name = format!("{}-{}", self.test_run.display(), test_run_id);
        self.base.join(name)
    }

    pub fn result(&self, test_run_id: u32) -> PathBuf {
        self.test_run(test_run_id).join(&self.results)
    }

    pub fn worker_configs(&self, test_run_id: u32) -> PathBuf {
        self.test_run(test_run_id).join(&self.worker_configs)
    }

    pub fn coordinator_config(&self, test_run_id: u32) -> PathBuf {
        self.test_run(test_run_id)
            .join(&self.coordinator_config_file)
    }

    pub fn test_sets(&self, test_run_id: u32) -> PathBuf {
        self.test_run(test_run_id).join(&self.test_sets_file)
    }

    pub fn test_set_execs(&self, test_run_id: u32) -> PathBuf {
        self.test_run(test_run_id).join(&self.test_set_execs_file)
    }

    pub fn test_set_results(&self, test_run_id: u32) -> PathBuf {
        self.test_run(test_run_id).join(&self.test_set_results_file)
    }
}

