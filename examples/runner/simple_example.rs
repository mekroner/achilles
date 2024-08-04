use std::{path::PathBuf, thread, time::Duration};

use log::LevelFilter;
use nes_runner::{runner::Runner, runner_config::RunnerConfig};
use simple_logger;

fn main() {
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();

    let config = RunnerConfig {
        coordinator_exec_path: PathBuf::from(
            "../../nebulastream/build/nes-coordinator/nesCoordinator",
        ),
        worker_exec_path: PathBuf::from("../../nebulastream/build/nes-worker/nesWorker"),
        coordinator_config_path: Some(PathBuf::from("./examples/example_configs/coordinator.yml")),
        worker_config_path: Some(PathBuf::from("./examples/example_configs/worker-1.yml")),
    };
    let mut runner = Runner::new(config);
    runner.start_coordinator();
    thread::sleep(Duration::from_secs(1));
    runner.start_worker();
    thread::sleep(Duration::from_secs(5));
    if runner.health_check().is_ok() {
        log::info!("Health is good!");
    }
    runner.stop_all();
}
