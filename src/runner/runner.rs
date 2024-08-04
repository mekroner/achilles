use std::{
    fs::File,
    io::{self, Write},
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

use super::runner_config::{OutputIO, RunnerConfig};

pub struct Runner {
    coordinator: Option<Child>,
    workers: Vec<Child>,
    config: RunnerConfig,
}

impl Runner {
    pub fn new(config: RunnerConfig) -> Self {
        Self {
            coordinator: None,
            workers: Vec::new(),
            config,
        }
    }

    pub fn start_coordinator(&mut self) {
        let child = self.start_sub_process(
            &self.config.coordinator_exec_path,
            self.config.coordinator_config_path.as_deref(),
        );
        self.coordinator = Some(child);
    }

    pub fn start_worker(&mut self) {
        let child = self.start_sub_process(
            &self.config.worker_exec_path,
            self.config.worker_config_path.as_deref(),
        );
        self.workers.push(child);
    }

    pub fn start_all(&mut self) -> Result<(), io::Error> {
        self.start_coordinator();
        thread::sleep(Duration::from_secs(3));
        self.start_worker();
        thread::sleep(Duration::from_secs(3));
        self.health_check()
    }

    fn start_sub_process(&self, exec_path: &Path, config_path: Option<&Path>) -> Child {
        let exec_path = exec_path
            .canonicalize()
            .expect("Failed to get absolute path.");
        log::info!("Attempt starting executable in path {:?}", exec_path);
        let mut cmd = Command::new(exec_path);
        if let Some(path) = config_path {
            if let Err(err) = path.canonicalize() {
                log::error!("Failed to canonicalize config path: {err}");
                // TODO: This should return an error
                // return Err(...);
            }
            let arg = format!("--configPath={}", path.to_string_lossy());
            log::info!("with Argument: {}", &arg);
            cmd.arg(arg);
        }
        if let OutputIO::ToFile(_) = self.config.output_io {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        } else if OutputIO::Null == self.config.output_io {
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        }
        cmd.spawn().expect("Executable field should be there!")
    }

    pub fn stop_all(&mut self) {
        self.stop_workers();
        self.stop_coordinator();
    }

    pub fn stop_workers(&mut self) {
        log::info!("Attempting to stop workers:");
        let mut index = 0;
        while let Some(mut worker) = self.workers.pop() {
            worker.kill().expect("Worker should be killable!");
            self.cleanup_subprocess(worker, format!("worker{index}_output.log"));
            index += 1;
        }
        log::info!("Stopped all workers.");
    }

    pub fn stop_coordinator(&mut self) {
        log::info!("Attempting to stop coordinator:");
        let Some(mut coord) = self.coordinator.take() else {
            log::warn!("Coordinator does not exist!");
            return;
        };
        coord.kill().expect("Coordinator should be killable!");
        self.cleanup_subprocess(coord, "coordinator_output.log".into());
        log::info!("Stopped coordinator.");
    }

    fn cleanup_subprocess(&self, mut child_process: Child, file_name: String) {
        if let OutputIO::ToFile(ref file_path) = &self.config.output_io {
            let output = child_process
                .wait_with_output()
                .expect("Wait with output should not fail!");
            let mut output_file =
                File::create(file_path.join(file_name)).expect("Should not failt to create worker output file!");
            output_file
                .write_all(&output.stdout)
                .expect("Should not fail to write stdout to output file!");
            output_file
                .write_all(&output.stderr)
                .expect("Should not fail to write stderr to output file!");
            return;
        }
        child_process.wait().expect("Wait should not fail!");
    }

    pub fn health_check(&mut self) -> io::Result<()> {
        log::info!("Check coordinators health:");
        let Some(ref mut coord) = self.coordinator else {
            panic!("Coordinator should exist!");
        };
        match coord.try_wait() {
            Ok(Some(status)) => log::info!("Coordinator has exited with {}", status),
            Ok(None) => log::info!("Coordinator is running"),
            Err(err) => panic!("Coordinator should either exited or still running! {}", err),
        }

        log::info!("Check workers health:");
        for worker in self.workers.iter_mut() {
            match worker.try_wait() {
                Ok(Some(status)) => log::info!("Worker has exited with {}", status),
                Ok(None) => log::info!("Woker is running"),
                Err(err) => panic!("Worker should either exited or still running! {}", err),
            }
        }
        Ok(())
    }
}