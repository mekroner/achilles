use std::path::{Path, PathBuf};

// TODO: Enable this to be read from a file
#[derive(Clone)]
pub struct RunnerConfig {
    pub coordinator_exec_path: PathBuf,
    pub worker_exec_path: PathBuf,
    pub coordinator_config_path: Option<PathBuf>,
    pub worker_config_path: Vec<PathBuf>,
    pub output_io: OutputIO,
}

#[derive(PartialEq, Eq, Clone)]
pub enum OutputIO {
    ToFile(PathBuf),
    Null,
    Print,
}

impl RunnerConfig {
    pub fn from_file(path: &Path) {
        todo!();
    }
}
