use std::{path::PathBuf, time::Duration};

pub struct LancerConfig {
    pub generated_files_path: PathBuf,
            pub max_query_exec_duration: Duration,
}

impl Default for LancerConfig {
    fn default() -> Self {
        LancerConfig {
            generated_files_path: PathBuf::from("./generated_files"),
            max_query_exec_duration: Duration::from_secs(10),
        }
    }
}
