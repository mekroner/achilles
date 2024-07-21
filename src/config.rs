use std::path::PathBuf;

pub struct LancerConfig {
    pub generated_files_path: PathBuf,
}

impl Default for LancerConfig {
    fn default() -> Self {
        LancerConfig {
            generated_files_path: PathBuf::from("./generated_files"),
        }
    }
}
