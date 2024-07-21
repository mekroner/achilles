use std::{
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use nes_lancer::{check_results, generate_files, run_queries, generate_queries, LancerConfig};
use nes_types::NesType;

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Debug)
        .expect("Simple Logger should not fail to init!");
    let config = LancerConfig::default();
    generate_files(&config);
    let queries = generate_queries(&config);
    run_queries(&queries, &config);
    check_results(&config);
}
