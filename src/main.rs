use std::{
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use nes_lancer::{
    check_results, generate_files, generate_query_runs, run_queries, LancerConfig,
};
use nes_types::NesType;

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Debug)
        .expect("Simple Logger should not fail to init!");
    let config = LancerConfig::default();
    generate_files(&config);
    let queries = generate_query_runs(&config);
    let run_results = run_queries(&queries, &config).await;
    // check_results(&config, &queries);
}
