use std::{
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use nes_lancer::{check_results, generate_files, generate_queries, run_queries, LancerConfig};
use nes_types::NesType;

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Debug)
        .expect("Simple Logger should not fail to init!");
    let config = LancerConfig::default();
    generate_files(&config);
    let mut queries = generate_queries(&config);
    run_queries(&mut queries, &config).await;
    check_results(&config, &queries);
}
