use nes_lancer::{
    check_results, generate_files, generate_query_runs, run_queries::process_test_runs,
    LancerConfig,
};

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Debug)
        .expect("Simple Logger should not fail to init!");
    let config = LancerConfig::default();
    generate_files(&config);
    let queries = generate_query_runs(&config);
    let run_results = process_test_runs(&config, queries).await;
    check_results(run_results);
}
