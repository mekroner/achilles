use achilles::{
    check_test_sets,
    eval::check_results::write_test_set_results_to_file,
    generate_files, generate_test_sets,
    query_gen::test_case::{read_test_sets_to_file, write_test_sets_to_file},
    run_queries::process_test_sets,
    stages::Stages,
    stream_schema::read_stream_schema_from_file,
    test_case_exec::{read_test_set_execs_from_file, write_test_set_execs_to_file},
    LancerConfig,
};
#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Debug)
        .expect("Simple Logger should not fail to init!");
    let config = LancerConfig::default();

    if config.skip_to_stage <= Stages::StreamGen {
        generate_files(&config);
    } else {
        log::info!("Skipping Stage StreamGen...");
    }

    if config.skip_to_stage <= Stages::QueryGen {
        let schema = read_stream_schema_from_file(&config);
        let test_sets = generate_test_sets(&config, &schema);
        write_test_sets_to_file(&config, &test_sets);
    } else {
        log::info!("Skipping Stage QueryGen...");
    }

    if config.skip_to_stage <= Stages::QueryExec {
        let test_sets = read_test_sets_to_file(&config);
        let test_set_execs = process_test_sets(&config, test_sets).await;
        write_test_set_execs_to_file(&config, &test_set_execs);
    } else {
        log::info!("Skipping Stage QueryExec...");
    }

    if config.skip_to_stage <= Stages::Evaluation {
        let test_set_execs = read_test_set_execs_from_file(&config);
        let test_set_results = check_test_sets(&test_set_execs);
        write_test_set_results_to_file(&config, &test_set_results)
    } else {
        log::info!("Skipping Stage Evaluation...");
    }
}
