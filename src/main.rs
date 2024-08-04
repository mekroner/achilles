use achilles::{
    check_results, generate_files, generate_test_cases,
    query_gen::test_case::{read_test_cases_to_file, write_test_cases_to_file},
    run_queries::process_test_cases,
    stages::Stages,
    test_case_exec::{read_test_case_execs_from_file, write_test_case_execs_to_file},
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
        let test_cases = generate_test_cases(&config);
        write_test_cases_to_file(&config, &test_cases);
    } else {
        log::info!("Skipping Stage QueryGen...");
    }

    if config.skip_to_stage <= Stages::QueryExec {
        let test_cases = read_test_cases_to_file(&config);
        let test_case_execs = process_test_cases(&config, test_cases).await;
        write_test_case_execs_to_file(&config, &test_case_execs);
    } else {
        log::info!("Skipping Stage QueryExec...");
    }

    if config.skip_to_stage <= Stages::Evaluation {
        let test_case_execs = read_test_case_execs_from_file(&config);
        check_results(test_case_execs);
    } else {
        log::info!("Skipping Stage Evaluation...");
    }
}
