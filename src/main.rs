use std::fs;

use achilles::{
    check_test_sets,
    eval::check_results::write_test_set_results_to_file,
    generate_files, generate_test_sets,
    process_test_case::process_test_sets::process_test_sets,
    replay_exec::{replay_exec, ReplayExec},
    stages::Stages,
    stream_schema::read_stream_schema_from_file,
    test_case_exec::{read_test_set_execs_from_file, write_test_set_execs_to_file},
    test_case_gen::test_case::{read_test_sets_to_file, write_test_sets_to_file},
    LancerConfig,
};

#[derive(Default, Clone)]
pub enum OperationMode {
    #[default]
    Default,
    ReplayExec(ReplayExec),
    Summary,
}

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Debug)
        .expect("Simple Logger should not fail to init!");
    let config = LancerConfig::default();
    let operation_mode = OperationMode::Default;
    // let operation_mode = OperationMode::ReplayExec(ReplayExec::test_set(0, 1));
    match operation_mode {
        OperationMode::Default => default_operation(&config).await,
        OperationMode::ReplayExec(replay) => replay_exec(&replay, &config).await,
        OperationMode::Summary => (),
    }
}

async fn default_operation(config: &LancerConfig) {
    let override_files = true;
    let test_run_count = 1;

    let path = config.path_config.base();
    if override_files && path.exists() {
        log::info!("Deleting existing files in path: {:?}", path);
        fs::remove_dir_all(path).unwrap();
    }
    fs::create_dir(path).unwrap();

    for id in 0..test_run_count {
        log::info!("Starting test run {id}.");
        log::info!(
            "Creating test-run-{id} directory in {:?}",
            config.path_config.test_run(id)
        );
        if let Err(err) = fs::create_dir(config.path_config.test_run(id)) {
            log::error!("{err}");
            return;
        }
        log::info!(
            "Creating result directory in {:?}",
            config.path_config.result(id)
        );
        if let Err(err) = fs::create_dir(config.path_config.result(id)) {
            log::error!("{err}");
            return;
        }
        test_run(id, &config).await;
    }
}

async fn test_run(id: u32, config: &LancerConfig) {
    if config.skip_to_stage <= Stages::StreamGen {
        generate_files(id, config);
    } else {
        log::info!("Skipping Stage StreamGen...");
    }

    if config.skip_to_stage <= Stages::QueryGen {
        let schema = read_stream_schema_from_file(id, &config);
        let test_sets = generate_test_sets(id, &config, &schema);
        write_test_sets_to_file(id, &config, &test_sets);
    } else {
        log::info!("Skipping Stage QueryGen...");
    }

    if config.skip_to_stage <= Stages::QueryExec {
        let test_sets = read_test_sets_to_file(id, &config);
        let test_set_execs = process_test_sets(id, &config, test_sets).await;
        write_test_set_execs_to_file(id, &config, &test_set_execs);
    } else {
        log::info!("Skipping Stage QueryExec...");
    }

    if config.skip_to_stage <= Stages::Evaluation {
        let test_set_execs = read_test_set_execs_from_file(id, &config);
        let test_set_results = check_test_sets(&test_set_execs);
        write_test_set_results_to_file(id, &config, &test_set_results)
    } else {
        log::info!("Skipping Stage Evaluation...");
    }
}
