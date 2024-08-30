use core::panic;
use std::{fs, path::Path};

use achilles::{
    check_test_sets,
    eval::check_results::{check_test_set, write_test_set_results_to_file},
    generate_files, generate_test_sets,
    run_queries::{process_single_test_case, process_test_set, process_test_sets},
    stages::Stages,
    stream_schema::read_stream_schema_from_file,
    test_case_exec::{read_test_set_execs_from_file, write_test_set_execs_to_file},
    test_case_gen::{query_id::TestCaseId, test_case::{read_test_sets_to_file, write_test_sets_to_file, TestSet}},
    LancerConfig,
};
use yaml_rust2::{Yaml, YamlEmitter, YamlLoader};

#[derive(Default, Clone)]
pub enum OperationMode {
    #[default]
    Default,
    ReplayExec(ReplayExec),
}

#[derive(Clone)]
struct ReplayExec {
    run_id: u32,
    test_set_id: u32,
}

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Debug)
        .expect("Simple Logger should not fail to init!");
    let config = LancerConfig::default();
    let operation_mode = OperationMode::ReplayExec(ReplayExec {
        run_id: 0,
        test_set_id: 1,
    });
    match operation_mode {
        OperationMode::Default => default_operation(&config).await,
        OperationMode::ReplayExec(replay) => replay_exec_test_case(&replay, &config).await,
    }
}

// FIXME: Don't overwrite expisting files
async fn replay_exec_test_set(replay: &ReplayExec, config: &LancerConfig) {
    let Ok(test_sets) = load_yaml_array::<TestSet>(&config.path_config.test_sets(replay.run_id)) else {
        panic!("Unable to load test sets for run {}", replay.run_id);
    };
    let Some(test_set) = test_sets.into_iter().find(|set| set.id == replay.test_set_id) else {
        panic!("Unable to load test set {}", replay.test_set_id);
    };
    log::info!("Loaded test set {} in run {}.", replay.test_set_id, replay.run_id);
    let test_set_exec = process_test_set(replay.run_id, test_set, config).await;
    check_test_set(&test_set_exec);
}

async fn replay_exec_test_case(replay: &ReplayExec, config: &LancerConfig) {
    let Ok(test_sets) = load_yaml_array::<TestSet>(&config.path_config.test_sets(replay.run_id)) else {
        panic!("Unable to load test sets for run {}", replay.run_id);
    };
    let Some(test_set) = test_sets.into_iter().find(|set| set.id == replay.test_set_id) else {
        panic!("Unable to load test set {}", replay.test_set_id);
    };
    let id = TestCaseId::Other(3);
    let Some(test_case) = test_set.test_case(id) else {
        panic!("Unable to load test case {}", id);
    };
    log::info!("Loaded test set {} in run {}.", replay.test_set_id, replay.run_id);
    let test_case_exec = process_single_test_case(replay.run_id, test_case.clone(), config).await;
    log::info!("{:?}", test_case_exec);
}


use std::io::Write;

fn store_yaml_array<T>(path: &Path, values: &[T])
where
    for<'a> Yaml: From<&'a T>,
{
    let yaml_test_cases: Vec<Yaml> = values.iter().map(|test_case| test_case.into()).collect();
    let yaml_arr = Yaml::Array(yaml_test_cases);
    let mut out_str = String::new();
    let mut emitter = YamlEmitter::new(&mut out_str);
    emitter.dump(&yaml_arr).unwrap();
    let mut file = fs::File::create(path).expect("coordinator.yml has to be created!");
    write!(file, "{out_str}").unwrap();
}

fn load_yaml_array<T>(path: &Path) -> Result<Vec<T>, String>
where
    for<'a> T: TryFrom<&'a Yaml, Error = String>,
{
    let content = fs::read_to_string(path).expect("Should have been able to read the file!");
    let docs = YamlLoader::load_from_str(&content).expect("Should have been able to parse Yaml.");
    let Some(doc) = &docs.first() else {
        return Err(format!("Yaml doc should exist!"));
    };
    let Yaml::Array(arr) = doc else {
        return Err(format!("Should have been able to parse Yaml::Array."));
    };
    arr.iter()
        .map(|yaml_obj| T::try_from(yaml_obj))
        .collect::<Result<Vec<T>, String>>()
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
