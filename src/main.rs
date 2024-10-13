use std::fs;

use achilles::{
    check_test_sets,
    eval::check_results::write_test_set_results_to_file,
    generate_files, generate_test_sets,
    process_test_case::process_test_sets::process_test_sets,
    replay_exec::{replay_exec, ReplayExec},
    stages::Stages,
    stream_schema::read_stream_schema_from_file,
    test_case_exec::{
        read_test_set_execs_from_file, write_test_set_execs_to_file, TestCaseExecStatus,
    },
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
    let operation_mode = OperationMode::Summary;
    // let operation_mode = OperationMode::ReplayExec(ReplayExec::test_set(0, 1));
    match operation_mode {
        OperationMode::Default => default_operation(&config).await,
        OperationMode::ReplayExec(replay) => replay_exec(&replay, &config).await,
        OperationMode::Summary => summary_operation(&config),
    }
}

// support per oracle stats
#[derive(Default)]
struct SummaryStats {
    total_count: u32,
    success_count: u32,
    fail_count: u32,
    timeout_count: u32,
    skipped_count: u32,
}

// FIXME: this should work for multiple test runs
fn calculate_summery_stats(run_id: u32, config: &LancerConfig) -> SummaryStats {
    let mut stats = SummaryStats::default();
    let test_set_execs = read_test_set_execs_from_file(run_id, &config);
    for test_case_execs in test_set_execs {
        let iter = std::iter::once(&test_case_execs.origin).chain(test_case_execs.others.iter());
        for exec in iter {
            stats.total_count += 1;
            match exec.status {
                TestCaseExecStatus::Success => stats.success_count += 1,
                TestCaseExecStatus::Failed(_) => stats.fail_count += 1,
                TestCaseExecStatus::TimedOut => stats.timeout_count += 1,
                TestCaseExecStatus::Skipped => stats.skipped_count += 1,
            }
        }
    }

    // test set evals

    stats
}

fn summary_operation(config: &LancerConfig) {
    log::info!("Starting Summary Mode.");
    let run_id = 0;
    let stats = calculate_summery_stats(run_id, config);
    println!("---( run {}) ---", run_id);
    println!("Successful Executed: {} of {} ({:.2})", stats.success_count, stats.total_count, stats.success_count as f32 / stats.total_count as f32 * 100.0);
    println!("Skipped Executed: {} of {} ({:.2})", stats.skipped_count, stats.total_count, stats.skipped_count as f32 / stats.total_count as f32 * 100.0);
    println!("Failed Executed: {} of {} ({:.2})", stats.fail_count, stats.total_count, stats.fail_count as f32 / stats.total_count as f32 * 100.0);
    println!("Timeout Executed: {} of {} ({:.2})", stats.timeout_count, stats.total_count, stats.timeout_count as f32 / stats.total_count as f32 * 100.0);
}

async fn default_operation(config: &LancerConfig) {
    if config.skip_to_stage <= Stages::StreamGen {
        reset_base_dir(config);
    }
    for id in 0..config.test_config.test_run_count {
        log::info!("Starting test run {id}.");
        test_run(id, &config).await;
    }
}

fn reset_base_dir(config: &LancerConfig) {
    let override_files = true;
    let path = config.path_config.base();
    if override_files && path.exists() {
        log::info!("Deleting existing files in path: {:?}", path);
        fs::remove_dir_all(path).unwrap();
    }
}

fn create_base_dir(test_run_id: u32, config: &LancerConfig) {
    let path = config.path_config.base();
    fs::create_dir(path).unwrap();
    log::info!(
        "Creating test-run-{test_run_id} directory in {:?}",
        config.path_config.test_run(test_run_id)
    );
    if let Err(err) = fs::create_dir(config.path_config.test_run(test_run_id)) {
        log::error!("{err}");
        return;
    }
    log::info!(
        "Creating result directory in {:?}",
        config.path_config.result(test_run_id)
    );
    if let Err(err) = fs::create_dir(config.path_config.result(test_run_id)) {
        log::error!("{err}");
        return;
    }
}

async fn test_run(id: u32, config: &LancerConfig) {
    if config.skip_to_stage <= Stages::StreamGen {
        create_base_dir(id, config);
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
