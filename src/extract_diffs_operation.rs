use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{
    eval::{
        check_results::{read_test_set_results_from_file, TestSetResult},
        evaluator::ResultRelation,
    },
    test_case_gen::query_id::TestCaseId,
    yaml_util::store_yaml_array,
    LancerConfig,
};

pub fn extract_diffs_operatoion(config: &LancerConfig) {
    for run_id in 0..config.test_config.test_run_count {
        reset_extract_dir();
        create_extract_dir(run_id);
        let test_set_results = read_test_set_results_from_file(run_id, &config);

        //extract diffs
        let extracted_test_set_results = test_set_results
            .into_iter()
            .map(|set| extract_diffs(set))
            .collect::<Vec<TestSetResult>>();
        let path = PathBuf::from(format!(
            "./extract-diffs/test-run-{run_id}/test-case-results.yml"
        ));
        store_yaml_array(&path, &extracted_test_set_results);

        // copy diff result files
        for test_set in extracted_test_set_results {
            // if there no diff in test_set there is nothing to do
            if test_set.test_cases.is_empty() {
                continue;
            }

            let test_set_id = test_set.id;
            let origin_file_name = format!("test-set{test_set_id}-origin.csv");
            let (origin_source_path, origin_dest_path) =
                generate_paths(run_id, &origin_file_name, config);
            copy_file(&origin_source_path, &origin_dest_path);

            for test_case in test_set.test_cases {
                let TestCaseId::Other(test_case_id) = test_case.id else {
                    continue;
                };
                let other_file_name = format!("test-set{test_set_id}-other{test_case_id}.csv");
                let (other_source_path, other_dest_path) =
                    generate_paths(run_id, &other_file_name, config);
                copy_file(&other_source_path, &other_dest_path)
            }
        }
    }
}

fn generate_paths(run_id: u32, file_name: &str, config: &LancerConfig) -> (PathBuf, PathBuf) {
    let source_path = config.path_config.result(run_id).join(file_name);
    let dest_path =
        PathBuf::from(format!("./extract-diffs/test-run-{}/results", run_id)).join(file_name);
    (source_path, dest_path)
}

fn copy_file(source: &Path, destination: &Path) {
    match fs::copy(source, destination) {
        Ok(bytes_copied) => {
            log::debug!("File copied successfully! {} bytes copied.", bytes_copied);
        }
        Err(e) => {
            log::error!("Error copying file: {}", e);
        }
    }
}

fn extract_diffs(set: TestSetResult) -> TestSetResult {
    let test_cases = set
        .test_cases
        .into_iter()
        .filter(|case| case.relation == ResultRelation::Diff)
        .collect();
    TestSetResult {
        id: set.id,
        strategy: set.strategy,
        test_cases,
    }
}

fn reset_extract_dir() {
    let override_files = true;
    let path = PathBuf::from("./extract-diffs");
    if override_files && path.exists() {
        log::info!("Deleting existing files in path: {:?}", &path);
        fs::remove_dir_all(&path).unwrap();
    }
    fs::create_dir(&path).unwrap();
}

fn create_extract_dir(test_run_id: u32) {
    let test_run_path = PathBuf::from(format!("./extract-diffs/test-run-{test_run_id}"));
    log::info!(
        "Creating test-run-{test_run_id} directory in {:?}",
        test_run_path
    );
    if let Err(err) = fs::create_dir(test_run_path) {
        log::error!("{err}");
        return;
    }

    let result_path = PathBuf::from(format!("./extract-diffs/test-run-{test_run_id}/results"));
    log::info!("Creating result directory in {:?}", result_path);
    if let Err(err) = fs::create_dir(result_path) {
        log::error!("{err}");
        return;
    }
}
