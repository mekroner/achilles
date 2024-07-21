use std::path::Path;

use nes_eval::evaluator::Evaluator;

use crate::LancerConfig;

pub fn check_results(config: &LancerConfig) {
    log::info!("Checking results for equivalence:");
    let eval = Evaluator::new();
    let path0 = Path::new(&config.generated_files_path).join("result-0.csv");
    let path1 = Path::new(&config.generated_files_path).join("result-1.csv");
    match eval.are_files_equal(&path0, &path1) {
        Ok(true) => log::info!("Result files are equal."),
        Ok(false) => log::info!("Result files are not equal."),
        Err(err) => log::error!("{err}"),
    }
    log::info!("Done, checking results for equivalence.");
}
