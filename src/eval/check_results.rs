use super::evaluator::*;
use nes_rust_client::query::stringify::stringify_query;

use crate::test_case_exec::{QueryExecStatus, TestCaseExec};

pub fn check_results(runs: Vec<TestCaseExec>) {
    log::info!("Checking results for equivalence:");
    for run in runs.iter() {
        for props in run.others.iter() {
            log::debug!("Checking results for equivalence:");
            if props.status != QueryExecStatus::Success {
                log::warn!(
                    "Skipping result check for query {} because of execution state {:?}.",
                    props.id(),
                    props.status
                );
                continue;
            }
            log::debug!(
                "Check results of query {}: {}.",
                props.id(),
                stringify_query(props.query())
            );
            match compare_files(run.origin.result_path(), props.result_path()) {
                Ok(ResultRelation::Equal) => log::debug!("Result files are equal."),
                Ok(ResultRelation::Reordered) => log::debug!("Result files are reordered."),
                Ok(ResultRelation::Diff) => log::warn!("Result files are not equal."),
                Err(err) => log::error!("{err}"),
            }
        }
    }
    log::info!("Done, checking results for equivalence.");
}
