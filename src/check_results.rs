use std::path::Path;

use nes_eval::prelude::*;
use nes_rust_client::query::stringify::stringify_query;

use crate::{LancerConfig, QueryList};

pub fn check_results(config: &LancerConfig, query_list: &QueryList) {
    log::info!("Checking results for equivalence:");
    for entry in &query_list.entries {
        for props in &entry.others {
            log::info!(
                "Check Results of Query {} and Query {}.",
                stringify_query(&entry.origin.query),
                stringify_query(&props.query)
            );
            match compare_files(&entry.origin.result_path, &props.result_path) {
                Ok(ResultRelation::Equal) => log::info!("Result files are equal."),
                Ok(ResultRelation::Reordered) => log::info!("Result files are reordered."),
                Ok(ResultRelation::Diff) => log::warn!("Result files are not equal."),
                Err(err) => log::error!("{err}"),
            }
        }
    }
    log::info!("Done, checking results for equivalence.");
}
