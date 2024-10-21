use std::{fs, path::Path};

use yaml_rust2::{Yaml, YamlLoader};

use crate::{config::TestConfig, LancerConfig};

// FIXME: fully implement this function
pub fn load_config(path: &Path) -> LancerConfig {
    let Ok(content) = fs::read_to_string(path) else {
        return LancerConfig::default();
    };
    let Ok(docs) = YamlLoader::load_from_str(&content) else {
        return LancerConfig::default();
    };
    let Some(doc) = &docs.first() else {
        return LancerConfig::default();
    };

    // Create a default LancerConfig
    let mut config = LancerConfig::default();

    // Update config with values from YAML
    if let Yaml::Hash(ref hash) = doc {
        for (key, value) in hash {
            if let Yaml::String(ref key_str) = key {
                match key_str.as_str() {
                    "test_config" => config.test_config = parse_test_config(value),
                    _ => {}
                }
            }
        }
    }

    config
}

fn parse_test_config(yaml: &Yaml) -> TestConfig {
    let mut config = TestConfig::default();
    if let Yaml::Hash(ref hash) = yaml {
        for (key, value) in hash {
            if let Yaml::String(ref key_str) = key {
                match key_str.as_str() {
                    // "oracles" => {
                    //     config.oracles = parse_string_array(value);
                    // }
                    "test_run_count" => {
                        let Some(test_run_count) = value.as_i64() else {
                            log::error!("Unable to parse test_run_count");
                            continue;
                        };
                        config.test_run_count = test_run_count as u32;
                    }
                    "oracle_reps" => {
                        let Some(oracle_reps) = value.as_i64() else {
                            log::error!("Unable to parse oracle_reps");
                            continue;
                        };
                        config.oracle_reps = oracle_reps as u32;
                    }
                    "test_case_count" => {
                        let Some(test_case_count) = value.as_i64() else {
                            log::error!("Unable to parse test_case_count");
                            continue;
                        };
                        config.test_case_count = test_case_count as u32;
                    }
                    "field_count" => {
                        let Some(field_count) = value.as_i64() else {
                            log::error!("Unable to parse field_count");
                            continue;
                        };
                        config.field_count = field_count as u32;
                    }
                    "record_count" => {
                        let Some(record_count) = value.as_i64() else {
                            log::error!("Unable to parse record_count");
                            continue;
                        };
                        config.record_count = record_count as u32;
                    }
                    "physical_source_count" => {
                        let Some(physical_source_count) = value.as_i64() else {
                            log::error!("Unable to parse physical_source_count");
                            continue;
                        };
                        config.physical_source_count = physical_source_count as u32;
                    }
                    _ => {}
                }
            }
        }
    }
    config
}
