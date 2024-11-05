use std::{
    fs,
    path::{Path, PathBuf},
};

use yaml_rust2::{Yaml, YamlLoader};

use crate::{
    config::{FilePathConfig, TestConfig},
    nes_query_comp_config::NesQueryCompilerConfig,
    LancerConfig,
};

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
                    "path_config" => config.path_config = parse_path_config(value),
                    "query_comp_config" => config.query_comp_config = parse_query_comp_config(value),
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
                    "predicate_depth" => {
                        let Some(predicate_depth) = value.as_i64() else {
                            log::error!("Unable to parse predicate_depth");
                            continue;
                        };
                        config.predicate_depth = predicate_depth as u32;
                    }
                    _ => {}
                }
            }
        }
    }
    config
}

fn parse_path_config(yaml: &Yaml) -> FilePathConfig {
    let mut config = FilePathConfig::default();
    if let Yaml::Hash(ref hash) = yaml {
        for (key, value) in hash {
            if let Yaml::String(ref key_str) = key {
                match key_str.as_str() {
                    "base" => {
                        let Some(base) = value.as_str() else {
                            log::error!("Unable to parse base");
                            continue;
                        };
                        config.base = PathBuf::from(base);
                    }
                    "test_run" => {
                        let Some(test_run) = value.as_str() else {
                            log::error!("Unable to parse test_run");
                            continue;
                        };
                        config.test_run = PathBuf::from(test_run);
                    }
                    "stream_config" => {
                        let Some(stream_config) = value.as_str() else {
                            log::error!("Unable to parse stream_config");
                            continue;
                        };
                        config.stream_config = PathBuf::from(stream_config);
                    }
                    "results" => {
                        let Some(results) = value.as_str() else {
                            log::error!("Unable to parse results");
                            continue;
                        };
                        config.results = PathBuf::from(results);
                    }
                    "coordinator_config_file" => {
                        let Some(coordinator_config_file) = value.as_str() else {
                            log::error!("Unable to parse coordinator_config_file");
                            continue;
                        };
                        config.coordinator_config_file = PathBuf::from(coordinator_config_file);
                    }
                    "worker_configs" => {
                        let Some(worker_configs) = value.as_str() else {
                            log::error!("Unable to parse worker_configs");
                            continue;
                        };
                        config.worker_configs = PathBuf::from(worker_configs);
                    }
                    "test_sets_file" => {
                        let Some(test_sets_file) = value.as_str() else {
                            log::error!("Unable to parse test_sets_file");
                            continue;
                        };
                        config.test_sets_file = PathBuf::from(test_sets_file);
                    }
                    "test_set_execs_file" => {
                        let Some(test_set_execs_file) = value.as_str() else {
                            log::error!("Unable to parse test_set_execs_file");
                            continue;
                        };
                        config.test_set_execs_file = PathBuf::from(test_set_execs_file);
                    }
                    "test_set_results_file" => {
                        let Some(test_set_results_file) = value.as_str() else {
                            log::error!("Unable to parse test_set_results_file");
                            continue;
                        };
                        config.test_set_results_file = PathBuf::from(test_set_results_file);
                    }
                    _ => {}
                }
            }
        }
    }
    config
}

fn parse_query_comp_config(yaml: &Yaml) -> NesQueryCompilerConfig {
    let mut config = NesQueryCompilerConfig::default();
    if let Yaml::Hash(ref hash) = yaml {
        for (key, value) in hash {
            if let Yaml::String(ref key_str) = key {
                match key_str.as_str() {
                    "pipelining_strategy" => {
                        let pipelining_strategy = match value.try_into() {
                            Ok(ok) => ok,
                            Err(err) => {
                                log::error!("Unable to parse compilation_strategy: {err}");
                                continue;
                            }
                        };
                        config.pipelining_strategy = pipelining_strategy;
                    }

                    "compilation_strategy" => {
                        let compilation_strategy = match value.try_into() {
                            Ok(ok) => ok,
                            Err(err) => {
                                log::error!("Unable to parse compilation_strategy: {err}");
                                continue;
                            }
                        };
                        config.compilation_strategy = compilation_strategy;
                    }

                    "output_buffer_optimization_level" => {
                        let output_buffer_optimization_level = match value.try_into() {
                            Ok(ok) => ok,
                            Err(err) => {
                                log::error!("Unable to parse output_buffer_optimization_level: {err}");
                                continue;
                            }
                        };
                        config.output_buffer_optimization_level = output_buffer_optimization_level;
                    }

                    "windowing_strategy" => {
                        let windowing_strategy = match value.try_into() {
                            Ok(ok) => ok,
                            Err(err) => {
                                log::error!("Unable to parse windowing_strategy: {err}");
                                continue;
                            }
                        };
                        config.windowing_strategy = windowing_strategy;
                    }

                    "query_compiler_type" => {
                        let query_compiler_type = match value.try_into() {
                            Ok(ok) => ok,
                            Err(err) => {
                                log::error!("Unable to parse query_compiler_type: {err}");
                                continue;
                            }
                        };
                        config.query_compiler_type = query_compiler_type;
                    }
                    _ => {}
                }
            }
        }
    }
    config
}
