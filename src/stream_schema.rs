use std::fs;

use yaml_rust2::YamlLoader;

use crate::{
    stream_gen::{yaml::YamlCoordinatorConfig, LogicalSource},
    LancerConfig,
};

#[derive(Debug, Clone)]
pub struct StreamSchema {
    pub logical_sources: Vec<LogicalSource>,
}

pub fn read_stream_schema_from_file(test_run_id: u32, config: &LancerConfig) -> StreamSchema {
    let path = config.path_config.coordinator_config(test_run_id);
    let content = fs::read_to_string(path).expect("Should have been able to read the file!");
    let docs = YamlLoader::load_from_str(&content).expect("Should have been able to parse Yaml.");
    let yaml = docs.first().expect("Should have one element");
    let Ok(coodinator): Result<YamlCoordinatorConfig, _> = yaml.try_into() else {
        panic!("Should have been able to parse coodinator.yml.")
    };
    let logical_sources = coodinator
        .logicalSources
        .into_iter()
        .map(|source| source.into())
        .collect();
    StreamSchema { logical_sources }
}
