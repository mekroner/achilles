use yaml_rust2::{yaml::Hash, Yaml};

#[allow(non_snake_case)]
pub struct YamlPhysicalSourceConfig {
    pub skipHeader: bool,
    pub filePath: String,
}

#[allow(non_snake_case)]
pub struct YamlPhysicalSource {
    pub logicalSourceName: String,
    pub physicalSourceName: String,
    pub r#type: String,
    pub configuration: YamlPhysicalSourceConfig,
}


#[allow(non_snake_case)]
pub struct YamlWorkerConfig {
    pub logLevel: String,
    pub physicalSources: Vec<YamlPhysicalSource>,
    pub workerId: i32,
}

impl Default for YamlWorkerConfig {
    fn default() -> Self {
        Self {
            logLevel: "LOG_ERROR".to_string(),
            physicalSources: Vec::new(),
            workerId: 0,
        }
    }
}

impl Into<Yaml> for &YamlWorkerConfig {
    fn into(self) -> Yaml {
        let mut config_map: Hash = Hash::new();
        config_map.insert(
            Yaml::String("logLevel".to_string()),
            Yaml::String(self.logLevel.to_string()),
        );
        let source_array = self
            .physicalSources
            .iter()
            .map(|source| source.into())
            .collect::<Vec<Yaml>>();
        config_map.insert(
            Yaml::String("physicalSources".to_string()),
            Yaml::Array(source_array),
        );
        config_map.insert(
            Yaml::String("workerId".to_string()),
            Yaml::Integer(self.workerId.into()),
        );
        Yaml::Hash(config_map)
    }
}

impl Into<Yaml> for &YamlPhysicalSource {
    fn into(self) -> Yaml {
        let mut config_map: Hash = Hash::new();
        config_map.insert(
            Yaml::String("logicalSourceName".to_string()),
            Yaml::String(self.logicalSourceName.to_string()),
        );
        config_map.insert(
            Yaml::String("physicalSourceName".to_string()),
            Yaml::String(self.physicalSourceName.to_string()),
        );
        config_map.insert(
            Yaml::String("type".to_string()),
            Yaml::String(self.r#type.to_string()),
        );
        config_map.insert(
            Yaml::String("configuration".to_string()),
            Yaml::Hash((&self.configuration).into()),
        );
        Yaml::Hash(config_map)
    }
}

impl Into<Hash> for &YamlPhysicalSourceConfig {
    fn into(self) -> Hash {
        let mut config_map: Hash = Hash::new();
        config_map.insert(
            Yaml::String("skipHeader".to_string()),
            Yaml::Boolean(self.skipHeader),
        );
        config_map.insert(
            Yaml::String("filePath".to_string()),
            Yaml::String(self.filePath.to_string()),
        );
        config_map
    }
}

