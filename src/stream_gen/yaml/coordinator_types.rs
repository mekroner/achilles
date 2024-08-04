use std::net::Ipv4Addr;
use yaml_rust2::{yaml::Hash, Yaml};

#[allow(non_snake_case)]
pub struct YamlField {
    pub name: String,
    pub r#type: String,
}

#[allow(non_snake_case)]
pub struct YamlLogicalSource {
    pub logicalSourceName: String,
    pub fields: Vec<YamlField>,
}

#[allow(non_snake_case)]
pub struct YamlCoordinatorConfig {
    pub logLevel: String,
    pub coordinatorIp: Ipv4Addr,
    pub restPort: u32,
    pub rpcPort: u32,
    pub logicalSources: Vec<YamlLogicalSource>,
}

impl Into<Yaml> for &YamlCoordinatorConfig {
    fn into(self) -> Yaml {
        let mut config_map: Hash = Hash::new();
        config_map.insert(
            Yaml::String("logLevel".into()),
            Yaml::String(self.logLevel.to_string()),
        );
        config_map.insert(
            Yaml::String("coordinatorIp".into()),
            Yaml::String(self.coordinatorIp.to_string()),
        );
        config_map.insert(
            Yaml::String("restPort".into()),
            Yaml::Integer(self.restPort.into()),
        );
        config_map.insert(
            Yaml::String("rpcPort".into()),
            Yaml::Integer(self.rpcPort.into()),
        );
        let source_array = self
            .logicalSources
            .iter()
            .map(|source| source.into())
            .collect::<Vec<Yaml>>();
        config_map.insert(
            Yaml::String("logicalSources".into()),
            Yaml::Array(source_array),
        );
        Yaml::Hash(config_map)
    }
}

impl Into<Yaml> for &YamlLogicalSource {
    fn into(self) -> Yaml {
        let mut config_map: Hash = Hash::new();
        config_map.insert(
            Yaml::String("logicalSourceName".into()),
            Yaml::String(self.logicalSourceName.to_string()),
        );
        let field_array = self
            .fields
            .iter()
            .map(|field| field.into())
            .collect::<Vec<Yaml>>();
        config_map.insert(Yaml::String("fields".into()), Yaml::Array(field_array));

        Yaml::Hash(config_map)
    }
}

impl Into<Yaml> for &YamlField {
    fn into(self) -> Yaml {
        let mut config_map: Hash = Hash::new();
        config_map.insert(
            Yaml::String("name".into()),
            Yaml::String(self.name.to_string()),
        );
        config_map.insert(
            Yaml::String("type".into()),
            Yaml::String(self.r#type.to_string()),
        );
        Yaml::Hash(config_map)
    }
}
