use nes_rust_client::expression::Field;
use std::net::Ipv4Addr;
use yaml_rust2::{yaml::Hash, Yaml};

use crate::{
    nes_opt_config::{MemoryLayoutPolicy, NesOptConfig, QueryMergerRule},
    stream_gen::LogicalSource,
};

use super::nes_type::YamlNesType;

#[allow(non_snake_case)]
pub struct YamlField {
    pub name: String,
    pub r#type: YamlNesType,
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
    pub opt_config: NesOptConfig,
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

        config_map.insert(Yaml::String("optimizer".into()), (&self.opt_config).into());

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

#[allow(non_snake_case)]
impl TryFrom<&Yaml> for YamlCoordinatorConfig {
    type Error = String;

    // FIXME: Ther could be optional parameters
    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Yaml::String(ref log_level) = value["logLevel"] else {
            return Err("Failed to parse YamlCoordinatorConfig: LogLevel".to_string());
        };
        let Yaml::String(ref coordinator_ip) = value["coordinatorIp"] else {
            return Err("Failed to parse YamlCoordinatorConfig: coordinatorIp".to_string());
        };
        let Ok(coordinatorIp) = coordinator_ip.parse::<Ipv4Addr>() else {
            return Err(
                "Failed to parse YamlCoordinatorConfig: coordinatorIp not valid.".to_string(),
            );
        };
        let Yaml::Integer(rest_port) = value["restPort"] else {
            return Err("Failed to parse YamlCoordinatorConfig: restPort".to_string());
        };
        let Yaml::Integer(rpc_port) = value["rpcPort"] else {
            return Err("Failed to parse YamlCoordinatorConfig: rpcPort".to_string());
        };
        let Yaml::Array(ref sources) = value["logicalSources"] else {
            return Err("Failed to parse YamlCoordinatorConfig: logicalSources".to_string());
        };
        
        let opt_config = NesOptConfig::try_from(&value["optimizer"])?;

        let logicalSources = sources
            .into_iter()
            .map(|source| source.try_into())
            .collect::<Result<Vec<YamlLogicalSource>, _>>()?;
        Ok(Self {
            logLevel: log_level.to_string(),
            coordinatorIp,
            restPort: rest_port as u32,
            rpcPort: rpc_port as u32,
            logicalSources,
            opt_config,
        })
    }
}

// YamlLogicalSource
impl Into<LogicalSource> for YamlLogicalSource {
    fn into(self) -> LogicalSource {
        LogicalSource {
            source_name: self.logicalSourceName,
            fields: self.fields.into_iter().map(|field| field.into()).collect(),
        }
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

impl TryFrom<&Yaml> for YamlLogicalSource {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Yaml::String(ref name) = value["logicalSourceName"] else {
            return Err("Failed to parse YamlLogicalSource: logicalSourceName.".into());
        };
        let Yaml::Array(ref fields_arr) = value["fields"] else {
            return Err("Failed to parse YamlLogicalSource: logicalSourceName.".into());
        };
        let fields = fields_arr
            .into_iter()
            .map(|field| field.try_into())
            .collect::<Result<Vec<YamlField>, _>>()?;
        Ok(Self {
            logicalSourceName: name.to_string(),
            fields,
        })
    }
}

// YamlField
impl Into<Field> for YamlField {
    fn into(self) -> Field {
        Field::typed(self.name, self.r#type.into())
    }
}

impl Into<Yaml> for &YamlField {
    fn into(self) -> Yaml {
        let mut config_map: Hash = Hash::new();
        config_map.insert(
            Yaml::String("name".into()),
            Yaml::String(self.name.to_string()),
        );
        config_map.insert(Yaml::String("type".into()), self.r#type.into());
        Yaml::Hash(config_map)
    }
}

impl TryFrom<&Yaml> for YamlField {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Yaml::String(ref name) = value["name"] else {
            return Err("Failed to parse YamlField: name.".into());
        };
        let data_type = (&value["type"]).try_into()?;
        Ok(Self {
            name: name.to_string(),
            r#type: data_type,
        })
    }
}
