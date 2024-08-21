use nes_type::YamlNesType;
use yaml_rust2::{Yaml, YamlEmitter};

use super::logical_source::LogicalSource;
use super::net_config::NetworkConfig;
use super::physical_source::PhysicalSource;
use super::stream_gen_builder::StreamGenBuilder;
use super::yaml::*;
use std::io::Write;
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

pub struct SourceBundle {
    pub logical_source: LogicalSource,
    pub physical_sources: Vec<PhysicalSource>,
}

#[derive(Debug, Default)]
pub enum NesLogLevel {
    Debug,
    Info,
    Warn,
    #[default]
    Error,
}

impl ToString for NesLogLevel {
    fn to_string(&self) -> String {
        match self {
            Self::Debug => "LOG_DEBUG",
            Self::Info => "LOG_INFO",
            Self::Warn => "LOG_WARN",
            Self::Error => "LOG_ERROR",
        }
        .to_string()
    }
}

// FIXME: Remove public access from fields
pub struct StreamGen {
    pub path: PathBuf,
    pub override_files: bool,
    pub sources: Vec<SourceBundle>,
    pub worker_log_level: NesLogLevel,
    pub coordinator_log_level: NesLogLevel,
    pub network_config: NetworkConfig,
}

impl StreamGen {
    pub fn builder() -> StreamGenBuilder {
        StreamGenBuilder::new()
    }

    // FIXME: Use Custom Error type here
    pub fn generate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.override_files && self.path.exists() {
            log::info!("Overriding existing files in path: {:?}", self.path);
            fs::remove_dir_all(&self.path)?;
        }

        let worker_dir_path: PathBuf = self.path.as_path().join("workers");
        let data_dir_path: PathBuf = self.path.as_path().join("data");

        fs::create_dir(&self.path)?;
        log::info!("Created dir {:?}", self.path);
        self.generate_coordinator_config(&self.path);

        fs::create_dir(&worker_dir_path)?;
        log::info!("Created dir {:?}", worker_dir_path);
        self.generate_worker_configs(&worker_dir_path);

        fs::create_dir(&data_dir_path)?;
        log::info!("Created dir {:?}", data_dir_path);
        self.generate_data(&data_dir_path);

        Ok(())
    }

    fn generate_coordinator_config(&self, path: &PathBuf) {
        let logical_source = self
            .sources
            .iter()
            .map(|source| {
                let fields = source
                    .logical_source
                    .fields
                    .iter()
                    .map(|field| YamlField {
                        name: field.name().to_string(),
                        r#type: YamlNesType::from(field.data_type()),
                    })
                    .collect();
                YamlLogicalSource {
                    logicalSourceName: source.logical_source.source_name.clone(),
                    fields,
                }
            })
            .collect();
        let coordinator_config = YamlCoordinatorConfig {
            logLevel: self.coordinator_log_level.to_string(),
            logicalSources: logical_source,
            coordinatorIp: self.network_config.coord_ip,
            restPort: self.network_config.rest_port.into(),
            rpcPort: self.network_config.rpc_port.into(),
        };
        let yaml_obj: Yaml = (&coordinator_config).into();
        let mut out_str = String::new();
        let mut emitter = YamlEmitter::new(&mut out_str);
        emitter.dump(&yaml_obj).unwrap();
        let mut file = fs::File::create(path.join("coordinator.yml"))
            .expect("coordinator.yml has to be created!");
        write!(file, "{out_str}").unwrap();
        log::debug!("Created coordinator.yml:\n{}", out_str);
    }

    // FIXME: file_path should be canonicalized
    fn generate_worker_configs(&self, worker_dir_path: &PathBuf) {
        let mut worker_id = 0;
        for source in self.sources.iter() {
            for phy_source in source.physical_sources.iter() {
                let file_path = self.path.join(format!(
                    "data/{}.csv",
                    phy_source.physical_source_name.clone()
                ));
                let file_path = file_path.to_string_lossy().to_string();
                log::info!("Path: {:?}", file_path);
                let worker_config = YamlWorkerConfig {
                    logLevel: self.worker_log_level.to_string(),
                    physicalSources: vec![YamlPhysicalSource {
                        logicalSourceName: source.logical_source.source_name.clone(),
                        physicalSourceName: phy_source.physical_source_name.clone(),
                        r#type: "CSV_SOURCE".to_string(),
                        configuration: YamlPhysicalSourceConfig {
                            skipHeader: true,
                            filePath: file_path,
                        },
                    }],
                    workerId: worker_id,
                    ..Default::default()
                };

                let yaml_obj: Yaml = (&worker_config).into();
                let mut out_str = String::new();
                let mut emitter = YamlEmitter::new(&mut out_str);
                emitter.dump(&yaml_obj).unwrap();
                let mut file =
                    fs::File::create(worker_dir_path.join(format!("worker-{worker_id}.yml")))
                        .expect("Worker files have to be created!");
                write!(file, "{out_str}").unwrap();
                log::debug!("Created worker-{worker_id}.yml:\n{out_str}");
                worker_id += 1;
            }
        }
    }

    fn generate_data(&mut self, data_dir_path: &Path) {
        for source in self.sources.iter_mut() {
            for phy_source in source.physical_sources.iter_mut() {
                let file_name = format!("{}.csv", phy_source.physical_source_name);
                let file_path = data_dir_path.join(&file_name);
                let mut wtr = csv::WriterBuilder::new()
                    .delimiter(b',')
                    .from_path(file_path)
                    .expect("Creating CSV Writer Should not fail!");
                let header = phy_source.generator.generate_header();
                wtr.write_record(header);
                for _ in 0..phy_source.generator.record_count {
                    let record = phy_source.generator.generate_record();
                    wtr.write_record(record);
                }
                log::debug!("Created {file_name}.");
            }
        }
    }
}
