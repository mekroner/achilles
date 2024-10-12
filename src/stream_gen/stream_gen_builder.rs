use std::path::PathBuf;

use super::{
    net_config::NetworkConfig,
    stream_gen::{NesLogLevel, StreamGen},
    LogicalSource, PhysicalSource, SourceBundle,
};

#[derive(Default)]
pub struct StreamGenBuilder {
    path: Option<PathBuf>,
    sources: Vec<SourceBundle>,
    worker_log_level: NesLogLevel,
    coordinator_log_level: NesLogLevel,
    network_config: NetworkConfig,
}

pub struct LogicalSourceBuilder {
    stream_gen_builder: StreamGenBuilder,
    logical_source: LogicalSource,
}

impl StreamGenBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    // FIXME: Add an Error type and don't just panic and validate sources
    pub fn build(self) -> StreamGen {
        let path = self.path.expect("Path should exist!");
        let sources = self.sources;
        let override_files = true;
        StreamGen {
            path,
            sources,
            override_files,
            worker_log_level: self.worker_log_level,
            coordinator_log_level: self.coordinator_log_level,
            network_config: self.network_config,
        }
    }

    pub fn network_config(mut self, network_config: NetworkConfig) -> Self {
        self.network_config = network_config;
        self
    }

    pub fn in_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn worker_log_level(mut self, log_level: NesLogLevel) -> Self {
        self.worker_log_level = log_level;
        self
    }

    pub fn coordinator_log_level(mut self, log_level: NesLogLevel) -> Self {
        self.coordinator_log_level = log_level;
        self
    }

    pub fn add_source_bundle(mut self, source_bundle: SourceBundle) -> Self {
        self.sources.push(source_bundle);
        self
    }

    pub fn add_source_bundles(
        mut self,
        source_bundles: impl IntoIterator<Item = SourceBundle>,
    ) -> Self {
        for source_bundle in source_bundles {
            self.sources.push(source_bundle)
        }
        self
    }

    pub fn add_logical_source(self, logical_source: LogicalSource) -> LogicalSourceBuilder {
        LogicalSourceBuilder {
            stream_gen_builder: self,
            logical_source,
        }
    }
}

impl LogicalSourceBuilder {
    pub fn with_physical_sources(
        mut self,
        physical_sources: impl IntoIterator<Item = PhysicalSource>,
    ) -> StreamGenBuilder {
        let physical_sources = physical_sources.into_iter().collect();
        let source_bundle = SourceBundle {
            logical_source: self.logical_source,
            physical_sources,
        };
        self.stream_gen_builder.sources.push(source_bundle);
        self.stream_gen_builder
    }
}
