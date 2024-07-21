use crate::config::LancerConfig;
use nes_stream_gen::{
    data_generator::{FieldGenerator, IncStrategy, RandomStrategy, RecordGenerator},
    physical_source::PhysicalSource,
    stream_generator::{NesLogLevel, StreamGen},
    Field, LogicalSource,
};
use nes_types::NesType;

pub fn generate_files(config: &LancerConfig) {
    log::info!("Start generating files.");
    let logical_source = LogicalSource {
        source_name: "test".to_string(),
        fields: vec![
            Field::new("ts", NesType::Int64),
            Field::new("id", NesType::Int64),
            Field::new("value", NesType::Int64),
        ],
    };
    let physical_source = PhysicalSource {
        physical_source_name: "test-1".to_string(),
        generator: RecordGenerator {
            record_count: 100,
            field_generators: vec![
                FieldGenerator::new("ts", NesType::Int64, IncStrategy::new()),
                FieldGenerator::new("id", NesType::Int64, RandomStrategy {}),
                FieldGenerator::new("value", NesType::Int64, RandomStrategy {}),
            ],
        },
    };
    let result = StreamGen::builder()
        .in_path(&config.generated_files_path)
        .add_logical_source(logical_source)
        .with_physical_sources([physical_source])
        .coordinator_log_level(NesLogLevel::Debug)
        .worker_log_level(NesLogLevel::Debug)
        .build()
        .generate();
    if let Err(err) = result {
        log::error!("Error generating files: {}", err);
        return;
    }
    log::info!("Generating files done.");
}
