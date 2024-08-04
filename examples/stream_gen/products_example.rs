use log::LevelFilter;
use nes_lancer::stream_gen::{
    data_generator::{IncStrategy, RecordGenerator},
    prelude::*,
};
use nes_types::NesType;

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Debug)
        .format_timestamp(None)
        .init();

    let logical_source = LogicalSource {
        source_name: "orders".to_string(),
        fields: vec![
            Field::new("id", NesType::Int64),
            Field::new("product_id", NesType::Int64),
            Field::new("items", NesType::Int64),
            Field::new("ts", NesType::Int64),
        ],
    };

    let physical_source = PhysicalSource {
        physical_source_name: "orders-1".to_string(),
        generator: RecordGenerator {
            record_count: 1_000,
            field_generators: vec![
                FieldGenerator::new("id", NesType::Int64, IncStrategy::new()),
                FieldGenerator::new("product_id", NesType::Int64, RandomStrategy {}),
                FieldGenerator::new("items", NesType::Int64, RandomStrategy {}),
                FieldGenerator::new("ts", NesType::Int64, RandomStrategy {}),
            ],
        },
    };

    let mut generator = StreamGen::builder()
        .in_path("./generated_files")
        .add_logical_source(logical_source)
        .with_physical_sources([physical_source])
        .build();
    if let Err(err) = generator.generate() {
        log::error!("{err}");
    }
}
