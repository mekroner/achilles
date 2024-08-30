use crate::config::LancerConfig;
use crate::stream_gen::SourceBundle;
use crate::stream_gen::{
    data_generator::{FieldGenerator, IncStrategy, RandomStrategy, RecordGenerator},
    physical_source::PhysicalSource,
    stream_gen::{NesLogLevel, StreamGen},
    LogicalSource,
};
use nes_rust_client::expression::Field;
use nes_types::NesType;
use rand::seq::SliceRandom;
use rand::thread_rng;

pub fn generate_files(test_run_id: u32, config: &LancerConfig) {
    log::info!("Start generating files.");
    // let logical_source = LogicalSource {
    //     source_name: "test".to_string(),
    //     fields: vec![
    //         Field::typed("ts", NesType::i64()),
    //         Field::typed("id", NesType::i64()),
    //         Field::typed("value", NesType::i64()),
    //     ],
    // };

    // let physical_source = PhysicalSource {
    //     physical_source_name: "test-1".to_string(),
    //     generator: RecordGenerator {
    //         record_count: 100,
    //         field_generators: vec![
    //             FieldGenerator::new("ts", NesType::i64(), IncStrategy::new()),
    //             FieldGenerator::new("id", NesType::i64(), RandomStrategy {}),
    //             FieldGenerator::new("value", NesType::i64(), RandomStrategy {}),
    //         ],
    //     },
    // };
    let source_bundle = get_random_source_bundle("source");

    let result = StreamGen::builder()
        .in_path(&config.path_config.test_run(test_run_id))
        .add_source_bundle(source_bundle)
        // .add_logical_source(logical_source)
        // .with_physical_sources([physical_source])
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

fn get_random_source_bundle(source_name: impl Into<String>) -> SourceBundle {
    let field_count = 5;
    let mut fields = Vec::new();
    let mut field_generators = Vec::new();
    fields.push(Field::typed("ts", NesType::i64()));
    field_generators.push(FieldGenerator::new(
        "ts",
        NesType::i64(),
        IncStrategy::new(),
    ));

    for id in 0..field_count {
        let name = format!("f{id}");
        let data_type = get_random_type();
        let field = Field::typed(&name, data_type);
        fields.push(field);
        field_generators.push(FieldGenerator::new(name, data_type, RandomStrategy {}));
    }
    let source_name = source_name.into();
    let logical_source = LogicalSource {
        source_name: source_name.clone(),
        fields,
    };

    let physical_sources = vec![PhysicalSource {
        physical_source_name: format!("{}-0", source_name),
        generator: RecordGenerator {
            field_generators,
            record_count: 100,
        },
    }];

    SourceBundle {
        logical_source,
        physical_sources,
    }
}

fn get_random_type() -> NesType {
    let types = [
        // NesType::bool(),
        NesType::u8(),
        NesType::i8(),
        NesType::u16(),
        NesType::i16(),
        NesType::u32(),
        NesType::i32(),
        NesType::u64(),
        NesType::i64(),
    ];
    let mut rng = thread_rng();
    *types
        .choose(&mut rng)
        .expect("Should be able to choose type")
}
