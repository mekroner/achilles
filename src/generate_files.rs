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
    let source_bundles = get_n_random_source_bundles(5);

    let builder = StreamGen::builder()
        .in_path(&config.path_config.test_run(test_run_id))
        .coordinator_log_level(NesLogLevel::Debug)
        .worker_log_level(NesLogLevel::Debug)
        .add_source_bundles(source_bundles);
    let result = builder.build().generate();

    if let Err(err) = result {
        log::error!("Error generating files: {}", err);
        return;
    }
    log::info!("Generating files done.");
}

fn get_n_random_source_bundles(source_count: u32) -> Vec<SourceBundle> {
    let mut source_bundles = Vec::new();
    for id in 0..source_count {
        let source_name = format!("source-{id}");
        let field_count = 5;
        let physical_source_count = 5;
        let record_count = 500;
        let source_bundle = get_random_source_bundle(
            source_name,
            field_count,
            physical_source_count,
            record_count,
        );
        source_bundles.push(source_bundle);
    }
    source_bundles
}

fn get_random_source_bundle(
    source_name: impl Into<String>,
    field_count: u32,
    physical_source_count: u32,
    record_count: u32,
) -> SourceBundle {
    let mut fields = Vec::new();
    //list of list of field generators (lolofigen)
    let mut lolofigen: Vec<Vec<FieldGenerator>> = Vec::new();

    fields.push(Field::typed("ts", NesType::i64()));
    for _ in 0..physical_source_count {
        let field_gens = vec![FieldGenerator::new(
            "ts",
            NesType::i64(),
            IncStrategy::new(),
        )];
        lolofigen.push(field_gens);
    }

    for id in 0..field_count {
        let name = format!("f{id}");
        let data_type = get_random_type();
        let field = Field::typed(&name, data_type);
        fields.push(field);
        for field_gens in &mut lolofigen {
            field_gens.push(FieldGenerator::new(&name, data_type, RandomStrategy {}));
        }
    }

    let source_name = source_name.into();
    let logical_source = LogicalSource {
        source_name: source_name.clone(),
        fields,
    };

    let mut physical_sources = vec![];
    for (id, field_gens) in lolofigen.into_iter().enumerate() {
        let physical_source = PhysicalSource {
            physical_source_name: format!("{source_name}-{id}"),
            generator: RecordGenerator {
                field_generators: field_gens,
                record_count: record_count.into(),
            },
        };
        physical_sources.push(physical_source);
    }

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
        NesType::f32(),
        NesType::f64(),
    ];
    let mut rng = thread_rng();
    *types
        .choose(&mut rng)
        .expect("Should be able to choose type")
}
