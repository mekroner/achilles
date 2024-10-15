use achilles::stream_gen::data_generator::{FieldGeneratorStrategy, TimeStampStrategy};

fn main() {
    simple_logger::init_with_level(log::Level::Trace).expect("Simple_logger should not fail!");
    log::info!("This example shows the time stamp data generator");

    let mut gen = TimeStampStrategy::new(0);
    for _ in 0..100 {
        log::info!("{}", gen.generate_field());
    }
}
