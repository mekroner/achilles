use nes_types::NesType;
use rand::{thread_rng, Rng};

pub struct RecordGenerator {
    pub field_generators: Vec<FieldGenerator>,
    pub record_count: u64,
}

pub struct FieldGenerator {
    field_name: String,
    data_type: NesType,
    strategy: Box<dyn FieldGeneratorStrategy>,
}

impl FieldGenerator {
    pub fn new(
        field_name: impl Into<String>,
        data_type: NesType,
        strategy: impl FieldGeneratorStrategy + 'static,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            data_type,
            strategy: Box::new(strategy),
        }
    }

    pub fn generate_field(&mut self) -> String {
        self.strategy.generate_field(self.data_type)
    }
}

impl RecordGenerator {
    pub fn generate_header(&self) -> Vec<String> {
        self.field_generators
            .iter()
            .map(|field_gen| field_gen.field_name.clone())
            .collect()
    }

    pub fn generate_record(&mut self) -> Vec<String> {
        self.field_generators
            .iter_mut()
            .map(|field_gen| field_gen.generate_field())
            .collect()
    }
}

pub trait FieldGeneratorStrategy {
    fn generate_field(&mut self, data_type: NesType) -> String;
}

pub struct RandomStrategy {
}

impl FieldGeneratorStrategy for RandomStrategy {
    // FIXME: implement this function
    fn generate_field(&mut self, data_type: NesType) -> String {
        let mut rng = thread_rng();
        match data_type {
            NesType::Undefined => panic!("FieldGenerator data_type cannot be Undefined"),
            NesType::Bool => rng.gen::<bool>().to_string(),
            NesType::Char => panic!("FieldGenerator char is currently not supported!"),
            NesType::Int32 => rng.gen::<i32>().to_string(),
            NesType::Int64 => rng.gen::<i64>().to_string(),
            NesType::Float32 => rng.gen::<f32>().to_string(),
            NesType::Float64 => rng.gen::<f64>().to_string(),
        }
    }
}

pub struct IncStrategy {
    counter: u32,
}

impl IncStrategy {
    pub fn new() -> Self {
        Self { counter: 0 }
    }
}

impl FieldGeneratorStrategy for IncStrategy {
    // FIXME: implement this function
    fn generate_field(&mut self, data_type: NesType) -> String {
        let result = self.counter.to_string();
        self.counter += 1;
        result
    }
}
