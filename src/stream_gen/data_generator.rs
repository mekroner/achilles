use std::ops::Range;

use nes_types::{FloatType, IntType, NesType};
use rand::{rngs::ThreadRng, thread_rng, Rng};

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
        self.strategy.generate_field()
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
    fn generate_field(&mut self) -> String;
}

pub struct RandomStrategy {
    data_type: NesType,
    rng: ThreadRng,
}

impl RandomStrategy {
    pub fn new(data_type: NesType) -> Self {
        let rng = thread_rng();
        Self { data_type, rng }
    }
}

impl FieldGeneratorStrategy for RandomStrategy {
    // FIXME: implement this function
    fn generate_field(&mut self) -> String {
        match self.data_type {
            NesType::Undefined => panic!("FieldGenerator data_type cannot be Undefined"),
            NesType::Bool => self.rng.gen::<bool>().to_string(),
            NesType::Char => panic!("FieldGenerator char is currently not supported!"),
            NesType::Int(t) => generate_int(&mut self.rng, t),
            NesType::Float(t) => generate_float(&mut self.rng, t),
        }
    }
}

fn generate_int(rng: &mut ThreadRng, data_type: IntType) -> String {
    match data_type {
        IntType::Signed8 => rng.gen::<i8>().to_string(),
        IntType::Unsigned8 => rng.gen::<u8>().to_string(),
        IntType::Signed16 => rng.gen::<i16>().to_string(),
        IntType::Unsigned16 => rng.gen::<u16>().to_string(),
        IntType::Signed32 => rng.gen::<i32>().to_string(),
        IntType::Unsigned32 => rng.gen::<u32>().to_string(),
        IntType::Signed64 => rng.gen::<i64>().to_string(),
        IntType::Unsigned64 => rng.gen::<u64>().to_string(),
    }
}

fn generate_float(rng: &mut ThreadRng, data_type: FloatType) -> String {
    match data_type {
        FloatType::Bit32 => rng.gen::<f32>().to_string(),
        FloatType::Bit64 => rng.gen::<f64>().to_string(),
    }
}

/// This strategy generates bursty time stamps
pub struct TimeStampStrategy {
    current_time: u32,
    burst_remaining: u32,
    in_burst: bool,
    burst_range: Range<u32>,
    burst_interval_range: Range<u32>,
    quiet_interval_range: Range<u32>,
    rng: ThreadRng,
}

impl TimeStampStrategy {
    pub fn new(start_time: u32) -> Self {
        let mut rng = thread_rng();
        let burst_range = 0..5;
        let burst_interval_range = 0..500;
        let quiet_interval_range = 500..3000;
        Self {
            current_time: start_time,
            // inc_range,
            burst_remaining: rng.gen_range(burst_range.clone()),
            burst_interval_range,
            quiet_interval_range,
            burst_range,
            in_burst: true,
            rng,
        }
    }
}

impl FieldGeneratorStrategy for TimeStampStrategy {
    fn generate_field(&mut self) -> String {
        if self.in_burst {
            self.current_time += self.rng.gen_range(self.burst_interval_range.clone());
            if self.burst_remaining > 0 {
                self.burst_remaining -= 1;
                return self.current_time.to_string();
            }
            self.in_burst = false;
            self.burst_remaining = self.rng.gen_range(self.burst_range.clone());
            return self.current_time.to_string();
        }
        self.current_time += self.rng.gen_range(self.quiet_interval_range.clone());
        self.in_burst = true;
        self.current_time.to_string()
    }
}
