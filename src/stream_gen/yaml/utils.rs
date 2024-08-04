use nes_types::NesType;
use yaml_rust2::{yaml::Hash, Yaml};

pub fn yaml_data_type(data_type: NesType) -> String {
    match data_type {
        NesType::Undefined => panic!("Fields cannot be undefined"),
        NesType::Bool => "BOOL",
        NesType::Char => "CHAR",
        NesType::Int32 => "INT32",
        NesType::Int64 => "INT64",
        NesType::Float32 => "FLOAT32",
        NesType::Float64 => "FLOAT64",
    }
    .to_string()
}

