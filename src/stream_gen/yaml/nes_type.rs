use nes_types::{FloatType, IntType, NesType};
use yaml_rust2::Yaml;

/// NewType to serialize `NesType` to `Yaml` and deserialize `Yaml` to `NesType`.
#[derive(Debug, Clone, Copy)]
pub struct YamlNesType(NesType);

impl From<NesType> for YamlNesType {
    fn from(value: NesType) -> Self {
        YamlNesType(value)
    }
}

impl Into<NesType> for YamlNesType {
    fn into(self) -> NesType {
        self.0
    }
}

impl Into<Yaml> for YamlNesType {
    fn into(self) -> Yaml {
        let str = match self.0 {
            NesType::Undefined => panic!("Fields cannot be undefined"),
            NesType::Bool => "BOOL",
            NesType::Char => "CHAR",
            NesType::Int(t) => int_type_to_str(t),
            NesType::Float(t) => float_type_to_str(t),
        };
        Yaml::String(str.to_string())
    }
}

fn int_type_to_str(t: IntType) -> &'static str {
    match t {
        IntType::Signed8 => "INT8",
        IntType::Unsigned8 => "UINT8",
        IntType::Signed16 => "INT16",
        IntType::Unsigned16 => "UINT16",
        IntType::Signed32 => "INT32",
        IntType::Unsigned32 => "UINT32",
        IntType::Signed64 => "INT64",
        IntType::Unsigned64 => "UINT64",
    }
}

fn str_to_int_type(string: &str) -> Option<IntType> {
    let int_type = match string {
        "INT8" => IntType::Signed8,
        "UINT8" => IntType::Unsigned8,
        "INT16" => IntType::Signed16,
        "UINT16" => IntType::Unsigned16,
        "INT32" => IntType::Signed32,
        "UINT32" => IntType::Unsigned32,
        "INT64" => IntType::Signed64,
        "UINT64" => IntType::Unsigned64,
        _ => return None,
    };
    Some(int_type)
}

fn float_type_to_str(t: FloatType) -> &'static str {
    match t {
        FloatType::Bit32 => "FLOAT32",
        FloatType::Bit64 => "FLOAT64",
    }
}

fn str_to_float_type(string: &str) -> Option<FloatType> {
    let float_type = match string {
        "FLOAT32" => FloatType::Bit32,
        "FLOAT64" => FloatType::Bit64,
        _ => return None,
    };
    Some(float_type)
}

impl TryFrom<&Yaml> for YamlNesType {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Yaml::String(ref str) = value else {
            return Err("Failed to parse YamlNesType: Expected Yaml::String.".into());
        };
        if let Some(int_type) = str_to_int_type(str) {
            return Ok(YamlNesType::from(NesType::Int(int_type)));
        }
        if let Some(float_type) = str_to_float_type(str) {
            return Ok(YamlNesType::from(NesType::Float(float_type)));
        }
        match str.as_str() {
            "BOOL" => return Ok(YamlNesType::from(NesType::Bool)),
            "CHAR" => return Ok(YamlNesType::from(NesType::Char)),
            _ => (),
        }
        Err(format!("Failed to parse YamlNesType: Unknown type {str}."))
    }
}
