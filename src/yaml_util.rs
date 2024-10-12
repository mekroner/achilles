use std::{fs, io::Write, path::Path};

use yaml_rust2::{Yaml, YamlEmitter, YamlLoader};

pub fn store_yaml_array<T>(path: &Path, values: &[T])
where
    for<'a> Yaml: From<&'a T>,
{
    let yaml_test_cases: Vec<Yaml> = values.iter().map(|test_case| test_case.into()).collect();
    let yaml_arr = Yaml::Array(yaml_test_cases);
    let mut out_str = String::new();
    let mut emitter = YamlEmitter::new(&mut out_str);
    emitter.dump(&yaml_arr).unwrap();
    let mut file = fs::File::create(path).expect("coordinator.yml has to be created!");
    write!(file, "{out_str}").unwrap();
}

pub fn load_yaml_array<T>(path: &Path) -> Result<Vec<T>, String>
where
    for<'a> T: TryFrom<&'a Yaml, Error = String>,
{
    let content = fs::read_to_string(path).expect("Should have been able to read the file!");
    let docs = YamlLoader::load_from_str(&content).expect("Should have been able to parse Yaml.");
    let Some(doc) = &docs.first() else {
        return Err(format!("Yaml doc should exist!"));
    };
    let Yaml::Array(arr) = doc else {
        return Err(format!("Should have been able to parse Yaml::Array."));
    };
    arr.iter()
        .map(|yaml_obj| T::try_from(yaml_obj))
        .collect::<Result<Vec<T>, String>>()
}

