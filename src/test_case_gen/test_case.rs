use std::io::Write;
use std::{
    fs,
    path::{self, Path, PathBuf},
};

use nes_rust_client::query::{stringify::stringify_query, Query};
use yaml_rust2::YamlLoader;
use yaml_rust2::{yaml::Hash, Yaml, YamlEmitter};

use crate::LancerConfig;

use super::oracle::QueryGenStrategy;
use super::query_id::TestCaseId;

///
#[derive(Debug, Clone)]
pub struct TestSet {
    pub id: u32,
    pub strategy: QueryGenStrategy,
    pub origin: TestCase,
    pub others: Vec<TestCase>,
}

impl TestSet {
    pub fn test_case(&self, id: TestCaseId) -> Option<&TestCase> {
        let mut iter = std::iter::once(&self.origin).chain(self.others.iter());
        iter.find(|test_case| test_case.id == id)
    }
}

#[derive(Debug, Clone)]
pub struct TestCase {
    pub id: TestCaseId,
    pub query: Query,
    pub result_path: PathBuf,
}

impl TestCase {
    /// Shorthand initalizer for `QueryProps`
    pub fn new(lancer_query_id: TestCaseId, query: Query, result_path: PathBuf) -> Self {
        Self {
            id: lancer_query_id,
            query,
            result_path,
        }
    }

    /// Shorthand initalizer for the origin `QueryProps`
    pub fn origin(query: Query, result_path: PathBuf) -> Self {
        Self::new(TestCaseId::Origin, query, result_path)
    }

    /// Shorthand initalizer for the other `QueryProps`
    pub fn other(id: u32, query: Query, result_path: PathBuf) -> Self {
        Self::new(TestCaseId::Other(id), query, result_path)
    }

    pub fn id(&self) -> TestCaseId {
        self.id
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn result_path(&self) -> &Path {
        &self.result_path
    }
}

// Yaml and that jazz
/// Writes the `TestCase`s to the in `LancerConfig` specified location.
pub fn write_test_sets_to_file(test_run_id: u32, config: &LancerConfig, test_cases: &[TestSet]) {
    let path = config.path_config.test_sets(test_run_id);
    let yaml_test_cases: Vec<Yaml> = test_cases
        .iter()
        .map(|test_case| test_case.into())
        .collect();
    let yaml_arr = Yaml::Array(yaml_test_cases);
    let mut out_str = String::new();
    let mut emitter = YamlEmitter::new(&mut out_str);
    emitter.dump(&yaml_arr).unwrap();
    let mut file = fs::File::create(path).expect("coordinator.yml has to be created!");
    write!(file, "{out_str}").unwrap();
}

/// Reads the `TestCase`s to the in `LancerConfig` specified location.
pub fn read_test_sets_to_file(test_run_id: u32, config: &LancerConfig) -> Vec<TestSet> {
    let path = config.path_config.test_sets(test_run_id);
    let content = fs::read_to_string(path).expect("Should have been able to read the file!");
    let docs = YamlLoader::load_from_str(&content).expect("Should have been able to parse Yaml.");
    let doc = &docs.first().expect("Should have one element");
    let Yaml::Array(arr) = doc else {
        panic!("Should have been able to parse Yaml::Array.")
    };
    arr.iter()
        .map(|yaml_obj| yaml_obj.try_into().expect("Should not fail to parse Yaml."))
        .collect()
}

impl Into<Yaml> for &TestSet {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("id".into()), Yaml::Integer(self.id as i64));
        map.insert(Yaml::String("strategy".into()), (&self.strategy).into());
        map.insert(Yaml::String("origin".into()), (&self.origin).into());
        let others: Vec<Yaml> = self.others.iter().map(|props| props.into()).collect();
        map.insert(Yaml::String("others".into()), Yaml::Array(others));
        Yaml::Hash(map)
    }
}

impl TryFrom<&Yaml> for TestSet {
    type Error = String;
    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Some(id) = value["id"].as_i64() else {
            return Err("Should be able to parse id field.".to_string());
        };
        let strategy = (&value["strategy"]).try_into()?;
        let origin = (&value["origin"]).try_into()?;
        let Yaml::Array(arr) = &value["others"] else {
            return Err("Should be able to parse others field as Array".to_string());
        };
        let others = arr
            .iter()
            .map(|yaml_obj| TestCase::try_from(yaml_obj))
            .collect::<Result<Vec<_>, Self::Error>>()?;
        Ok(Self {
            id: id as u32,
            strategy,
            origin,
            others,
        })
    }
}

impl Into<Yaml> for &TestCase {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("id".into()), (&self.id).into());
        map.insert(
            Yaml::String("query_string".into()),
            Yaml::String(stringify_query(self.query())),
        );
        map.insert(
            Yaml::String("query_ron".into()),
            Yaml::String(ron::to_string(self.query()).expect("Should not fail")),
        );
        map.insert(
            Yaml::String("result_path".into()),
            Yaml::String(self.result_path().to_string_lossy().to_string()),
        );
        Yaml::Hash(map)
    }
}

impl TryFrom<&Yaml> for TestCase {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let id: TestCaseId = (&value["id"]).try_into()?;
        let Yaml::String(ref query_str) = value["query_ron"] else {
            return Err("Could not parse query string.".to_string());
        };
        let Ok(query) = ron::from_str(query_str) else {
            return Err("Unable to parse RON.".to_string());
        };
        let Yaml::String(ref path_str) = value["result_path"] else {
            return Err("Unable to parse result_path.".to_string());
        };
        let test_case = Self {
            id,
            query,
            result_path: PathBuf::from(path_str),
        };
        Ok(test_case)
    }
}

#[cfg(test)]
mod yaml_tests {
    use std::{fmt::Debug, path::PathBuf};

    use nes_rust_client::{
        prelude::{ExprBuilder as EB, *},
        query::time::{Duration, TimeCharacteristic, TimeUnit},
    };
    use yaml_rust2::{Yaml, YamlEmitter, YamlLoader};

    use crate::test_case_gen::test_case::TestCase;

    use super::TestSet;

    fn common_queries() -> Vec<Query> {
        let sink = Sink::csv_file("./result.csv", false);
        let query0 = QueryBuilder::from_source("test");
        let query1 = QueryBuilder::from_source("test").filter(
            EB::field("test1")
                .less_than(EB::field("test0"))
                .build_logical()
                .unwrap(),
        );
        let query2 = QueryBuilder::from_source("test")
            .window(WindowDescriptor::TumblingWindow {
                duration: Duration::from_milliseconds(100),
                time_character: TimeCharacteristic::EventTime {
                    field_name: "name".into(),
                    unit: TimeUnit::Milliseconds,
                },
            })
            .apply([Aggregation::sum("test0")]);
        vec![
            query0.sink(sink.clone()),
            query1.sink(sink.clone()),
            query2.sink(sink.clone()),
        ]
    }

    fn yaml_helper<O>(obj: &O) -> (String, String)
    where
        O: std::fmt::Debug + for<'a> TryFrom<&'a Yaml>,
        for<'a> &'a O: Into<Yaml>,
        for<'a> <O as TryFrom<&'a Yaml>>::Error: std::fmt::Debug,
    {
        // to string
        let yaml_obj: Yaml = obj.into();
        let mut out_str = String::new();
        let mut emitter = YamlEmitter::new(&mut out_str);
        emitter.dump(&yaml_obj).unwrap();
        println!("{}", &out_str);
        // to test_case
        let yaml_obj = &YamlLoader::load_from_str(&out_str).unwrap()[0];
        let deser_test_case = O::try_from(yaml_obj).unwrap();
        (format!("{:?}", obj), format!("{:?}", deser_test_case))
    }

    #[test]
    fn yaml_query_case() {
        let queries = common_queries();
        for (i, query) in queries.iter().enumerate() {
            let path = PathBuf::from("./result.csv");
            let props = if i == 0 {
                TestCase::origin(query.clone(), path)
            } else {
                TestCase::other(i as u32, query.clone(), path)
            };
            let (expected, result) = yaml_helper(&props);
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn yaml_test_set() {
        let queries = common_queries();
        let path = PathBuf::from("./result.csv");
        let mut iter = queries.iter();
        let q_origin = iter.next().unwrap();
        let origin = TestCase::origin(q_origin.clone(), path.clone());
        let others = iter
            .enumerate()
            .map(|(i, q_other)| TestCase::other(i as u32, q_other.clone(), path.clone()))
            .collect();
        let test_set = TestSet {
            id: 42,
            origin,
            others,
        };
        let (expected, result) = yaml_helper(&test_set);
        assert_eq!(expected, result);
    }
}
