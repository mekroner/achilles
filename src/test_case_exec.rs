use std::{fs, path::Path};

use nes_rust_client::query::Query;
use std::io::Write;
use yaml_rust2::{yaml::Hash, Yaml, YamlEmitter, YamlLoader};

use crate::{
    test_case_gen::{oracle::QueryGenStrategy, query_id::TestCaseId, test_case::TestCase},
    LancerConfig,
};

#[derive(Debug, Clone)]
pub struct TestSetExec {
    pub id: u32,
    pub strategy: QueryGenStrategy,
    pub origin: TestCaseExec,
    pub others: Vec<TestCaseExec>,
}

#[derive(Debug, Clone)]
pub struct TestCaseExec {
    pub query: TestCase,
    pub status: TestCaseExecStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestCaseExecStatus {
    Success,
    Failed(String),
    TimedOut,
    Skipped,
}

impl TestCaseExec {
    pub fn from_with(query_props: TestCase, status: TestCaseExecStatus) -> Self {
        Self {
            query: query_props,
            status,
        }
    }

    pub fn id(&self) -> TestCaseId {
        self.query.id()
    }
    pub fn query(&self) -> &Query {
        self.query.query()
    }
    pub fn result_path(&self) -> &Path {
        self.query.result_path()
    }
}

// yaml and that jazz

impl Into<Yaml> for &TestCaseExecStatus {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        let str = match self {
            TestCaseExecStatus::Skipped => "Skipped",
            TestCaseExecStatus::Success => "Success",
            TestCaseExecStatus::Failed(_) => "Failed",
            TestCaseExecStatus::TimedOut => "TimedOut",
        };
        map.insert(Yaml::String("status".into()), Yaml::from_str(str));
        if let TestCaseExecStatus::Failed(reason) = self {
            map.insert(Yaml::String("reason".into()), Yaml::from_str(reason));
        }
        Yaml::Hash(map)
    }
}

impl TryFrom<&Yaml> for TestCaseExecStatus {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Yaml::String(ref status) = value["status"] else {
            return Err("Failed to parse QueryExecStatus. Expected Yaml::String.".to_string());
        };
        match status.as_str() {
            "Skipped" => Ok(TestCaseExecStatus::Skipped),
            "Success" => Ok(TestCaseExecStatus::Success),
            "Failed" => {
                let Yaml::String(ref reason) = value["reason"] else {
                    return Err(
                        "Failed to parse QueryExecStatus::Failed. Unable to pase reason.".into(),
                    );
                };
                Ok(TestCaseExecStatus::Failed(reason.into()))
            }
            "TimedOut" => Ok(TestCaseExecStatus::TimedOut),
            err => Err(format!(
                "Failed to Parse QueryExecStatus. Unknown state: {err}"
            )),
        }
    }
}

impl Into<Yaml> for &TestCaseExec {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("query".into()), (&self.query).into());
        map.insert(Yaml::String("status".into()), (&self.status).into());
        Yaml::Hash(map)
    }
}

impl TryFrom<&Yaml> for TestCaseExec {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let query = TestCase::try_from(&value["query"])?;
        let status = TestCaseExecStatus::try_from(&value["status"])?;
        Ok(Self { query, status })
    }
}

impl Into<Yaml> for &TestSetExec {
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

impl TryFrom<&Yaml> for TestSetExec {
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
            .map(|yaml_obj| TestCaseExec::try_from(yaml_obj))
            .collect::<Result<Vec<_>, Self::Error>>()?;
        Ok(Self {
            id: id as u32,
            strategy,
            origin,
            others,
        })
    }
}

pub fn read_test_set_execs_from_file(test_run_id: u32, config: &LancerConfig) -> Vec<TestSetExec> {
    let path = config.path_config.test_set_execs(test_run_id);
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

pub fn write_test_set_execs_to_file(
    test_run_id: u32,
    config: &LancerConfig,
    test_case_execs: &[TestSetExec],
) {
    let path = config.path_config.test_set_execs(test_run_id);
    let yaml_test_cases: Vec<Yaml> = test_case_execs
        .iter()
        .map(|test_case| test_case.into())
        .collect();
    let yaml_arr = Yaml::Array(yaml_test_cases);
    let mut out_str = String::new();
    let mut emitter = YamlEmitter::new(&mut out_str);
    emitter.dump(&yaml_arr).unwrap();
    let mut file = fs::File::create(path).expect("test_set_execs.yml has to be created!");
    write!(file, "{out_str}").unwrap();
}
