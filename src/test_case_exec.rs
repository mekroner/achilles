use std::{fs, path::Path};

use nes_rust_client::query::Query;
use std::io::Write;
use yaml_rust2::{yaml::Hash, Yaml, YamlEmitter, YamlLoader};

use crate::{
    query_gen::{query_id::LancerQueryId, test_case::QueryProps},
    LancerConfig,
};

#[derive(Debug, Clone)]
pub struct TestCaseExec {
    pub id: u32,
    pub origin: QueryExecProps,
    pub others: Vec<QueryExecProps>,
}

#[derive(Debug, Clone)]
pub struct QueryExecProps {
    pub query: QueryProps,
    pub status: QueryExecStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryExecStatus {
    Pending,
    Success,
    Failed,
    TimedOut,
}

impl QueryExecProps {
    pub fn from_with(query_props: QueryProps, status: QueryExecStatus) -> Self {
        Self {
            query: query_props,
            status,
        }
    }

    pub fn id(&self) -> LancerQueryId {
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

impl Into<Yaml> for &QueryExecStatus {
    fn into(self) -> Yaml {
        let str = match self {
            QueryExecStatus::Pending => "Pending",
            QueryExecStatus::Success => "Success",
            QueryExecStatus::Failed => "Failed",
            QueryExecStatus::TimedOut => "TimedOut",
        };
        Yaml::from_str(str)
    }
}

impl TryFrom<&Yaml> for QueryExecStatus {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Yaml::String(str) = value else {
            return Err("Failed to parse QueryExecStatus. Expected Yaml::String.".to_string());
        };
        match str.as_str() {
            "Pending" => Ok(QueryExecStatus::Pending),
            "Success" => Ok(QueryExecStatus::Success),
            "Failed" => Ok(QueryExecStatus::Failed),
            "TimedOut" => Ok(QueryExecStatus::TimedOut),
            err => Err(format!(
                "Failed to Parse QueryExecStatus. Unknown state: {err}"
            )),
        }
    }
}

impl Into<Yaml> for &QueryExecProps {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("query".into()), (&self.query).into());
        map.insert(Yaml::String("status".into()), (&self.status).into());
        Yaml::Hash(map)
    }
}

impl TryFrom<&Yaml> for QueryExecProps {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let query = QueryProps::try_from(&value["query"])?;
        let status = QueryExecStatus::try_from(&value["status"])?;
        Ok(Self { query, status, })
    }
}

impl Into<Yaml> for &TestCaseExec {
    fn into(self) -> Yaml {
        let mut map: Hash = Hash::new();
        map.insert(Yaml::String("id".into()), Yaml::Integer(self.id as i64));
        map.insert(Yaml::String("origin".into()), (&self.origin).into());
        let others: Vec<Yaml> = self.others.iter().map(|props| props.into()).collect();
        map.insert(Yaml::String("others".into()), Yaml::Array(others));
        Yaml::Hash(map)
    }
}

impl TryFrom<&Yaml> for TestCaseExec {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Some(id) = value["id"].as_i64() else {
            return Err("Should be able to parse id field.".to_string());
        };
        let origin = (&value["origin"]).try_into()?;
        let Yaml::Array(arr) = &value["others"] else {
            return Err("Should be able to parse others field as Array".to_string());
        };
        let others = arr
            .iter()
            .map(|yaml_obj| QueryExecProps::try_from(yaml_obj))
            .collect::<Result<Vec<_>, Self::Error>>()?;
        Ok(Self {
            id: id as u32,
            origin,
            others,
        })
    }
}

pub fn read_test_case_execs_from_file(config: &LancerConfig) -> Vec<TestCaseExec> {
    let path = config.generated_files_path.join("test_case_execs.yml");
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

pub fn write_test_case_execs_to_file(config: &LancerConfig, test_case_execs: &[TestCaseExec]) {
    let path = config.generated_files_path.join("test_case_execs.yml");
    let yaml_test_cases: Vec<Yaml> = test_case_execs
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
