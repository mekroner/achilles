use std::fmt::Display;

use yaml_rust2::Yaml;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum TestCaseId {
    Origin,
    Other(u32),
}

impl Display for TestCaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestCaseId::Origin => write!(f, "Origin"),
            TestCaseId::Other(id) => write!(f, "Other {id}"),
        }
    }
}

impl Into<Yaml> for &TestCaseId {
    fn into(self) -> Yaml {
        match self {
            TestCaseId::Origin => Yaml::String("Origin".into()),
            TestCaseId::Other(id) => Yaml::String(format!("Other{id}")),
        }
    }
}

impl TryFrom<&Yaml> for TestCaseId {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Yaml::String(str) = value else {
            return Err("Failed to parse TestCase: Expected Yaml::String.".into());
        };
        if str == "Origin" {
            return Ok(TestCaseId::Origin);
        }
        let Some(id_str) = str.strip_prefix("Other") else {
            return Err("Failed to parse TestCase: Expected id to start with Origin or Other.".into());
        };
        let Ok(id) = id_str.parse::<u32>() else {
            return Err("Failed to parse TestCase: Unable to parse id.".into());
        };
        Ok(TestCaseId::Other(id))
    }
}
