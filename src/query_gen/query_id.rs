use std::fmt::Display;

use yaml_rust2::Yaml;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum LancerQueryId {
    Origin,
    Other(u32),
}

impl Display for LancerQueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LancerQueryId::Origin => write!(f, "Origin"),
            LancerQueryId::Other(id) => write!(f, "Other {id}"),
        }
    }
}

impl Into<Yaml> for &LancerQueryId {
    fn into(self) -> Yaml {
        match self {
            LancerQueryId::Origin => Yaml::String("Origin".into()),
            LancerQueryId::Other(id) => Yaml::String(format!("Other{id}")),
        }
    }
}

impl TryFrom<&Yaml> for LancerQueryId {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Yaml::String(str) = value else {
            return Err("Failed to parse LancerQueryId. Expected Yaml::String.".into());
        };
        if str == "Origin" {
            return Ok(LancerQueryId::Origin);
        }
        let Some(id_str) = str.strip_prefix("Other") else {
            return Err("Failed to parse LancerQueryId. Expected id to start with Origin or Other.".into());
        };
        let Ok(id) = id_str.parse::<u32>() else {
            return Err("Failed to parse LancerQueryId. Unable to parse id.".into());
        };
        Ok(LancerQueryId::Other(id))
    }
}
