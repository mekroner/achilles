use std::{fmt::Display, path::PathBuf};

use nes_rust_client::prelude::*;

#[derive(Clone)]
pub struct TestRun {
    pub run_id: u32,
    pub origin: QueryProps,
    pub others: Vec<QueryProps>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub enum LancerQueryId {
    Origin,
    Other(u32),
}

#[derive(Clone)]
pub struct QueryProps {
    pub lancer_query_id: LancerQueryId,
    pub query: Query,
    pub result_path: PathBuf,
    pub status: QueryExecStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryExecStatus {
    Pending,
    Success,
    Failed,
    TimedOut,
}

impl Display for LancerQueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LancerQueryId::Origin => write!(f, "Origin"),
            LancerQueryId::Other(id) => write!(f, "Other {id}"),
        }
    }
}
