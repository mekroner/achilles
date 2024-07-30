use std::path::PathBuf;

use nes_rust_client::prelude::*;

#[derive(Clone)]
pub struct QueryRun {
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
}

pub struct QueryRunResult {
    pub run_id: u32,
    pub origin: QueryResultProps,
    pub others: Vec<QueryResultProps>,
}

pub struct QueryResultProps {
    pub query_props: QueryProps,
    pub status: QueryResultStatus,
}

pub enum QueryResultStatus {
    Success,
    Skipped,
    Failed,
}
