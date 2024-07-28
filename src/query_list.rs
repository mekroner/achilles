use std::{path::PathBuf, time::Instant};

use nes_rust_client::prelude::*;

pub struct QueryList {
    pub entries: Vec<QueryListEntry>,
}

pub struct QueryListEntry {
    pub origin: QueryExecutionProps,
    pub others: Vec<QueryExecutionProps>,
}

pub struct QueryRun {
    pub run_id: u32,
    pub origin: QueryProps,
    pub others: Vec<QueryProps>,
}

pub struct QueryProps {
    interal_id: u32,
    query: Query,
    result_path: PathBuf,
}

pub struct QueryExecProps {
    pub query_id: i64,
    pub start_time: Instant,
    pub query: Query,
}

pub struct QueryRunResult {}

pub struct QueryExecutionProps {
    pub query_id: Option<i64>,
    pub start_time: Option<Instant>,
    pub result_path: PathBuf,
    pub query: Query,
}
