use std::time::Instant;

use nes_rust_client::prelude::*;


pub struct QueryList {
    pub entries: Vec<QueryListEntry>,
}

pub struct QueryListEntry {
    pub origin: QueryExecutionProps,
    pub others: Vec<QueryExecutionProps>,
}

pub struct QueryExecutionProps {
    pub query_id: Option<u64>,
    pub start_time: Option<Instant>,
    pub query: Query,
}
