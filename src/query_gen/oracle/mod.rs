pub mod aggregation_min;
pub mod filter;
pub mod query_gen_factory;
pub mod map;

pub use query_gen_factory::{QueryGenFactory, QueryGenStrategy};

use nes_rust_client::prelude::*;
use crate::stream_schema::StreamSchema;

pub trait QueryGen {
    fn new(schema: &StreamSchema) -> Self
    where
        Self: Sized;
    fn origin(&self) -> QueryBuilder;
    fn other(&self) -> QueryBuilder;
}
