pub mod filter;
pub mod query_gen_factory;
pub mod map;
pub mod aggregation_min;
pub mod aggregation_max;
pub mod aggregation_sum;
pub mod aggregation_count;
pub mod aggregation_avg;

pub mod key_aggregation_max;
pub mod key_aggregation_min;
pub mod key_aggregation_sum;
pub mod key_aggregation_count;
pub mod key_aggregation_avg;

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
