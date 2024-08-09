use crate::stream_schema::StreamSchema;

use super::aggregation_min::AggregationMinOracle;
use super::filter::FilterOracle;
use super::QueryGen;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryGenStrategy {
    Filter,
    AggregationMin,
}

pub struct QueryGenFactory {}

impl QueryGenFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_query_gen(
        &self,
        schema: &StreamSchema,
        strat: QueryGenStrategy,
    ) -> Box<dyn QueryGen> {
        match strat {
            QueryGenStrategy::Filter => Box::new(FilterOracle::new(schema)),
            QueryGenStrategy::AggregationMin => Box::new(AggregationMinOracle::new(schema)),
        }
    }
}
