use crate::stream_schema::StreamSchema;

use super::aggregation_avg::AggregationAvgOracle;
use super::aggregation_min::AggregationMinOracle;
use super::filter::FilterOracle;
use super::map::MapOracle;
use super::QueryGen;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryGenStrategy {
    Filter,
    Map,
    AggregationMin,
    AggregationMax,
    AggregationSum,
    AggregationCount,
    AggregationAvg,
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
            QueryGenStrategy::Map => Box::new(MapOracle::new(schema)),
            QueryGenStrategy::AggregationMin => Box::new(AggregationMinOracle::new(schema)),
            QueryGenStrategy::AggregationMax => todo!(),
            QueryGenStrategy::AggregationSum => todo!(),
            QueryGenStrategy::AggregationCount => todo!(),
            QueryGenStrategy::AggregationAvg => Box::new(AggregationAvgOracle::new(schema)),
        }
    }
}
