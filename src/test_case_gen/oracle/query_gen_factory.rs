use yaml_rust2::Yaml;

use crate::stream_schema::StreamSchema;

use super::aggregation_avg::AggregationAvgOracle;
use super::aggregation_count::AggregationCountOracle;
use super::aggregation_max::AggregationMaxOracle;
use super::aggregation_min::AggregationMinOracle;
use super::aggregation_sum::AggregationSumOracle;
use super::filter::FilterOracle;
use super::key_aggregation_avg::KeyAggregationAvgOracle;
use super::key_aggregation_count::KeyAggregationCountOracle;
use super::key_aggregation_max::KeyAggregationMaxOracle;
use super::key_aggregation_min::KeyAggregationMinOracle;
use super::key_aggregation_sum::KeyAggregationSumOracle;
use super::map::MapOracle;
use super::QueryGen;

#[derive(Hash, Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryGenStrategy {
    Filter,
    Map,
    AggMin,
    AggMax,
    AggSum,
    AggCount,
    AggAvg,
    KeyAggMin,
    KeyAggMax,
    KeyAggSum,
    KeyAggCount,
    KeyAggAvg,
}

impl Into<Yaml> for &QueryGenStrategy {
    fn into(self) -> Yaml {
        let str = match self {
            QueryGenStrategy::Filter => "Filter",
            QueryGenStrategy::Map => "Map",
            QueryGenStrategy::AggMin => "AggMin",
            QueryGenStrategy::AggMax => "AggMax",
            QueryGenStrategy::AggSum => "AggSum",
            QueryGenStrategy::AggCount => "AggCount",
            QueryGenStrategy::AggAvg => "AggAvg",
            QueryGenStrategy::KeyAggMin => "KeyAggMin",
            QueryGenStrategy::KeyAggMax => "KeyAggMax",
            QueryGenStrategy::KeyAggSum => "KeyAggSum",
            QueryGenStrategy::KeyAggCount => "KeyAggCount",
            QueryGenStrategy::KeyAggAvg => "KeyAggAvg",
        };
        Yaml::String(str.to_string())
    }
}

impl TryFrom<&Yaml> for QueryGenStrategy {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        if let Yaml::String(s) = value {
            match s.as_str() {
                "Filter" => Ok(QueryGenStrategy::Filter),
                "Map" => Ok(QueryGenStrategy::Map),
                "AggMin" => Ok(QueryGenStrategy::AggMin),
                "AggMax" => Ok(QueryGenStrategy::AggMax),
                "AggSum" => Ok(QueryGenStrategy::AggSum),
                "AggCount" => Ok(QueryGenStrategy::AggCount),
                "AggAvg" => Ok(QueryGenStrategy::AggAvg),
                "KeyAggMin" => Ok(QueryGenStrategy::KeyAggMin),
                "KeyAggMax" => Ok(QueryGenStrategy::KeyAggMax),
                "KeyAggSum" => Ok(QueryGenStrategy::KeyAggSum),
                "KeyAggCount" => Ok(QueryGenStrategy::KeyAggCount),
                "KeyAggAvg" => Ok(QueryGenStrategy::KeyAggAvg),
                _ => Err(format!("Unknown strategy: {}", s)),
            }
        } else {
            Err("Expected a YAML string".to_string())
        }
    }
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
            QueryGenStrategy::AggMin => Box::new(AggregationMinOracle::new(schema)),
            QueryGenStrategy::AggMax => Box::new(AggregationMaxOracle::new(schema)),
            QueryGenStrategy::AggSum => Box::new(AggregationSumOracle::new(schema)),
            QueryGenStrategy::AggCount => Box::new(AggregationCountOracle::new(schema)),
            QueryGenStrategy::AggAvg => Box::new(AggregationAvgOracle::new(schema)),
            QueryGenStrategy::KeyAggMin => Box::new(KeyAggregationMinOracle::new(schema)),
            QueryGenStrategy::KeyAggMax => Box::new(KeyAggregationMaxOracle::new(schema)),
            QueryGenStrategy::KeyAggSum =>  Box::new(KeyAggregationSumOracle::new(schema)),
            QueryGenStrategy::KeyAggCount => Box::new(KeyAggregationCountOracle::new(schema)),
            QueryGenStrategy::KeyAggAvg => Box::new(KeyAggregationAvgOracle::new(schema)),
        }
    }
}
