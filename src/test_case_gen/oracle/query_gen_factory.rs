use yaml_rust2::Yaml;

use crate::stream_schema::StreamSchema;

use super::aggregation_avg::AggregationAvgOracle;
use super::aggregation_count::AggregationCountOracle;
use super::aggregation_max::AggregationMaxOracle;
use super::aggregation_min::AggregationMinOracle;
use super::aggregation_sum::AggregationSumOracle;
use super::filter::FilterOracle;
use super::map::MapOracle;
use super::QueryGen;

#[derive(Hash,Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryGenStrategy {
    Filter,
    Map,
    AggregationMin,
    AggregationMax,
    AggregationSum,
    AggregationCount,
    AggregationAvg,
}

impl Into<Yaml> for &QueryGenStrategy {
    fn into(self) -> Yaml {
        let str = match self {
            QueryGenStrategy::Filter => "Filter",
            QueryGenStrategy::Map => "Map",
            QueryGenStrategy::AggregationMin => "AggregationMin",
            QueryGenStrategy::AggregationMax => "AggregationMax",
            QueryGenStrategy::AggregationSum => "AggregationSum",
            QueryGenStrategy::AggregationCount => "AggregationCount",
            QueryGenStrategy::AggregationAvg => "AggregationAvg",
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
                "AggregationMin" => Ok(QueryGenStrategy::AggregationMin),
                "AggregationMax" => Ok(QueryGenStrategy::AggregationMax),
                "AggregationSum" => Ok(QueryGenStrategy::AggregationSum),
                "AggregationCount" => Ok(QueryGenStrategy::AggregationCount),
                "AggregationAvg" => Ok(QueryGenStrategy::AggregationAvg),
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
            QueryGenStrategy::AggregationMin => Box::new(AggregationMinOracle::new(schema)),
            QueryGenStrategy::AggregationMax => Box::new(AggregationMaxOracle::new(schema)),
            QueryGenStrategy::AggregationSum => Box::new(AggregationSumOracle::new(schema)),
            QueryGenStrategy::AggregationCount => Box::new(AggregationCountOracle::new(schema)),
            QueryGenStrategy::AggregationAvg => Box::new(AggregationAvgOracle::new(schema)),
        }
    }
}
