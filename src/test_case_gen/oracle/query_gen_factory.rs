use yaml_rust2::Yaml;

use crate::stream_schema::StreamSchema;

use super::aggregation_avg::AggregationAvgQueryGen;
use super::aggregation_count::AggregationCountQueryGen;
use super::aggregation_max::AggregationMaxQueryGen;
use super::aggregation_min::AggregationMinQueryGen;
use super::aggregation_sum::AggregationSumQueryGen;
use super::filter::FilterQueryGen;
use super::key_aggregation_avg::KeyAggregationAvgQueryGen;
use super::key_aggregation_count::KeyAggregationCountQueryGen;
use super::key_aggregation_max::KeyAggregationMaxQueryGen;
use super::key_aggregation_min::KeyAggregationMinQueryGen;
use super::key_aggregation_sum::KeyAggregationSumQueryGen;
use super::map::MapQueryGen;
use super::window_part_avg::WindowPartAverageQueryGen;
use super::window_part_count::WindowPartCountQueryGen;
use super::window_part_max::WindowPartMaxQueryGen;
use super::window_part_min::WindowPartMinQueryGen;
use super::window_part_sum::WindowPartSumQueryGen;
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
    WinPartMin,
    WinPartMax,
    WinPartSum,
    WinPartCount,
    WinPartAvg,
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
            QueryGenStrategy::WinPartMin => "WinPartMin",
            QueryGenStrategy::WinPartMax => "WinPartMax",
            QueryGenStrategy::WinPartSum => "WinPartSum",
            QueryGenStrategy::WinPartCount => "WinPartCount",
            QueryGenStrategy::WinPartAvg => "WinPartAvg",
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
                "WinPartMin" => Ok(QueryGenStrategy::WinPartMin),
                "WinPartMax" => Ok(QueryGenStrategy::WinPartMax),
                "WinPartSum" => Ok(QueryGenStrategy::WinPartSum),
                "WinPartCount" => Ok(QueryGenStrategy::WinPartCount),
                "WinPartAvg" => Ok(QueryGenStrategy::WinPartAvg),
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
            QueryGenStrategy::Filter => Box::new(FilterQueryGen::new(schema)),
            QueryGenStrategy::Map => Box::new(MapQueryGen::new(schema)),
            QueryGenStrategy::AggMin => Box::new(AggregationMinQueryGen::new(schema)),
            QueryGenStrategy::AggMax => Box::new(AggregationMaxQueryGen::new(schema)),
            QueryGenStrategy::AggSum => Box::new(AggregationSumQueryGen::new(schema)),
            QueryGenStrategy::AggCount => Box::new(AggregationCountQueryGen::new(schema)),
            QueryGenStrategy::AggAvg => Box::new(AggregationAvgQueryGen::new(schema)),
            QueryGenStrategy::KeyAggMin => Box::new(KeyAggregationMinQueryGen::new(schema)),
            QueryGenStrategy::KeyAggMax => Box::new(KeyAggregationMaxQueryGen::new(schema)),
            QueryGenStrategy::KeyAggSum =>  Box::new(KeyAggregationSumQueryGen::new(schema)),
            QueryGenStrategy::KeyAggCount => Box::new(KeyAggregationCountQueryGen::new(schema)),
            QueryGenStrategy::KeyAggAvg => Box::new(KeyAggregationAvgQueryGen::new(schema)),
            QueryGenStrategy::WinPartMin => Box::new(WindowPartMinQueryGen::new(schema)),
            QueryGenStrategy::WinPartMax => Box::new(WindowPartMaxQueryGen::new(schema)),
            QueryGenStrategy::WinPartSum => Box::new(WindowPartSumQueryGen::new(schema)),
            QueryGenStrategy::WinPartCount => Box::new(WindowPartCountQueryGen::new(schema)),
            QueryGenStrategy::WinPartAvg => Box::new(WindowPartAverageQueryGen::new(schema)),
        }
    }
}
