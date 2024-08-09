use crate::{
    query_gen::generate_predicate, stream_gen::LogicalSource, stream_schema::StreamSchema,
};
use nes_rust_client::{
    prelude::*,
    query::{self, expression::Field},
};

use super::QueryGen;

pub struct AggregationMinOracle {
    // static values
    predicate_depth: u32,
    // dynamic values
    source: LogicalSource,
    window_desc: WindowDescriptor,
    aggregation: Aggregation,
}

fn random_source(schema: &StreamSchema) -> LogicalSource {
    use rand::seq::SliceRandom;
    schema
        .logical_sources
        .choose(&mut rand::thread_rng())
        .unwrap()
        .clone()
}

// TODO: Actually implement this function!!!
fn generate_window_descriptor() -> WindowDescriptor {
    WindowDescriptor::TumblingWindow {
        duration: query::time::Duration::from_minutes(5),
        time_character: query::time::TimeCharacteristic::EventTime {
            field_name: "ts".to_string(),
            unit: query::time::TimeUnit::Minutes,
        },
    }
}

// TODO: Actually implement this function!!!
fn generate_aggregation() -> Aggregation {
    Aggregation::min("value")
}

impl QueryGen for AggregationMinOracle {
    fn new(schema: &StreamSchema) -> Self {
        let source = random_source(&schema);
        let window_desc = generate_window_descriptor();
        let aggregation = generate_aggregation();
        Self {
            predicate_depth: 3,
            source,
            window_desc,
            aggregation,
        }
    }

    fn origin(&self) -> QueryBuilder {
        let builder = QueryBuilder::from_source(&self.source.source_name);
        builder
            .window(self.window_desc.clone())
            .apply([self.aggregation.clone()])
    }

    fn other(&self) -> QueryBuilder {
        let predicate = generate_predicate(self.predicate_depth, &self.source.fields);
        let builder = QueryBuilder::from_source(&self.source.source_name);
        let query = builder
            .clone()
            .filter(predicate.clone())
            .window(self.window_desc.clone())
            .apply([self.aggregation.clone()]);
        let query_not = builder
            .filter(predicate.not())
            .window(self.window_desc.clone())
            .apply([self.aggregation.clone()]);
        query
            .union(query_not)
            .window(self.window_desc.clone())
            .apply([self.aggregation.clone()])
    }
}
