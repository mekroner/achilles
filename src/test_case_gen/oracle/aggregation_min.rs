use crate::{
    test_case_gen::util::{
        generate_aggregation, generate_predicate, generate_window_descriptor, random_source,
    },
    stream_gen::LogicalSource,
    stream_schema::StreamSchema,
};
use nes_rust_client::{prelude::*, query::time::{Duration, TimeCharacteristic, TimeUnit}};

use super::QueryGen;

pub struct AggregationMinOracle {
    // static values
    predicate_depth: u32,
    // dynamic values
    source: LogicalSource,
    window_desc: WindowDescriptor,
    aggregation: Aggregation,
}

impl QueryGen for AggregationMinOracle {
    fn new(schema: &StreamSchema) -> Self {
        let source = random_source(&schema);
        let window_desc = generate_window_descriptor();
        let aggregation = generate_aggregation(&source);
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
        let union_window = WindowDescriptor::TumblingWindow {
            duration: Duration::from_minutes(5),
            time_character: TimeCharacteristic::EventTime {
                field_name: "start".to_string(),
                unit: TimeUnit::Milliseconds,
            },
        };

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
            .window(union_window)
            .apply([self.aggregation.clone()])
    }
}
