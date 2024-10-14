use crate::{
    stream_gen::LogicalSource,
    stream_schema::StreamSchema,
    test_case_gen::util::{
        generate_predicate, generate_window_descriptor, get_random_field_name, random_source,
    },
};
use nes_rust_client::{
    prelude::*,
    query::time::{Duration, TimeCharacteristic, TimeUnit},
};

use super::QueryGen;

pub struct AggregationMaxOracle {
    // static values
    predicate_depth: u32,
    // dynamic values
    source: LogicalSource,
    window_desc: WindowDescriptor,
    agg_field_name: String,
}

impl QueryGen for AggregationMaxOracle {
    fn new(schema: &StreamSchema) -> Self {
        let source = random_source(&schema);
        let window_desc = generate_window_descriptor();
        let agg_field_name = get_random_field_name(&source);
        Self {
            predicate_depth: 3,
            source,
            window_desc,
            agg_field_name,
        }
    }

    fn origin(&self) -> QueryBuilder {
        let builder = QueryBuilder::from_source(&self.source.source_name);
        builder
            .window(self.window_desc.clone())
            .apply([Aggregation::max(self.agg_field_name.clone())])
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
            .apply([Aggregation::max(self.agg_field_name.clone())]);
        let query_not = builder
            .filter(predicate.not())
            .window(self.window_desc.clone())
            .apply([Aggregation::max(self.agg_field_name.clone())]);
        query
            .union(query_not)
            .window(union_window)
            .apply([Aggregation::max(self.agg_field_name.clone())])
    }
}
