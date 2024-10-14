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

pub struct AggregationCountOracle {
    // static values
    predicate_depth: u32,
    // dynamic values
    source: LogicalSource,
    window_desc: WindowDescriptor,
}

impl QueryGen for AggregationCountOracle {
    fn new(schema: &StreamSchema) -> Self {
        let source = random_source(&schema);
        let window_desc = generate_window_descriptor();
        Self {
            predicate_depth: 3,
            source,
            window_desc,
        }
    }

    fn origin(&self) -> QueryBuilder {
        let builder = QueryBuilder::from_source(&self.source.source_name);
        builder
            .window(self.window_desc.clone())
            .apply([Aggregation::count()])
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
            .apply([Aggregation::count()]);
        let query_not = builder
            .filter(predicate.not())
            .window(self.window_desc.clone())
            .apply([Aggregation::count()]);
        query
            .union(query_not)
            .window(union_window)
            .apply([Aggregation::sum("count")])
    }
}
