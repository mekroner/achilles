use crate::{
    stream_gen::LogicalSource,
    stream_schema::StreamSchema,
    test_case_gen::util::{
        generate_predicate, generate_window_descriptor, get_random_field_name, random_source,
    },
};
use nes_rust_client::prelude::*;

use super::QueryGen;

pub struct KeyAggregationMinOracle {
    // static values
    predicate_depth: u32,
    // dynamic values
    source: LogicalSource,
    window_desc: WindowDescriptor,
    agg_field_name: String,
}

impl QueryGen for KeyAggregationMinOracle {
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
            .by_key("key")
            .apply([Aggregation::min(self.agg_field_name.clone())])
    }

    fn other(&self) -> QueryBuilder {
        let predicate = generate_predicate(self.predicate_depth, &self.source.fields);
        let builder = QueryBuilder::from_source(&self.source.source_name);

        let query = builder
            .clone()
            .filter(predicate.clone())
            .window(self.window_desc.clone())
            .by_key("key")
            .apply([Aggregation::min(self.agg_field_name.clone())]);
        let query_not = builder
            .filter(predicate.not())
            .window(self.window_desc.clone())
            .by_key("key")
            .apply([Aggregation::min(self.agg_field_name.clone())]);
        query
            .union(query_not)
            .project([
                Field::from("start").rename("ts"),
                Field::from("end"),
                Field::from("key"),
                Field::from(self.agg_field_name.clone()),
            ])
            .window(self.window_desc.clone())
            .by_key("key")
            .apply([Aggregation::min(self.agg_field_name.clone())])
    }
}
