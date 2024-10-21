use crate::{
    stream_gen::LogicalSource,
    stream_schema::StreamSchema,
    test_case_gen::util::{
        generate_predicate, generate_window_descriptor, get_random_field_name, random_source,
    },
};
use nes_rust_client::prelude::*;

use super::QueryGen;

pub struct AggregationAvgQueryGen {
    // static values
    predicate_depth: u32,
    // dynamic values
    source: LogicalSource,
    window_desc: WindowDescriptor,
    agg_field_name: String,
}

impl AggregationAvgQueryGen {
    pub fn with_predicate_depth(mut self, depth: u32) -> Self {
        self.predicate_depth = depth;
        self
    }
}

impl QueryGen for AggregationAvgQueryGen {
    fn new(schema: &StreamSchema) -> Self {
        let source = random_source(&schema);
        let window_desc = generate_window_descriptor();
        let field_name = get_random_field_name(&source);
        Self {
            predicate_depth: 3,
            source,
            window_desc,
            agg_field_name: field_name,
        }
    }

    fn origin(&self) -> QueryBuilder {
        let builder = QueryBuilder::from_source(&self.source.source_name);
        let aggregation = Aggregation::average(self.agg_field_name.clone());
        builder
            .window(self.window_desc.clone())
            .apply([aggregation])
    }

    fn other(&self) -> QueryBuilder {
        let predicate = generate_predicate(self.predicate_depth, &self.source.fields);
        let builder = QueryBuilder::from_source(&self.source.source_name);
        let sum_agg = Aggregation::sum(self.agg_field_name.clone()).as_field("sum");
        let count_agg = Aggregation::count().as_field("count");
        let union_expr = ExprBuilder::field("sum")
            .div(ExprBuilder::field("count"))
            .build_arith()
            .unwrap();

        let query = builder
            .clone()
            .filter(predicate.clone())
            .window(self.window_desc.clone())
            .apply([sum_agg.clone(), count_agg.clone()]);
        let query_not = builder
            .filter(predicate.not())
            .window(self.window_desc.clone())
            .apply([sum_agg.clone(), count_agg.clone()]);
        query
            .union(query_not)
            .project([
                Field::from("start").rename("ts"),
                Field::from("end"),
                Field::from("sum"),
                Field::from("count"),
            ])
            .window(self.window_desc.clone())
            .apply([Aggregation::sum("sum"), Aggregation::sum("count")])
            .map(self.agg_field_name.clone(), union_expr)
            .project([
                Field::untyped("start"),
                Field::untyped("end"),
                Field::untyped(self.agg_field_name.clone()),
            ])
    }
}
