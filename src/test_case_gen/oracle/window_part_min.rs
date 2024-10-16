use crate::{
    stream_gen::LogicalSource,
    stream_schema::StreamSchema,
    test_case_gen::util::{
        generate_inner_window, generate_outer_window, generate_window_descriptor, get_random_field_name, random_source
    },
};
use nes_rust_client::prelude::*;

use super::QueryGen;

pub struct WindowPartMinQueryGen {
    // static values
    // dynamic values
    source: LogicalSource,
    outer_window: WindowDescriptor,
    agg_field_name: String,
}

impl QueryGen for WindowPartMinQueryGen {
    fn new(schema: &StreamSchema) -> Self {
        let source = random_source(&schema);
        let outer_window = generate_outer_window();
        let agg_field_name = get_random_field_name(&source);
        Self {
            source,
            outer_window,
            agg_field_name,
        }
    }

    fn origin(&self) -> QueryBuilder {
        let builder = QueryBuilder::from_source(&self.source.source_name);
        builder
            .window(self.outer_window.clone())
            .apply([Aggregation::min(self.agg_field_name.clone())])
    }

    fn other(&self) -> QueryBuilder {
        let inner_window = generate_inner_window(&self.outer_window);
        QueryBuilder::from_source(&self.source.source_name)
            .window(inner_window.clone())
            .apply([Aggregation::min(self.agg_field_name.clone())])
            .project([
                Field::from("start").rename("ts"),
                Field::from("end"),
                Field::from(self.agg_field_name.clone()),
            ])
            .window(self.outer_window.clone())
            .apply([Aggregation::min(self.agg_field_name.clone())])
    }
}
