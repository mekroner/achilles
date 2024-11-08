use nes_rust_client::prelude::*;

use crate::{
    test_case_gen::util::{generate_predicate, random_source}, stream_gen::LogicalSource, stream_schema::StreamSchema,
};

use super::QueryGen;

pub struct FilterQueryGen {
    // static values
    predicate_depth: u32,
    // dynamic values
    source: LogicalSource,
}

impl FilterQueryGen {
    pub fn with_predicate_depth(mut self, depth: u32) -> Self {
        self.predicate_depth = depth;
        self
    }
}


impl QueryGen for FilterQueryGen {
    fn new(schema: &StreamSchema) -> Self {
        let source = random_source(&schema);
        Self {
            predicate_depth: 3,
            source,
        }
    }

    fn origin(&self) -> QueryBuilder {
        QueryBuilder::from_source(&self.source.source_name)
    }

    fn other(&self) -> QueryBuilder {
        let builder = QueryBuilder::from_source(&self.source.source_name);
        let predicate = generate_predicate(self.predicate_depth, &self.source.fields);
        let query = builder.clone().filter(predicate.clone());
        let query_not = builder.filter(predicate.not());
        query.union(query_not)
    }
}
