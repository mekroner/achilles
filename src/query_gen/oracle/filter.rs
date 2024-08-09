use nes_rust_client::{prelude::*, query::expression::Field};

use crate::{
    query_gen::generate_predicate, stream_gen::LogicalSource, stream_schema::StreamSchema,
};

use super::QueryGen;

pub struct FilterOracle {
    // static values
    predicate_depth: u32,
    // dynamic values
    source: LogicalSource,
}

fn random_source(schema: &StreamSchema) -> LogicalSource {
    use rand::seq::SliceRandom;
    schema
        .logical_sources
        .choose(&mut rand::thread_rng())
        .unwrap()
        .clone()
}

impl QueryGen for FilterOracle {
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
