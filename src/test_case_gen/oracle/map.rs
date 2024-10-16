use crate::expr_gen::expr_gen::generate_raw_expr;
use crate::{
    test_case_gen::util::{generate_predicate, random_source},
    stream_gen::LogicalSource,
    stream_schema::StreamSchema,
};
use nes_rust_client::{
    expression::{ArithmeticExpr, Field},
    prelude::*,
};

use super::QueryGen;

pub struct MapQueryGen {
    // static values
    predicate_depth: u32,
    // dynamic values
    source: LogicalSource,
    expr: ArithmeticExpr,
}

fn generate_arithmetic_expr(fields: &[Field]) -> ArithmeticExpr {
    // ExprBuilder::field("value").build_arith().unwrap()
    ArithmeticExpr(loop {
        let Ok(p) = generate_raw_expr(1, &fields, nes_types::NesType::i64()) else {
            continue;
        };
        break p;
    })
}

impl QueryGen for MapQueryGen {
    fn new(schema: &StreamSchema) -> Self {
        let source = random_source(schema);
        let expr = generate_arithmetic_expr(&source.fields);
        Self {
            predicate_depth: 1,
            source,
            expr,
        }
    }

    fn origin(&self) -> QueryBuilder {
        let builder = QueryBuilder::from_source(&self.source.source_name);
        builder.map("new_value", self.expr.clone())
    }

    fn other(&self) -> QueryBuilder {
        let predicate = generate_predicate(self.predicate_depth, &self.source.fields);
        let builder = QueryBuilder::from_source(&self.source.source_name);
        let query = builder
            .clone()
            .filter(predicate.clone())
            .map("new_value", self.expr.clone());
        let query_not = builder
            .filter(predicate.not())
            .map("new_value", self.expr.clone());
        query.union(query_not)
    }
}
