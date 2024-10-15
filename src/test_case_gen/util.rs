use crate::expr_gen::expr_gen::generate_logical_expr;
use nes_rust_client::expression::binary_expression::BinaryExpr;
use nes_rust_client::expression::expression::RawExpr;
use nes_rust_client::expression::Field;
use nes_rust_client::expression::LogicalExpr;
use nes_rust_client::query;
use nes_rust_client::query::stringify::stringify_expr;
use nes_rust_client::query::window::window_descriptor::WindowDescriptor;
use nes_types::NesType;
use rand::Rng;

use crate::stream_gen::LogicalSource;
use crate::stream_schema::StreamSchema;

fn has_literal_literal(logical_expr: &LogicalExpr) -> bool {
    let parents = logical_expr.0.leaf_parents();
    for expr in parents {
        let RawExpr::Binary(BinaryExpr { lhs, rhs, .. }) = expr else {
            continue;
        };
        if let (RawExpr::Literal(_), RawExpr::Literal(_)) = (*lhs, *rhs) {
            return true;
        }
    }
    false
}

fn contains_boolean_literal(logical_expr: &LogicalExpr) -> bool {
    logical_expr.0.traverse_and_check(|expr| {
        if let RawExpr::Literal(lit) = expr {
            if lit.data_type() == NesType::bool() {
                return true;
            }
        }
        false
    })
}

fn is_literal(logical_expr: &LogicalExpr) -> bool {
    if let RawExpr::Literal(_) = logical_expr.0 {
        return true;
    }
    false
}

pub fn generate_predicate(depth: u32, fields: &[Field]) -> LogicalExpr {
    loop {
        let Ok(p) = generate_logical_expr(depth, &fields) else {
            continue;
        };
        if has_literal_literal(&p) || is_literal(&p) || contains_boolean_literal(&p) {
            log::debug!(
                "Skipping predicate {} due to literal literal pattern.",
                stringify_expr(&p.0)
            );
            continue;
        }
        break p;
    }
}

pub fn random_source(schema: &StreamSchema) -> LogicalSource {
    use rand::seq::SliceRandom;
    schema
        .logical_sources
        .choose(&mut rand::thread_rng())
        .unwrap()
        .clone()
}

// TODO: Actually implement this function!!!
pub fn generate_window_descriptor() -> WindowDescriptor {
    let mut rng = rand::thread_rng();
    let dur = rng.gen_range(200..10_000);
    WindowDescriptor::TumblingWindow {
        duration: query::time::Duration::from_milliseconds(dur),
        time_character: query::time::TimeCharacteristic::EventTime {
            field_name: "ts".to_string(),
            unit: query::time::TimeUnit::Milliseconds,
        },
    }
}

/// returns a random field that is not the ts
pub fn get_random_field_name(source: &LogicalSource) -> String {
    use rand::seq::IteratorRandom;
    let mut rng = rand::thread_rng();
    let field = source
        .fields
        .iter()
        .filter(|field| field.name() != "ts" || field.name() != "key")
        .choose(&mut rng)
        .expect("Expect to get random field.");
    field.name().to_string()
}

// TODO: Actually implement this function!!!
// pub fn generate_aggregation(source: &LogicalSource) -> Aggregation {
//     use rand::seq::IteratorRandom;
//     let mut rng = rand::thread_rng();
//     let field = source
//         .fields
//         .iter()
//         .filter(|field| field.name() != "ts")
//         .choose(&mut rng)
//         .expect("Expect to get random field.");
//     Aggregation::min(field.name())
// }
