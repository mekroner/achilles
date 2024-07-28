use std::path::PathBuf;

use crate::QueryListEntry;
use crate::{query_list::QueryExecutionProps, LancerConfig, QueryList};
use nes_rust_client::prelude::*;
use nes_rust_client::query::expr_gen::expr_gen::generate_logical_expr;
use nes_rust_client::query::expression::binary_expression::BinaryExpr;
use nes_rust_client::query::expression::expression::RawExpr;
use nes_rust_client::query::expression::Field;
use nes_rust_client::query::expression::LogicalExpr;
use nes_rust_client::query::stringify::stringify_expr;
use nes_types::NesType;

pub fn generate_queries(config: &LancerConfig) -> QueryList {
    log::info!("Started generating queries:");
    let entries = (0..2)
        .map(|run_id| {
            let origin_path = config
                .generated_files_path
                .join(format!("result-run{run_id}-origin.csv"));
            let origin_sink = Sink::csv_file(&origin_path, false);
            let origin = QueryExecutionProps {
                query_id: None,
                start_time: None,
                query: query_origin_filter(origin_sink),
                result_path: PathBuf::from(origin_path),
            };
            let others = (0..10)
                .map(|id| {
                    let other_path = config
                        .generated_files_path
                        .join(format!("result-run{run_id}-other{id}.csv"));
                    let other_sink = Sink::csv_file(&other_path, false);
                    QueryExecutionProps {
                        query_id: None,
                        start_time: None,
                        query: query_part_filter(other_sink),
                        result_path: PathBuf::from(other_path),
                    }
                })
                .collect();
            QueryListEntry { origin, others }
        })
        .collect();
    log::info!("Generatong queries done!");
    QueryList { entries }
}

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

fn generate_predicate(depth: u32, fields: &[Field]) -> LogicalExpr {
    loop {
        let Ok(p) = generate_logical_expr(depth, &fields) else {
            continue;
        };
        if has_literal_literal(&p) {
            log::debug!("Skipping predicate {} due to literal literal pattern.", stringify_expr(&p.0));
            continue;
        }
        break p;
    }
}

fn query_origin_filter(sink: Sink) -> Query {
    QueryBuilder::from_source("test").sink(sink)
}

fn query_part_filter(sink: Sink) -> Query {
    let fields = [
        Field::typed("value", NesType::Int64),
        Field::typed("id", NesType::Int64),
    ];
    let predicate = generate_predicate(1, &fields);

    let query = QueryBuilder::from_source("test").filter(predicate.clone());
    let query_not = QueryBuilder::from_source("test").filter(predicate.not());
    query.union(query_not).sink(sink)
}

// fn query_min() -> (Query, Query) {
//     let sink0 = Sink::csv_file("./generated_files/result-0.csv", false);
//     let sink1 = Sink::csv_file("./generated_files/result-1.csv", false);

//     let query0 = QueryBuilder::from_source("test")
//         .window(WindowDescriptor::TumblingWindow {
//             duration: query::time::Duration::from_minutes(5),
//             time_character: TimeCharacteristic::EventTime {
//                 field_name: "ts".to_string(),
//                 unit: query::time::TimeUnit::Minutes,
//             },
//         })
//         .apply([Aggregation::min("value")])
//         .sink(sink0);

//     let query_sub = QueryBuilder::from_source("test")
//         .filter(
//             EB::field("value")
//                 .greater_than(EB::literal(0))
//                 .not()
//                 .build_logical()
//                 .unwrap(),
//         )
//         .window(WindowDescriptor::TumblingWindow {
//             duration: query::time::Duration::from_minutes(5),
//             time_character: TimeCharacteristic::EventTime {
//                 field_name: "ts".to_string(),
//                 unit: query::time::TimeUnit::Minutes,
//             },
//         })
//         .apply([Aggregation::min("value")]);
//     let query1 = QueryBuilder::from_source("test")
//         .filter(
//             EB::field("value")
//                 .greater_than(EB::literal(0))
//                 .build_logical()
//                 .unwrap(),
//         )
//         .window(WindowDescriptor::TumblingWindow {
//             duration: query::time::Duration::from_minutes(5),
//             time_character: TimeCharacteristic::EventTime {
//                 field_name: "ts".to_string(),
//                 unit: query::time::TimeUnit::Minutes,
//             },
//         })
//         .apply([Aggregation::min("value")])
//         .union(query_sub)
//         .window(WindowDescriptor::TumblingWindow {
//             duration: query::time::Duration::from_minutes(5),
//             time_character: TimeCharacteristic::EventTime {
//                 field_name: "start".to_string(),
//                 unit: query::time::TimeUnit::Milliseconds,
//             },
//         })
//         .apply([Aggregation::min("value")])
//         .sink(sink1);

//     (query0, query1)
// }

// fn query_average() -> (Query, Query) {
//     unimplemented!();
//     let sink0 = Sink::csv_file("./generated_files/result-0.csv", false);
//     let sink1 = Sink::csv_file("./generated_files/result-1.csv", false);

//     let query0 = QueryBuilder::from_source("test")
//         .window(WindowDescriptor::TumblingWindow {
//             duration: query::time::Duration::from_minutes(5),
//             time_character: TimeCharacteristic::EventTime {
//                 field_name: "ts".to_string(),
//                 unit: query::time::TimeUnit::Minutes,
//             },
//         })
//         .apply([Aggregation::average("value")])
//         .sink(sink0);

//     let query1 = QueryBuilder::from_source("test")
//         .filter(
//             EB::field("value")
//                 .greater_than(EB::literal(0))
//                 .build_logical()
//                 .unwrap(),
//         )
//         .window(WindowDescriptor::TumblingWindow {
//             duration: query::time::Duration::from_minutes(5),
//             time_character: TimeCharacteristic::EventTime {
//                 field_name: "ts".to_string(),
//                 unit: query::time::TimeUnit::Minutes,
//             },
//         })
//         .apply([
//             Aggregation::count().as_field("count"),
//             Aggregation::sum("value").as_field("sum"),
//         ])
//         .union(
//             QueryBuilder::from_source("test")
//                 .filter(
//                     EB::field("value")
//                         .greater_than(EB::literal(0))
//                         .not()
//                         .build_logical()
//                         .unwrap(),
//                 )
//                 .window(WindowDescriptor::TumblingWindow {
//                     duration: query::time::Duration::from_minutes(5),
//                     time_character: TimeCharacteristic::EventTime {
//                         field_name: "ts".to_string(),
//                         unit: query::time::TimeUnit::Minutes,
//                     },
//                 })
//                 .apply([
//                     Aggregation::count().as_field("count"),
//                     Aggregation::sum("value").as_field("sum"),
//                 ]),
//         )
//         // .map(
//         //     "avg",
//         //     EB::field("sum")
//         //         .divide(EB::field("count"))
//         //         .build_arith()
//         //         .unwrap(),
//         // )
//         .sink(sink1);

//     (query0, query1)
// }
