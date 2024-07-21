use std::path::Path;

use crate::QueryListEntry;
use crate::{query_list::QueryExecutionProps, LancerConfig, QueryList};
use nes_rust_client::prelude::*;
use nes_rust_client::query::expr_gen::expr_gen::generate_logical_expr;
use nes_types::NesType;

pub fn generate_queries(config: LancerConfig) -> QueryList {
    let entries = (0..1)
        .map(|run_id| {
            let origin_sink = origin_sink(run_id, &config.generated_files_path);
            let origin = QueryExecutionProps {
                query_id: None,
                start_time: None,
                query: query_origin_filter(origin_sink),
            };
            let others = (0..2)
                .map(|id| {
                    let other_sink = other_sink(run_id, id, &config.generated_files_path);
                    QueryExecutionProps {
                        query_id: None,
                        start_time: None,
                        query: query_part_filter(other_sink),
                    }
                })
                .collect();
            QueryListEntry { origin, others }
        })
        .collect();
    QueryList { entries }
}

fn origin_sink(run_id: u32, path: &Path) -> Sink {
    let file_path = path.join(format!("result{run_id}-origin.csv"));
    Sink::csv_file(file_path.into_os_string().into_string().unwrap(), false)
}

fn other_sink(run_id: u32, id: u32, path: &Path) -> Sink {
    let file_path = path.join(format!("result{run_id}-{id}.csv"));
    Sink::csv_file(file_path.into_os_string().into_string().unwrap(), false)
}

fn query_origin_filter(sink: Sink) -> Query {
    QueryBuilder::from_source("test").sink(sink)
}

fn query_part_filter(sink: Sink) -> Query {
    use nes_rust_client::query::expression::Field;

    let fields = [
        Field::typed("value", NesType::Int64),
        Field::typed("id", NesType::Int64),
    ];

    let predicate = loop {
        if let Ok(p) = generate_logical_expr(1, &fields) {
            break p;
        }
    };
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
