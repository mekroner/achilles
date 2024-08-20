pub mod generate_queries;
pub mod oracle;
pub mod query_id;
pub mod test_case;
pub mod util;

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
