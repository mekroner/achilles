use nes_rust_client::{
    prelude::*,
    query::{expr_gen::expr_gen::generate_raw_expr, expression::Field, stringify::stringify_expr},
};
use nes_types::NesType;

fn main() {
    simple_logger::init_with_level(log::Level::Trace).expect("Simple_logger should not fail!");
    log::info!("This example shows the expression generator and pretty printing of expressions.");

    let fields = [
        Field::typed("Int64Value1", NesType::Int64),
        Field::typed("Int64Value2", NesType::Int64),
        // Field::typed("Int32Value", NesType::Int32),
        // Field::typed("Float64Value", NesType::Float64),
        // Field::typed("Float32Value", NesType::Float32),
        // Field::typed("BooleanValue", NesType::Bool),
    ];

    for i in 0..32 {
        let expr = match generate_raw_expr(2, &fields, nes_types::NesType::Bool) {
            Ok(ok) => ok,
            Err(e) => {
                log::error!("{e}");
                continue;
            }
        };
        let expr_str = stringify_expr(&expr);
        log::info!("Expression {i}: {expr_str}");
    }
}
