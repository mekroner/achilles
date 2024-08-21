use std::fmt::Display;

use nes_types::{FloatType, IntType, NesType};
use rand::prelude::*;
use strum::IntoEnumIterator;

use nes_rust_client::expression::{
    binary_expression::{BinaryExpr, BinaryOp},
    expression::RawExpr,
    literal::Literal,
    Field, LogicalExpr,
};

pub struct GenerationError(String);

impl Display for GenerationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GenerationError: {}", self.0)
    }
}

/// Generates a random logical expression. Each branch has the specified `depth`. Fields are selected from the List
/// of `fields`. See `generate_raw_expr` for more details.
pub fn generate_logical_expr(depth: u32, fields: &[Field]) -> Result<LogicalExpr, GenerationError> {
    Ok(LogicalExpr(generate_raw_expr(
        depth,
        fields,
        NesType::Bool,
    )?))
}

/// Generates a random expression. Leaf nodes are `Fields` or `Literal`s and non leaf nodes are
/// `BinaryExpr` or `UnaryExpr`. Each branch has the specified depth. Fields are selected from the List
/// of `fields`. The `output_type` specifies the return type of the expression. Undefined results in a
/// random return type.
pub fn generate_raw_expr(
    depth: u32,
    fields: &[Field],
    output_type: NesType,
) -> Result<RawExpr, GenerationError> {
    let mut rng = rand::thread_rng();
    if depth == 0 {
        let is_field = rng.gen_bool(0.75);
        if is_field {
            let Some(field) = generate_field(fields, output_type) else {
                let literal = generate_literal(output_type)?;
                return Ok(RawExpr::Literal(literal));
            };
            return Ok(RawExpr::Field(field));
        }
        let literal = generate_literal(output_type)?;
        return Ok(RawExpr::Literal(literal));
    }
    let operator = BinaryOp::iter()
        .filter(|&operator| binary_op_can_return(operator, output_type))
        .choose(&mut rng)
        .expect("Failed to find binary operator");
    // should only select types with existing fields
    let input_type = binary_op_input_type(operator, fields, output_type)?;
    let binary = BinaryExpr {
        lhs: Box::new(generate_raw_expr(depth - 1, fields, input_type)?),
        rhs: Box::new(generate_raw_expr(depth - 1, fields, input_type)?),
        data_type: output_type,
        operator,
    };
    let expr = RawExpr::Binary(binary);
    Ok(expr)
}

fn binary_op_can_return(operator: BinaryOp, output_type: NesType) -> bool {
    output_type == NesType::Undefined
        || binary_op_accepted_output_types(operator).contains(&output_type)
}

fn generate_field(fields: &[Field], data_type: NesType) -> Option<Field> {
    let mut rng = rand::thread_rng();
    if data_type == NesType::Undefined {
        return fields.choose(&mut rng).cloned();
    }
    fields
        .iter()
        .filter(|field| field.data_type() == data_type)
        .choose(&mut rng)
        .cloned()
}

fn generate_literal(data_type: NesType) -> Result<Literal, GenerationError> {
    let mut rng = rand::thread_rng();
    match data_type {
        NesType::Undefined => Err(GenerationError(
            "Cannot generate literal of type undefined.".into(),
        )),
        NesType::Bool => Ok(Literal::typed(
            rng.gen::<bool>().to_string().to_string(),
            NesType::Bool,
        )),
        NesType::Char => Err(GenerationError(
            "Cannot generate literal of type char.".into(),
        )),
        NesType::Int(t) => Ok(Literal::typed(generate_int(&mut rng, t), NesType::Int(t))),
        NesType::Float(t) => Ok(Literal::typed(generate_float(&mut rng, t), NesType::Float(t))),
    }
}

fn generate_int(rng: &mut ThreadRng, data_type: IntType) -> String {
    match data_type {
        IntType::Signed8 => rng.gen::<i8>().to_string(),
        IntType::Unsigned8 => rng.gen::<u8>().to_string(),
        IntType::Signed16 => rng.gen::<i16>().to_string(),
        IntType::Unsigned16 => rng.gen::<u16>().to_string(),
        IntType::Signed32 => rng.gen::<i32>().to_string(),
        IntType::Unsigned32 => rng.gen::<u32>().to_string(),
        IntType::Signed64 => rng.gen::<i64>().to_string(),
        IntType::Unsigned64 => rng.gen::<u64>().to_string(),
    }
}

fn generate_float(rng: &mut ThreadRng, data_type: FloatType) -> String {
    match data_type {
        FloatType::Bit32 => rng.gen::<f32>().to_string(),
        FloatType::Bit64 => rng.gen::<f64>().to_string(),
    }
}

fn generate_binary_op(input_type: NesType, output_type: NesType) -> Option<BinaryOp> {
    let mut rng = rand::thread_rng();
    BinaryOp::iter()
        .filter(|&operator| {
            input_type == NesType::Undefined
                || binary_op_accepted_input_types(operator).contains(&input_type)
        })
        .filter(|&operator| {
            output_type == NesType::Undefined
                || binary_op_output_type(operator, input_type) == output_type
        })
        .choose(&mut rng)
}

fn binary_op_accepted_input_types(operator: BinaryOp) -> Vec<NesType> {
    match operator {
        BinaryOp::And | BinaryOp::Or => logical_types(),
        BinaryOp::Equals
        | BinaryOp::Greater
        | BinaryOp::GreaterEquals
        | BinaryOp::Less
        | BinaryOp::LessEquals
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Multiply
        | BinaryOp::Divide => arithmetic_types(),
    }
}

fn binary_op_accepted_output_types(operator: BinaryOp) -> Vec<NesType> {
    match operator {
        BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Equals
        | BinaryOp::Greater
        | BinaryOp::GreaterEquals
        | BinaryOp::Less
        | BinaryOp::LessEquals => logical_types(),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Multiply | BinaryOp::Divide => arithmetic_types(),
    }
}

fn binary_op_output_type(operator: BinaryOp, input_type: NesType) -> NesType {
    match operator {
        BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Equals
        | BinaryOp::Greater
        | BinaryOp::GreaterEquals
        | BinaryOp::Less
        | BinaryOp::LessEquals => NesType::Bool,
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Multiply | BinaryOp::Divide => input_type,
    }
}

fn binary_op_input_type(
    operator: BinaryOp,
    fields: &[Field],
    output_type: NesType,
) -> Result<NesType, GenerationError> {
    let mut rng = rand::thread_rng();
    match operator {
        BinaryOp::And | BinaryOp::Or => Ok(NesType::Bool),
        BinaryOp::Equals
        | BinaryOp::Greater
        | BinaryOp::GreaterEquals
        | BinaryOp::Less
        | BinaryOp::LessEquals => match binary_op_accepted_input_types(operator)
            .iter()
            .filter(|&input_type| {
                fields
                    .iter()
                    .map(|field| field.data_type())
                    .find(|t| t == input_type)
                    .is_some()
            })
            .choose(&mut rng)
        {
            Some(t) => Ok(*t),
            None => Err(GenerationError("Unable to find input type.".to_string())),
        },
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Multiply | BinaryOp::Divide => Ok(output_type),
    }
}

fn arithmetic_types() -> Vec<NesType> {
    vec![
        NesType::Int(IntType::Signed8),
        NesType::Int(IntType::Unsigned8),
        NesType::Int(IntType::Signed16),
        NesType::Int(IntType::Unsigned16),
        NesType::Int(IntType::Signed32),
        NesType::Int(IntType::Unsigned32),
        NesType::Int(IntType::Signed64),
        NesType::Int(IntType::Unsigned64),
        NesType::Float(FloatType::Bit32),
        NesType::Float(FloatType::Bit64),
    ]
}

fn logical_types() -> Vec<NesType> {
    vec![NesType::Bool]
}
