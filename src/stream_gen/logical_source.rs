use nes_rust_client::query::expression::Field;

pub struct LogicalSource {
    pub source_name: String,
    pub fields: Vec<Field>,
}
