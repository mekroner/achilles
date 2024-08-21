use nes_rust_client::expression::Field;

#[derive(Debug, Clone,)]
pub struct LogicalSource {
    pub source_name: String,
    pub fields: Vec<Field>,
}
