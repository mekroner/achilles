use super::{data_generator::RecordGenerator, LogicalSource};

pub struct PhysicalSource {
    // pub logical_source: &'a LogicalSource,
    pub physical_source_name: String,
    pub generator: RecordGenerator,
}

pub struct PhysicalSourceBuilder<'a> {
    logical_source: &'a LogicalSource,
    physical_source_name: Option<String>,
    generator: RecordGenerator,
}

impl PhysicalSource {
    pub fn builder<'a>(logical_source: &'a LogicalSource) -> PhysicalSourceBuilder<'a> {
        PhysicalSourceBuilder::new(logical_source)
    }
}

impl<'a> PhysicalSourceBuilder<'a> {
    pub fn new(logical_source: &'a LogicalSource) -> Self {
        Self {
            logical_source,
            physical_source_name: None,
            generator: RecordGenerator {
                record_count: 1000,
                field_generators: Vec::new(),
            },
        }
    }
}
