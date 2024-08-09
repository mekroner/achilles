use crate::stream_gen::LogicalSource;

#[derive(Debug, Clone)]
pub struct StreamSchema {
    pub logical_sources: Vec<LogicalSource>,
}
