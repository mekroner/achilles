#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stages {
    #[default]
    StreamGen,
    QueryGen,
    QueryExec,
    Evaluation,
}
