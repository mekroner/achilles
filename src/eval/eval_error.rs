use std::num::ParseFloatError;
use std::fmt;


#[derive(Debug)]
pub enum EvalError {
    HeaderConflictError(String),
    ParseFloatError(ParseFloatError),
    TypeExtractionError(String),
    TypeConversionError(String),
    CsvError(csv::Error),
}

impl From<ParseFloatError> for EvalError {
    fn from(err: ParseFloatError) -> EvalError {
        EvalError::ParseFloatError(err)
    }
}

impl From<csv::Error> for EvalError {
    fn from(err: csv::Error) -> EvalError {
        EvalError::CsvError(err)
    }
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EvalError::HeaderConflictError(err) => write!(f, "Failed to parse float: {}", err),
            EvalError::ParseFloatError(err) => write!(f, "Failed to parse float: {}", err),
            EvalError::TypeExtractionError(msg) => write!(f, "Type extraction error: {}", msg),
            EvalError::TypeConversionError(msg) => write!(f, "Type conversion error: {}", msg),
            EvalError::CsvError(err) => write!(f, "CSV error: {}", err),
        }
    }
}
