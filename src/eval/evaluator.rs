use std::{cmp::Ordering, error::Error, num::ParseFloatError, path::Path};

use csv::StringRecord;
use yaml_rust2::Yaml;

use super::eval_error::EvalError;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ResultRelation {
    Equal,
    Reordered,
    Diff,
}

impl Into<Yaml> for &ResultRelation {
    fn into(self) -> Yaml {
        let str = match self {
            ResultRelation::Equal => "Equal",
            ResultRelation::Reordered => "Reordered",
            ResultRelation::Diff => "Diff",
        };
        Yaml::from_str(str)
    }
}

impl TryFrom<&Yaml> for ResultRelation {
    type Error = String;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        let Some(str) = value.as_str() else {
            return Err("Failed to parse ResultRelation.".to_string());
        };
        match str {
            "Equal" => Ok(ResultRelation::Equal),
            "Reordered" => Ok(ResultRelation::Reordered),
            "Diff" => Ok(ResultRelation::Diff),
            err => Err(format!(
                "Failed to parse ResultRelation: Unknown enum member {err}."
            )),
        }
    }
}

pub fn compare_files(path0: &Path, path1: &Path) -> Result<ResultRelation, EvalError> {
    if !is_row_count_equal(path0, path1)? {
        return Ok(ResultRelation::Diff);
    }
    let res = are_files_equal(path0, path1)?;
    if res == ResultRelation::Equal {
        return Ok(res);
    }
    return are_files_reordered(path0, path1);
}

pub fn is_row_count_equal(path0: &Path, path1: &Path) -> Result<bool, EvalError> {
    let mut rdr0 = csv::Reader::from_path(path0)?;
    let mut rdr1 = csv::Reader::from_path(path1)?;
    Ok(rdr0.records().count() == rdr1.records().count())
}

pub fn are_files_equal(path0: &Path, path1: &Path) -> Result<ResultRelation, EvalError> {
    let mut rdr0 = csv::Reader::from_path(path0)?;
    let mut rdr1 = csv::Reader::from_path(path1)?;
    let iter = rdr0.records().zip(rdr1.records());
    for (res0, res1) in iter {
        let record0 = res0?;
        let record1 = res1?;
        log::trace!("{:?} == {:?}", record0, record1);
        if !are_records_equal_string(&record0, &record1) {
            return Ok(ResultRelation::Diff);
        }
    }
    Ok(ResultRelation::Equal)
}

/// compares two String records for ordering
/// The algorithm compares the first elements of both Records.
/// If they are unequal we return the result.
/// If they are equal we look at the second element and compare them.
/// We repeat this process until one of the records runs out of elements.
/// If this is the case it is the lesser records
pub fn comp_records(rec0: &StringRecord, rec1: &StringRecord) -> Ordering {
    let len0 = rec0.len();
    let len1 = rec1.len();
    let min_len = len0.min(len1);
    for i in 0..min_len {
        match rec0.get(i).cmp(&rec1.get(i)) {
            Ordering::Equal => continue,
            non_equal => return non_equal,
        }
    }
    len0.cmp(&len1)
}

pub fn are_files_reordered(path0: &Path, path1: &Path) -> Result<ResultRelation, EvalError> {
    let mut rdr0 = csv::Reader::from_path(path0)?;
    let mut rdr1 = csv::Reader::from_path(path1)?;

    let header0 = rdr0.headers()?;
    let header1 = rdr1.headers()?;
    let type_strings0 = extract_types(&header0)?;
    let type_strings1 = extract_types(&header1)?;
    let types0 = convert_types(&type_strings1)?;
    let types1 = convert_types(&type_strings0)?;
    // FIXME: This might lead to errors so
    if types0 != types1 {
        return Err(EvalError::HeaderConflictError(format!(
            "Header {:?}, conflicts with header {:?}",
            header0, header1
        )));
    }

    let mut data0: Vec<StringRecord> = rdr0
        .records()
        .collect::<Result<Vec<StringRecord>, csv::Error>>()?;
    let mut data1: Vec<StringRecord> = rdr1
        .records()
        .collect::<Result<Vec<StringRecord>, csv::Error>>()?;

    data0.sort_by(comp_records);
    data1.sort_by(comp_records);

    let iter = data0.iter().zip(data1.iter());
    for (rec0, rec1) in iter {
        log::trace!("{:?} == {:?}", rec0, rec1);
        if !are_records_equal(&rec0, &rec1, &types0)? {
            return Ok(ResultRelation::Diff);
        }
    }
    Ok(ResultRelation::Reordered)
}

fn extract_types(record: &StringRecord) -> Result<Vec<String>, EvalError> {
    let mut types = Vec::new();

    for field in record.iter() {
        if let Some(type_part) = field.split('$').nth(1) {
            if let Some(type_str) = type_part.split(':').nth(1) {
                types.push(type_str.to_string());
            } else {
                return Err(EvalError::TypeExtractionError(format!(
                    "No type found in field: {}",
                    field
                )));
            }
        } else {
            return Err(EvalError::TypeExtractionError(format!(
                "Invalid field format: {}",
                field
            )));
        }
    }
    Ok(types)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvalType {
    Int,
    Float,
}

fn convert_types(string_types: &[String]) -> Result<Vec<EvalType>, EvalError> {
    string_types
        .iter()
        .map(|str_type| match str_type.to_lowercase().as_str() {
            "integer(64 bits)" | "integer(32 bits)" | "integer(16 bits)" | "integer(8 bits)" => {
                Ok(EvalType::Int)
            }
            "float(32 bits)" | "float(64 bits)" => Ok(EvalType::Float),
            err => Err(EvalError::TypeConversionError(format!(
                "Unknown type string {err}"
            ))),
        })
        .collect()
}

fn are_types_equal(types0: &[EvalType], types1: &[EvalType]) -> bool {
    if types0.len() != types1.len() {
        return false;
    }

    let iter = types0.iter().zip(types1.iter());
    for (&a, &b) in iter {
        if a != b {
            return false;
        }
    }
    true
}

fn are_records_equal(
    rec0: &StringRecord,
    rec1: &StringRecord,
    types: &[EvalType],
) -> Result<bool, EvalError> {
    let rec_iter = rec0.iter().zip(rec1.iter()).zip(types.iter());
    for ((field0, field1), data_type) in rec_iter {
        let are_files_equal = are_fields_equal(field0, field1, *data_type)?;
        if !are_files_equal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn are_fields_equal(field0: &str, field1: &str, data_type: EvalType) -> Result<bool, EvalError> {
    let epsilon = 1e-3;
    match data_type {
        EvalType::Int => Ok(int_equal(field0, field1)),
        EvalType::Float => Ok(float_equal(field0, field1, epsilon)?),
    }
}

fn int_equal(field0: &str, field1: &str) -> bool {
    field1 == field0
}

fn float_equal(field0: &str, field1: &str, epsilon: f64) -> Result<bool, EvalError> {
    let a = field0.parse::<f64>()?;
    let b = field1.parse::<f64>()?;
    Ok((a - b).abs() < epsilon)
}

fn are_records_equal_string(rec0: &StringRecord, rec1: &StringRecord) -> bool {
    let rec_iter = rec0.iter().zip(rec1.iter());
    for (field0, field1) in rec_iter {
        if field0 != field1 {
            return false;
        }
    }
    true
}
