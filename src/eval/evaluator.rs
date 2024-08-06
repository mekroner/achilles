use std::{cmp::Ordering, error::Error, path::Path};

use csv::StringRecord;
use yaml_rust2::Yaml;

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

pub fn compare_files(path0: &Path, path1: &Path) -> Result<ResultRelation, Box<dyn Error>> {
    if !is_row_count_equal(path0, path1)? {
        return Ok(ResultRelation::Diff);
    }
    let res = are_files_equal(path0, path1)?;
    if res == ResultRelation::Equal {
        return Ok(res);
    }
    return are_files_reordered(path0, path1);
}

pub fn is_row_count_equal(path0: &Path, path1: &Path) -> Result<bool, Box<dyn Error>> {
    let mut rdr0 = csv::Reader::from_path(path0)?;
    let mut rdr1 = csv::Reader::from_path(path1)?;
    Ok(rdr0.records().count() == rdr1.records().count())
}

pub fn are_files_equal(path0: &Path, path1: &Path) -> Result<ResultRelation, Box<dyn Error>> {
    let mut rdr0 = csv::Reader::from_path(path0)?;
    let mut rdr1 = csv::Reader::from_path(path1)?;
    let iter = rdr0.records().zip(rdr1.records());
    for (res0, res1) in iter {
        let record0 = res0?;
        let record1 = res1?;
        log::trace!("{:?} == {:?}", record0, record1);
        if !are_records_equal(&record0, &record1) {
            return Ok(ResultRelation::Diff);
        }
    }
    Ok(ResultRelation::Equal)
}

fn are_records_equal(rec0: &StringRecord, rec1: &StringRecord) -> bool {
    let rec_iter = rec0.iter().zip(rec1.iter());
    for (field0, field1) in rec_iter {
        if field0 != field1 {
            return false;
        }
    }
    true
}

pub fn comp_records(rec0: &StringRecord, rec1: &StringRecord) -> Ordering {
    rec0.get(0).cmp(&rec1.get(0))
}

pub fn are_files_reordered(path0: &Path, path1: &Path) -> Result<ResultRelation, Box<dyn Error>> {
    let mut rdr0 = csv::Reader::from_path(path0)?;
    let mut rdr1 = csv::Reader::from_path(path1)?;
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
        if !are_records_equal(&rec0, &rec1) {
            return Ok(ResultRelation::Diff);
        }
    }
    Ok(ResultRelation::Reordered)
}
