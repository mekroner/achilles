use std::{error::Error, process, path::PathBuf};
use nes_eval::prelude::*;

fn example() -> Result<(), Box<dyn Error>> {
    let file_path0 = PathBuf::from("./examples/example-files/result-0.csv");
    let file_path1 = PathBuf::from("./examples/example-files/result-1.csv");
    match are_files_equal(&file_path0, &file_path1)?{
        ResultRelation::Equal => log::info!("Files are Equal"),
        ResultRelation::Reordered => log::info!("Files are Equal"),
        ResultRelation::Diff => log::info!("Files not Equal"),
    }
    Ok(())
}

fn main() {
    simple_logger::init_with_level(log::Level::Debug).expect("Cannot fail to init simple_logger!");
    if let Err(err) = example() {
        log::error!("{err}");
        process::exit(1);
    }
}
