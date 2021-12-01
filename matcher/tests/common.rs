use std::{path::PathBuf, fs::File, io::{Read, BufReader}};

use fs_extra::{dir::{CopyOptions, remove}, copy_items};
use md5::Digest;
use serde_json::Value;

///
/// Create a worker folder structure under the base_dir specified.
///
/// This function will delete any existing files in base_dir before creating a new waiting folder.
///
/// Finally, any test data files in the source list are copied into the waiting folder.
///
pub fn init_base_dir(data_files: Vec<&PathBuf>, base_dir: &PathBuf) {

    // Delete everything in base_dir.
    remove(base_dir)
        .expect(&format!("Cannot remove base_dir {}", base_dir.to_string_lossy()));

    // Create a waiting folder to put the test data files.
    // let waiting = format!("{}/waiting", base_dir);
    let waiting = base_dir.join("waiting/");
    std::fs::create_dir_all(&waiting)
        .expect("Cannot create a waiting folder");

    // Copy the test data files into a temporary folder.
    copy_items(&data_files, &waiting, &CopyOptions::new())
        .expect(&format!("Cannot copy test data {:?} into {}", data_files, base_dir.to_string_lossy()));

}

///
/// Read the entire file to a JSON Value.
///
pub fn read_json_file(path: PathBuf) -> Value {
    let file = File::open(path.clone()).expect(&format!("Could not open {}", path.to_string_lossy()));
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).expect(&format!("Could not read {}", path.to_string_lossy()))
}

///
/// Calculate the MD5 of the file specified.
///
pub fn md5(path: PathBuf) -> Digest {
    let mut f = File::open(path.clone()).expect(&format!("Cannot open {}", path.to_string_lossy()));
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer).expect(&format!("Cannot read {}", path.to_string_lossy()));
    md5::compute(buffer)
}