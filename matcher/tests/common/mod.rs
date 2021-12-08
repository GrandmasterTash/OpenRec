use md5::Digest;
use serde_json::Value;
use assert_json_diff::assert_json_include;
use std::{path::{PathBuf, Path}, fs::File, io::{Read, BufReader}};
use fs_extra::{dir::{CopyOptions, get_dir_content, remove}, copy_items};

const FIXED_TS: &str = "20211201_053700000";

///
/// Set-up logging and ensure a fixed timestamp is used when writing output matched files.
///
/// Set the working folder for the test to put data and write output files to.
///
// pub fn init_test(folder: &str) -> PathBuf {
//     dotenv::dotenv().ok();
//     let _ = env_logger::builder().is_test(true).try_init();
//     use_fixed_timestamp();
//     Path::new(env!("CARGO_TARGET_TMPDIR")).join(folder)
// }

///
/// Set-up logging and ensure a fixed timestamp is used when writing output matched files.
///
/// Create a worker folder structure under the base_dir specified.
///
/// This function will delete any existing files in base_dir before creating a new waiting folder.
///
/// Finally, any test data files in the source list are copied into the waiting folder.
///
pub fn init_test(folder: &str, data_files: &Vec<PathBuf>) -> PathBuf {
    dotenv::dotenv().ok();
    let _ = env_logger::builder().is_test(true).try_init();

    use_fixed_timestamp();
    let base_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(folder);

    // Delete everything in base_dir.
    remove(&base_dir)
        .expect(&format!("Cannot remove base_dir {}", base_dir.to_string_lossy()));

    // Create a waiting folder to put the test data files.
    // let waiting = format!("{}/waiting", base_dir);
    let waiting = base_dir.join("waiting/");
    std::fs::create_dir_all(&waiting)
        .expect("Cannot create a waiting folder");

    // Copy the test data files into a temporary folder.
    copy_items(&data_files, &waiting, &CopyOptions::new())
        .expect(&format!("Cannot copy test data {:?} into {}", data_files, base_dir.to_string_lossy()));

    base_dir
}

///
/// Copy the specified test data file into the unmatched folder.
///
pub fn copy_example_data_file(filename: &str, base_dir: &PathBuf) -> PathBuf {
    let data_file = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../examples/data/{}", filename));

    let waiting = base_dir.join("waiting/");

    // Copy the test data files into a temporary folder.
    copy_items(&vec!(data_file.clone()), &waiting, &CopyOptions::new())
        .expect(&format!("Cannot copy test data file {:?} into {}", filename, base_dir.to_string_lossy()));

    data_file
}

///
/// Get the full path to the example charter yaml file.
///
pub fn example_charter(filename: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../examples/{}", filename))
}

///
/// Locate the example data files and return their full paths.
///
pub fn example_data_files(filenames: Vec<&str>) -> Vec<PathBuf> {
    filenames
        .iter()
        .map(|f| Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../examples/data/{}", f)))
        .collect()
}

///
/// Check the matched file contents are as expected.
///
pub fn assert_matched_contents(matched: PathBuf, e: Value) {
    let a = read_json_file(matched);
    assert!(!a[0]["job_id"].as_str().expect("No jobId").is_empty()); // Uuid. Note the '!' in this assert!
    assert_json_include!(actual: a, expected: e);
}

///
/// Read the file to a string and compare it to the expected value.
///
pub fn assert_file_contents(path: &PathBuf, expected: &str) {
    assert_eq!(std::fs::read_to_string(path).unwrap(), expected);
}

///
/// Check there are not unmatched files.
/// Check there is a matched file.
/// Check the source data has been archived and not modified.
/// Returns the PathBuf to the matched.json file.
///
pub fn assert_matched_ok(data_files: &Vec<PathBuf>, base_dir: &PathBuf) -> PathBuf {
    // Check there's a matched JSON file.
    let matched = Path::new(base_dir).join("matched").join(format!("{}_matched.json", FIXED_TS));
    assert!(matched.exists(), "matched file {} doesn't exist", matched.to_string_lossy());

    // Check the data files have been archived, and for each one, ensure it's not been modified.
    for source in data_files {
        let archive = Path::new(base_dir).join("archive").join(source.file_name().unwrap());
        assert!(archive.exists(), "archived file {} doesn't exist", archive.to_string_lossy());

        // Compare an md5 hash of the source data and the archive data to ensure they are exact.
        assert_eq!(md5(source), md5(&archive), "Data file {} has changed", archive.to_string_lossy());
    }

    assert_eq!(get_dir_content(base_dir.join("unmatched")).expect("Unable to count unmatched files").files.len(), 0, "Unmatched files exist, expected none");

    matched
}

///
/// Check there are n unmatched files.
/// Check there is a matched file.
/// Check the source data has been archived and not modified.
/// Returns the PathBuf to the matched.json file and all the PathBufs pointing to the unmatched files.
///
pub fn assert_unmatched_ok(data_files: &Vec<PathBuf>, base_dir: &PathBuf, expected_unmatched: usize)
    -> (PathBuf /* matched */, Vec<PathBuf> /* unmatched files */) {

    // Check there's a matched JSON file.
    let matched = Path::new(base_dir).join("matched").join(format!("{}_matched.json", FIXED_TS));
    assert!(matched.exists(), "matched file {} doesn't exist", matched.to_string_lossy());

    // Check the data files have been archived, and for each one, ensure it's not been modified.
    for source in data_files {
        let archive = Path::new(base_dir).join("archive").join(source.file_name().unwrap());
        assert!(archive.exists(), "archived file {} doesn't exist", archive.to_string_lossy());

        // Compare an md5 hash of the source data and the archive data to ensure they are exact.
        assert_eq!(md5(source), md5(&archive), "Data file {} has changed", archive.to_string_lossy());
    }

    let unmatched_dir = get_dir_content(base_dir.join("unmatched")).expect("Unable to get the unmatched files");
    assert_eq!(unmatched_dir.files.len(), expected_unmatched, "Unmatched files didn't match expect number");

    (matched, unmatched_dir.files.iter().map(|f| PathBuf::from(f)).collect())
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
pub fn md5(path: &PathBuf) -> Digest {
    let mut f = File::open(path.clone()).expect(&format!("Cannot open {}", path.to_string_lossy()));
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer).expect(&format!("Cannot read {}", path.to_string_lossy()));
    md5::compute(buffer)
}

///
/// Ensure matched files are created with a deterministic filename.
///
/// Important: Use the same value across all tests otherwise we can't run them in parrallel as they would
/// corrupt each other's expected ENV value.
///
fn use_fixed_timestamp() {
    std::env::set_var("OPENREC_FIXED_TS", FIXED_TS);
}