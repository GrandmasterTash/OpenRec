use fs_extra::{dir::{CopyOptions, remove}, copy_items};

///
/// Create a worker folder structure under the base_dir specified.
///
/// This function will delete any existing files in base_dir before creating a new waiting folder.
///
/// Finally, any test data files in the source list are copied into the waiting folder.
///
pub fn init_base_dir(data_files: Vec<&str>, base_dir: &str) {

    // Delete everything in base_dir.
    remove(base_dir)
        .expect(&format!("Cannot remove base_dir {}", base_dir));

    // Create a waiting folder to put the test data files.
    let waiting = format!("{}/waiting", base_dir);
    std::fs::create_dir_all(&waiting)
        .expect("Cannot create a waiting folder");

    // Copy the test data files into a temporary folder.
    copy_items(&data_files, &waiting, &CopyOptions::new())
        .expect(&format!("Cannot copy test data {:?} into {}", data_files, base_dir));

}