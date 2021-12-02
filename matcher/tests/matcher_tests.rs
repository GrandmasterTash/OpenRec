mod common;
use std::path::Path;
use serde_json::{Value, json};
use fs_extra::dir::get_dir_content;
use assert_json_diff::assert_json_include;

// TODO: Write a test for each of the examples.

#[test]
fn test_01_basic_match_from_examples() {

    common::init_test();

    let base_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("tests/test01/");
    let charter = Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/01-Basic-Match.yaml");
    let src_invoices = Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/data/20211129_043300000_01-invoices.csv");
    let src_payments = Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/data/20211129_043300000_01-payments.csv");

    // Copy the test data files into a temporary working folder.
    common::init_base_dir(vec!(&src_invoices, &src_payments), &base_dir);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), base_dir.to_string_lossy().to_string()).unwrap();

    // Test the match groups and un-matched folder is empty.
    let matched = Path::new(&base_dir).join("matched").join("20211201_053700000_matched.json");
    let invoices = Path::new(&base_dir).join("archive").join("20211129_043300000_01-invoices.csv");
    let payments = Path::new(&base_dir).join("archive").join("20211129_043300000_01-payments.csv");


    assert!(matched.exists(), "matched file doesn't exist");
    assert!(invoices.exists(), "archived invoice file doesn't exist");
    assert!(payments.exists(), "archived payment file doesn't exist");
    assert_eq!(get_dir_content(base_dir.join("unmatched")).expect("Unable to count unmatched files").files.len(), 0);

    // Compare an md5 hash of the source data and the archive data to ensure they are exact.
    assert_eq!(common::md5(src_invoices), common::md5(invoices), "Invoices have changed");
    assert_eq!(common::md5(src_payments), common::md5(payments), "Payments have changed");

    // Check the matched file contains the correct groupings.
    let a = common::read_json_file(matched);
    let e: Value = json!(
    [
        {
            "charter_name": "Basic Match",
            "charter_version": 1,
            "files": [
                "20211129_043300000_01-invoices.csv",
                "20211129_043300000_01-payments.csv"
            ]
        },
        {
            "groups": [
                [[0,3],[1,3],[1,5]],
                [[0,4],[1,4]]
            ]
        }
    ]);

    assert!(!a[0]["job_id"].as_str().expect("No jobId").is_empty()); // Uuid. Note the '!' in this assert!
    assert_json_include!(actual: a, expected: e);
}


#[test]
fn test_03_net_with_tolerance_match_from_examples() {

    common::init_test();

    let base_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("tests/test03/");
    let charter = Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/03-Net-With-Tolerance.yaml");
    let src_invoices = Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/data/20211129_043300000_03-invoices.csv");
    let src_payments = Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/data/20211129_043300000_03-payments.csv");

    // Copy the test data files into a temporary working folder.
    common::init_base_dir(vec!(&src_invoices, &src_payments), &base_dir);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), base_dir.to_string_lossy().to_string()).unwrap();

    // Test the match groups and un-matched folder is empty.
    let matched = Path::new(&base_dir).join("matched").join("20211201_053700000_matched.json");
    let invoices = Path::new(&base_dir).join("archive").join("20211129_043300000_03-invoices.csv");
    let payments = Path::new(&base_dir).join("archive").join("20211129_043300000_03-payments.csv");

    assert!(matched.exists(), "matched file doesn't exist");
    assert!(invoices.exists(), "archived invoice file doesn't exist");
    assert!(payments.exists(), "archived payment file doesn't exist");
    assert_eq!(get_dir_content(base_dir.join("unmatched")).expect("Unable to count unmatched files").files.len(), 0);

    // Compare an md5 hash of the source data and the archive data to ensure they are exact.
    assert_eq!(common::md5(src_invoices), common::md5(invoices), "Invoices have changed");
    assert_eq!(common::md5(src_payments), common::md5(payments), "Payments have changed");

    // Check the matched file contains the correct groupings.
    let a = common::read_json_file(matched);
    let e: Value = json!(
    [
        {
            "charter_name": "NET with Tolerance",
            "charter_version": 1,
            "files": [
                "20211129_043300000_03-invoices.csv",
                "20211129_043300000_03-payments.csv"
            ]
        },
        {
            "groups": [
                [[0,3],[1,3],[1,5]],
                [[0,4],[1,4]]
            ]
        }
    ]);

    assert!(!a[0]["job_id"].as_str().expect("No jobId").is_empty()); // Uuid. Note the '!' in this assert!
    assert_json_include!(actual: a, expected: e);
}