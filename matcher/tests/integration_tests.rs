mod common;
use serde_json::{Value, json};
use assert_json_diff::assert_json_include;

// TODO: Write a test for each of the examples.

#[test]
fn test_01_basic_match_from_examples() {

    let charter = common::example_charter("01-Basic-Match.yaml");
    let data_files = common::example_data_files(vec!(
        "20211129_043300000_01-invoices.csv",
        "20211129_043300000_01-payments.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test("tests/test01/", &data_files);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), base_dir.to_string_lossy().to_string()).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

    // Check the matched file contains the correct groupings.
    let a = common::read_json_file(matched);
    let e: Value = json!(
    [
        {
            "charter_name": "Basic Match",
            "charter_version": 1,
            "files": [
                "20211129_043300000_01-invoices.csv",
                "20211129_043300000_01-payments.csv" ]
        },
        {
            "groups": [
                [[0,3],[1,3],[1,5]],
                [[0,4],[1,4]] ]
        }
    ]);

    assert!(!a[0]["job_id"].as_str().expect("No jobId").is_empty()); // Uuid. Note the '!' in this assert!
    assert_json_include!(actual: a, expected: e);
}

#[test]
fn test_02_projected_columns_from_examples() {

    let charter = common::example_charter("02-Projected-Columns.yaml");
    let data_files = common::example_data_files(vec!(
        "20211129_043300000_02-invoices.csv",
        "20211129_043300000_02-payments.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test("tests/test02/", &data_files);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), base_dir.to_string_lossy().to_string()).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

    // Check the matched file contains the correct groupings.
    let a = common::read_json_file(matched);
    let e: Value = json!(
    [
        {
            "charter_name": "Projected Columns",
            "charter_version": 1,
            "files": [
                "20211129_043300000_02-invoices.csv",
                "20211129_043300000_02-payments.csv"
            ]
        },
        {
            "groups": [
                [[0,3], [1,3]],
                [[0,4], [1,4], [1,5]],
                [[0,5], [1,6]]
            ]
        }
    ]);

    assert!(!a[0]["job_id"].as_str().expect("No jobId").is_empty()); // Uuid. Note the '!' in this assert!
    assert_json_include!(actual: a, expected: e);
}

#[test]
fn test_03_net_with_tolerance_match_from_examples() {

    let charter = common::example_charter("03-Net-With-Tolerance.yaml");
    let data_files = common::example_data_files(vec!(
        "20211129_043300000_03-invoices.csv",
        "20211129_043300000_03-payments.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test("tests/test03/", &data_files);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), base_dir.to_string_lossy().to_string()).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

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

#[test]
fn test_04_3_way_match_from_examples() {

    let charter = common::example_charter("04-3-Way-Match.yaml");
    let data_files = common::example_data_files(vec!(
        "20211129_043300000_04-invoices.csv",
        "20211129_043300000_04-payments.csv",
        "20211129_043300000_04-receipts.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test("tests/test04/", &data_files);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), base_dir.to_string_lossy().to_string()).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

    // Check the matched file contains the correct groupings.
    let a = common::read_json_file(matched);
    let e: Value = json!(
    [
        {
            "charter_name": "Three-way invoice match",
            "charter_version": 1,
            "files": [
                "20211129_043300000_04-invoices.csv",
                "20211129_043300000_04-payments.csv",
                "20211129_043300000_04-receipts.csv"
            ]
        },
        {
            "groups": [
                [[0,4],[1,4],[1,5],[2,4],[2,5]],
                [[0,3],[1,3],[2,3]],
                [[0,5],[1,6],[2,6]]
            ]
        }
    ]);

    assert!(!a[0]["job_id"].as_str().expect("No jobId").is_empty()); // Uuid. Note the '!' in this assert!
    assert_json_include!(actual: a, expected: e);
}

#[test]
fn test_05_2_stage_match_from_examples() {

    let charter = common::example_charter("05-2-Stage-Match.yaml");
    let data_files = common::example_data_files(vec!("20211129_043300000_05-2-stage.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test("tests/test05/", &data_files);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), base_dir.to_string_lossy().to_string()).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

    // Check the matched file contains the correct groupings.
    let a = common::read_json_file(matched);
    let e: Value = json!(
    [
        {
            "charter_name": "Two-stage match",
            "charter_version": 1,
            "files": [ "20211129_043300000_05-2-stage.csv" ]
        },
        {
            "groups": [
                [[0,3],[0,4]],
                [[0,5],[0,6]],
                [[0,7],[0,8]]
            ]
        }
    ]);

    assert!(!a[0]["job_id"].as_str().expect("No jobId").is_empty()); // Uuid. Note the '!' in this assert!
    assert_json_include!(actual: a, expected: e);
}