use crate::common::{self, FIXED_JOB_ID};
use serde_json::json;

#[test]
fn test_01_basic_match_from_examples() {

    let charter = common::example_charter("01-Basic-Match.yaml");
    let data_files = common::example_data_files(vec!(
        "20211129_043300000_01-invoices.csv",
        "20211129_043300000_01-payments.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_01_basic_match_from_examples/", &data_files);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "name": "Basic Match",
                "version": 1,
                "file": charter.canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
            "files": [
                "20211129_043300000_01-invoices.csv",
                "20211129_043300000_01-payments.csv" ]
        },
        {
            "groups": [
                [[0,3],[1,3],[1,5]],
                [[0,4],[1,4]] ]
        },
        {
          "changesets": [],
          "unmatched": []
        }
    ]));
}

#[test]
fn test_02_projected_columns_from_examples() {

    let charter = common::example_charter("02-Projected-Columns.yaml");
    let data_files = common::example_data_files(vec!(
        "20211129_043300000_02-invoices.csv",
        "20211129_043300000_02-payments.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_02_projected_columns_from_examples/", &data_files);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "name": "Projected Columns",
                "version": 1,
                "file": charter.canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
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
        },
        {
          "changesets": [],
          "unmatched": []
        }
    ]));
}

#[test]
fn test_03_net_with_tolerance_match_from_examples() {

    let charter = common::example_charter("03-Net-With-Tolerance.yaml");
    let data_files = common::example_data_files(vec!(
        "20211129_043300000_03-invoices.csv",
        "20211129_043300000_03-payments.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_03_net_with_tolerance_match_from_examples/", &data_files);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "name": "NET with Tolerance",
                "version": 1,
                "file": charter.canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
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
        },
        {
          "changesets": [],
          "unmatched": []
        }
    ]));
}

#[test]
fn test_04_3_way_match_from_examples() {

    let charter = common::example_charter("04-3-Way-Match.yaml");
    let data_files = common::example_data_files(vec!(
        "20211129_043300000_04-invoices.csv",
        "20211129_043300000_04-payments.csv",
        "20211129_043300000_04-receipts.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_04_3_way_match_from_examples", &data_files);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "name": "Three-way invoice match",
                "version": 1,
                "file": charter.canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
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
        },
        {
          "changesets": [],
          "unmatched": []
        }
    ]));
}

#[test]
fn test_05_2_stage_match_from_examples() {

    let charter = common::example_charter("05-2-Stage-Match.yaml");
    let data_files = common::example_data_files(vec!("20211129_043300000_05-2-stage.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_05_2_stage_match_from_examples/", &data_files);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "name": "Two-stage match",
                "version": 1,
                "file": charter.canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
            "files": [ "20211129_043300000_05-2-stage.csv" ]
        },
        {
            "groups": [
                [[0,3],[0,4]],
                [[0,5],[0,6]],
                [[0,7],[0,8]]
            ]
        },
        {
          "changesets": [],
          "unmatched": []
        }
    ]));
}

#[test]
fn test_07_unmatched_data_from_examples() {

    let charter = common::example_charter("07-Unmatched-Data.yaml");
    let mut data_files = common::example_data_files(vec!(
        "20211129_043300000_07-invoices.csv",
        "20211129_043300000_07-payments-a.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_07_unmatched_data_from_examples/", &data_files);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the output files.
    let (matched, unmatched) = common::assert_unmatched_ok(&data_files, &base_dir, 2);

    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "name": "Unmatched Data",
                "version": 1,
                "file": charter.canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
            "files": [
                "20211129_043300000_07-invoices.csv",
                "20211129_043300000_07-payments-a.csv"
            ]
        },
        {
            "groups": [ [[0,4],[1,4]] ]
        },
        {
          "changesets": [],
          "unmatched": [
            {
                "file": "20211129_043300000_07-invoices.unmatched.csv",
                "rows": 1
            },
            {
                "file": "20211129_043300000_07-payments-a.unmatched.csv",
                "rows": 1
            }
          ]
        }
    ]));

    // Check the unmatched folder contains the invoice file and the contents are exactly as follows: -
    assert_eq!(unmatched[0].file_name().unwrap().to_string_lossy(), "20211129_043300000_07-invoices.unmatched.csv");
    assert_eq!(unmatched[1].file_name().unwrap().to_string_lossy(), "20211129_043300000_07-payments-a.unmatched.csv");

    // Compare the unmatched invoices.
    let expected = r#""OpenRecStatus","Invoice No","InvoiceRef","Invoice Date","InvoiceAmount"
"IN","ST","ST","DT","DE"
"0","0001","INV0001","2021-11-25T04:36:08.000Z","1050.99"
"#;
    common::assert_file_contents(&unmatched[0], expected);

    // Compare the unmatched payments.
    let expected = r#""OpenRecStatus","PaymentId","PaymentRef","PaymentAmount","Payment Date"
"IN","ST","ST","DE","DT"
"0","P1","INV0001","50.99","2021-11-27T04:36:08.000Z"
"#;
    common::assert_file_contents(&unmatched[1], expected);

    // Now copy payments-b in and run the match charter again.
    data_files.push(common::copy_example_data_file("20211129_043300000_07-payments-b.csv", &base_dir));

    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&data_files, &base_dir);

    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "name": "Unmatched Data",
                "version": 1,
                "file": charter.canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
            "files": [
                "20211129_043300000_07-invoices.unmatched.csv",
                "20211129_043300000_07-payments-a.unmatched.csv",
                "20211129_043300000_07-payments-b.csv"
            ]
        },
        {
            "groups": [
                [[0,3],[1,3],[2,3]]
            ]
        },
        {
          "changesets": [],
          "unmatched": []
        }
    ]));
}