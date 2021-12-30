use serde_json::json;
use crate::common::{self, FIXED_JOB_ID};

// TODO: Test where net to zero considers a positive netted against a negative - i.e. ensure both values are abs before subtraction.

#[test]
fn test_no_data_files() {
    let charter = common::example_charter("02-Projected-Columns.yaml");

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test("tests/test_no_data/");

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&vec!(), &base_dir);

    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "name": "Projected Columns",
                "file": charter.canonicalize().unwrap().to_string_lossy(),
                "version": 1
            },
            "job_id": FIXED_JOB_ID,
            "files": []
        },
        {
            "groups": []
        },
        {
            "changesets": [],
            "unmatched": []
        }
    ]));
}

#[test]
fn test_empty_data_file() {
    let charter = common::example_charter("02-Projected-Columns.yaml");

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test("tests/test_empty_data_file/");

    // Create an empty invoice and empty payment file.
    common::write_file(&base_dir.join("waiting/"), "20211129_043300000_02-invoices.csv",
r#""Reference","TotalAmount","Currency","Date"
"ST","DE","ST","DT"
"#);

    common::write_file(&base_dir.join("waiting/"), "20211129_043300000_02-payments.csv",
r#""Reference","Currency","Amount","Date","FXRate"
"ST","ST","DE","DT","DE"
"#);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    // Check the output files.
    let matched = common::assert_matched_ok(&vec!(), &base_dir);

    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "name": "Projected Columns",
                "file": charter.canonicalize().unwrap().to_string_lossy(),
                "version": 1
            },
            "job_id": FIXED_JOB_ID,
            "files": [
                "20211129_043300000_02-invoices.csv",
                "20211129_043300000_02-payments.csv"]
        },
        {
            "groups": []
        },
        {
            "changesets": [],
            "unmatched": []
        }
    ]));
}

#[test]
fn test_no_charter_instructions() {

    let base_dir = common::init_test("tests/test_no_charter_instructions/");

    // Write 2 transactions to match with each other.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""TransId","Date","Amount","Type"
"IN","DT","DE","ST"
"0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0002","2021-12-19T08:29:00.000Z","100.00","T2"
"#);

    // Write a charter file without any instructions.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: changeset test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
"#);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    // Check the output files.
    let (matched, unmatched) = common::assert_unmatched_ok(&vec!(), &base_dir, 1);
    assert_eq!(unmatched[0].file_name().unwrap().to_string_lossy(), "20211219_082900000_transactions.unmatched.csv");

    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "file": charter.canonicalize().unwrap().to_string_lossy(),
                "name": "changeset test",
                "version": 1
            },
            "job_id": FIXED_JOB_ID,
            "files": [ "20211219_082900000_transactions.csv" ]
        },
        {
            "groups": []
        },
        {
            "changesets": [],
            "unmatched": [
                {
                    "file": "20211219_082900000_transactions.unmatched.csv",
                    "rows": 2
                }
            ]
        }
    ]));
}

#[test]
fn test_data_exists_but_no_matches() {
    let base_dir = common::init_test("tests/test_data_exists_but_no_matches/");

    // Write 2 invoices.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_invoices.csv",
r#""TransId","Date","Amount","Type"
"IN","DT","DE","ST"
"0001","2021-12-19T08:29:00.000Z","100.00","INV1"
"0003","2021-12-18T08:29:00.000Z","100.00","INV1"
"#);

    // Write 2 payments - neither will match their invoice.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_payments.csv",
r#""TransId","Date","Amount","Type"
"IN","DT","DE","ST"
"0001","2021-12-19T08:29:00.000Z","55.00","PAY1"
"0003","2021-12-18T08:29:00.000Z","66.00","PAY1"
"#);

    // Write a charter file.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: no-match test
version: 1
matching:
  source_files:
    - pattern: .*invoices.csv
      field_prefix: INV
    - pattern: .*payments.csv
      field_prefix: PAY
  instructions:
    - merge:
        columns: ['INV.Amount', 'PAY.Amount']
        into: AMOUNT
    - merge:
        columns: ['INV.Date', 'PAY.Date']
        into: DATE
    - group:
        by: ['DATE']
        match_when:
        - nets_to_zero:
            column: AMOUNT
            lhs: record["META.prefix"] == "INV"
            rhs: record["META.prefix"] == "PAY"
"#);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    // Check the output files.
    let (matched, unmatched) = common::assert_unmatched_ok(&vec!(), &base_dir, 2);
    assert_eq!(unmatched[0].file_name().unwrap().to_string_lossy(), "20211219_082900000_invoices.unmatched.csv");
    assert_eq!(unmatched[1].file_name().unwrap().to_string_lossy(), "20211219_082900000_payments.unmatched.csv");


    // Check the matched file contains the correct groupings.
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "file": charter.canonicalize().unwrap().to_string_lossy(),
                "name": "no-match test",
                "version": 1
            },
            "job_id": FIXED_JOB_ID,
            "files": [
                "20211219_082900000_invoices.csv",
                "20211219_082900000_payments.csv" ]
        },
        {
            "groups": []
        },
        {
            "changesets": [],
            "unmatched": [
                {
                    "file": "20211219_082900000_invoices.unmatched.csv",
                    "rows": 2
                },
                {
                    "file": "20211219_082900000_payments.unmatched.csv",
                    "rows": 2
                }

            ]
        }
    ]));
}