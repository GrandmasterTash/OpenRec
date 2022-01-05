use serde_json::json;
use crate::common::{self, FIXED_JOB_ID};

// TODO: Test where net to zero considers a positive netted against a negative - i.e. ensure both values are abs before subtraction.

#[test]
fn test_no_data_files() {
    let charter = common::example_charter("02-Projected-Columns.yaml");

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test("tests/test_no_data/");

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

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
r#""OpenRecStatus","Reference","TotalAmount","Currency","Date"
"IN","ST","DE","ST","DT"
"#);

    common::write_file(&base_dir.join("waiting/"), "20211129_043300000_02-payments.csv",
r#""OpenRecStatus","Reference","Currency","Amount","Date","FXRate"
"IN","ST","ST","DE","DT","DE"
"#);

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

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
fn test_okay_when_not_all_files_present() {

    let base_dir = common::init_test("tests/test_not_all_files_present/");

    // Writer some payments.
    common::write_file(&base_dir.join("waiting/"), "20211129_043300000_02-payments.csv",
r#""OpenRecStatus","Reference","Currency","Amount","Date","FXRate"
"IN","ST","ST","DE","DT","DE"
"0","PAY001XXINV001XX","USD","1000.0000","2021-11-25T04:36:08.000Z","0.75"
"0","PAY002XXINV002XX","EUR","400.9900","2021-10-21T11:16:08.000Z","0.844"
"0","PAY003XXINV002XX","EUR","50.0000","2021-10-22T15:02:48.000Z","0.846"
"0","PAY004XXINV003XX","USD","1234.56","2022-03-20T22:22:48.000Z","0.715"
"#);

    // Create a charter that merges and projects both invoice and payment columns.
    let charter = common::example_charter("02-Projected-Columns.yaml");

    // Run the charter without any payment files.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    // Check we have unmatch invoices.
    let (matched, unmatched) = common::assert_unmatched_ok(&vec!(), &base_dir, 1);
    assert_eq!(unmatched[0].file_name().unwrap().to_string_lossy(), "20211129_043300000_02-payments.unmatched.csv");

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
            "files": [ "20211129_043300000_02-payments.csv" ]
        },
        {
            "groups": []
        },
        {
            "changesets": [],
            "unmatched": [
                {
                    "file": "20211129_043300000_02-payments.unmatched.csv",
                    "rows": 4
                }
            ]
        }
    ]));
}

#[test]
fn test_no_charter_instructions() {

    let base_dir = common::init_test("tests/test_no_charter_instructions/");

    // Write 2 transactions to match with each other.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0","0002","2021-12-19T08:29:00.000Z","100.00","T2"
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
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

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
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T08:29:00.000Z","100.00","INV1"
"0","0003","2021-12-18T08:29:00.000Z","100.00","INV1"
"#);

    // Write 2 payments - neither will match their invoice.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_payments.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T08:29:00.000Z","55.00","PAY1"
"0","0003","2021-12-18T08:29:00.000Z","66.00","PAY1"
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
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

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