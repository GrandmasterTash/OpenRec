use serde_json::json;
use fs_extra::dir::get_dir_content;
use crate::common::{self, FIXED_JOB_ID, function};

#[test]
fn test_no_data_files() {
    let charter = common::example_charter("02-Projected-Columns.yaml");

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Run the match.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();
    celerity::run_charter(&charter, &base_dir).unwrap();

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

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create an empty invoice and empty payment file.
    common::write_file(&base_dir.join("inbox/"), "02-invoices.csv",
r#""Reference","TotalAmount","Currency","Date"
"#);

    common::write_file(&base_dir.join("inbox/"), "02-payments.csv",
r#""Reference","Currency","Amount","Date","FXRate""#);

    common::assert_files_in_folders(&base_dir, vec!(
        (2, "inbox"),
        (0, "waiting")));

    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (2, "waiting"),
        (2, "archive/jetwash")));

    celerity::run_charter(&charter, &base_dir).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (0, "waiting"),
        (2, "archive/jetwash"),
        (2, "archive/celerity"),
        (0, "unmatched"),
        (1, "matched")));

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
fn test_unmatched_okay_when_not_all_files_present() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Writer some payments.
    common::write_file(&base_dir.join("inbox/"), "02-payments.csv",
r#""Reference","Currency","Amount","Date","FXRate"
"PAY001XXINV001XX","USD","1000.0000","2021-11-25T04:36:08.000Z","0.75"
"PAY002XXINV002XX","EUR","400.9900","2021-10-21T11:16:08.000Z","0.844"
"PAY003XXINV002XX","EUR","50.0000","2021-10-22T15:02:48.000Z","0.846"
"PAY004XXINV003XX","USD","1234.56","2022-03-20T22:22:48.000Z","0.715"
"#);

    // Create a charter that merges and projects both invoice and payment columns.
    let charter = common::example_charter("02-Projected-Columns.yaml");

    // Run the charter without any payment files.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check we have unmatch invoices.
    let (matched, unmatched) = common::assert_unmatched_ok(&vec!(), &base_dir, 1);
    assert_eq!(unmatched[0].file_name().unwrap().to_string_lossy(), "20211201_053700000_02-payments.unmatched.csv");

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
            "files": [ "20211201_053700000_02-payments.csv" ]
        },
        {
            "groups": []
        },
        {
            "changesets": [],
            "unmatched": [
                {
                    "file": "20211201_053700000_02-payments.unmatched.csv",
                    "rows": 4
                }
            ]
        }
    ]));
}


#[test]
fn test_no_charter_instructions() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Write 2 transactions to match with each other.
    common::write_file(&base_dir.join("inbox/"), "transactions.csv",
r#""TransId","Date","Amount","Type"
"0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0002","2021-12-19T08:29:00.000Z","100.00","T2"
"#);

    // Write a charter file without any instructions.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: changeset test
version: 1
jetwash:
  source_files:
    - pattern: .*.csv
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
"#);

    // Run the match.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the output files.
    let (matched, unmatched) = common::assert_unmatched_ok(&vec!(), &base_dir, 1);
    assert_eq!(unmatched[0].file_name().unwrap().to_string_lossy(), "20211201_053700000_transactions.unmatched.csv");

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
            "files": [ "20211201_053700000_transactions.csv" ]
        },
        {
            "groups": []
        },
        {
            "changesets": [],
            "unmatched": [
                {
                    "file": "20211201_053700000_transactions.unmatched.csv",
                    "rows": 2
                }
            ]
        }
    ]));
}


#[test]
fn test_data_exists_but_no_matches() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

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
    celerity::run_charter(&charter, &base_dir).unwrap();

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

#[test]
fn test_ensure_archive_filenames_are_unqiue() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    let file_content = r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T00:00:00.000Z","100.00","T1"
"0","0002","2021-12-19T00:00:00.000Z","75.00","T2"
"0","0003","2021-12-19T00:00:00.000Z","25.00","T2"
"#;

    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv", file_content);

    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: archive filename test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - group:
        by: ['Date']
        match_when:
        - nets_to_zero:
            column: Amount
            lhs: record["Type"] == "T1"
            rhs: record["Type"] == "T2"
"#);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
    assert_eq!(get_dir_content(base_dir.join("archive")).unwrap().files.len(), 1);
    assert_eq!(common::get_filenames(&base_dir.join("archive")), vec!("20211219_082900000_transactions.csv"));

    // Create another file with the same name.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv", file_content);

    celerity::run_charter(&charter, &base_dir).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1); // 2 in real life, but the fixed TS means only one.
    assert_eq!(get_dir_content(base_dir.join("archive")).unwrap().files.len(), 2);
    assert_eq!(common::get_filenames(&base_dir.join("archive")), vec!(
        "20211219_082900000_transactions.csv",
        "20211219_082900000_transactions.csv_01"));

    // Ensure the renamed archive file is recorded in the job.
    common::assert_matched_contents(base_dir.join("matched/20211201_053700000_matched.json"), json!(
        [
            {
                "charter": {
                    "name": "archive filename test",
                    "version": 1,
                    "file": base_dir.join("charter.yaml").canonicalize().unwrap().to_string_lossy()
                },
                "job_id": FIXED_JOB_ID,
                "files": [
                    "20211219_082900000_transactions.csv"
                ]
            },
            {
                "groups": [[[0,3],[0,4],[0,5]]]
            },
            {
                "unmatched": [],
                "changesets": []
            }
        ]));


    // Create ANOTHER file with the same name.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv", file_content);

    celerity::run_charter(&charter, &base_dir).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1); // 3 in real life, but the fixed TS means only one.
    assert_eq!(get_dir_content(base_dir.join("archive")).unwrap().files.len(), 3);
    assert_eq!(common::get_filenames(&base_dir.join("archive")), vec!(
        "20211219_082900000_transactions.csv",
        "20211219_082900000_transactions.csv_01",
        "20211219_082900000_transactions.csv_02"));

    // Ensure the renamed archive file is recorded in the job.
    common::assert_matched_contents(base_dir.join("matched/20211201_053700000_matched.json"), json!(
        [
            {
                "charter": {
                    "name": "archive filename test",
                    "version": 1,
                    "file": base_dir.join("charter.yaml").canonicalize().unwrap().to_string_lossy()
                },
                "job_id": FIXED_JOB_ID,
                "files": [
                    "20211219_082900000_transactions.csv"
                ]
            },
            {
                "groups": [[[0,3],[0,4],[0,5]]]
            },
            {
                "unmatched": [],
                "changesets": []
            }
        ]));
}


#[test]
fn test_netting_to_zero_uses_abs_sides() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    let file_content = r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T00:00:00.000Z","100.00","T1"
"0","0002","2021-12-19T00:00:00.000Z","-75.00","T2"
"0","0003","2021-12-19T00:00:00.000Z","-25.00","T2"
"#;

    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv", file_content);

    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: archive filename test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - group:
        by: ['Date']
        match_when:
        - nets_to_zero:
            column: Amount
            lhs: record["Type"] == "T1"
            rhs: record["Type"] == "T2"
"#);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();
    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
}