use serde_json::json;
use crate::common::{self, FIXED_JOB_ID};

#[test]
fn test_01_basic_match_from_examples() {

    let charter = common::example_charter("01-Basic-Match.yaml");
    let data_files = common::example_data_files(vec!("01-invoices.csv", "01-payments.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_01_basic_match_from_examples/", &data_files);
    common::assert_n_files_in(2, "inbox", &base_dir);

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(2, "waiting", &base_dir);
    common::assert_n_files_in(2, "archive/jetwash", &base_dir);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(0, "waiting", &base_dir);
    common::assert_n_files_in(2, "archive/jetwash", &base_dir);
    common::assert_n_files_in(2, "archive/celerity", &base_dir);
    common::assert_n_files_in(0, "unmatched", &base_dir);
    common::assert_n_files_in(1, "matched", &base_dir);

    // Check the output files.
    let matched = common::get_match_job_file(&base_dir);

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
                "20211201_053700000_01-invoices.csv",
                "20211201_053700000_01-payments.csv" ]
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
    let data_files = common::example_data_files(vec!("02-invoices.csv", "02-payments.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_02_projected_columns_from_examples/", &data_files);
    common::assert_n_files_in(2, "inbox", &base_dir);

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(2, "waiting", &base_dir);
    common::assert_n_files_in(2, "archive/jetwash", &base_dir);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(0, "waiting", &base_dir);
    common::assert_n_files_in(2, "archive/jetwash", &base_dir);
    common::assert_n_files_in(2, "archive/celerity", &base_dir);
    common::assert_n_files_in(0, "unmatched", &base_dir);
    common::assert_n_files_in(1, "matched", &base_dir);

    // Check the output files.
    let matched = common::get_match_job_file(&base_dir);

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
                "20211201_053700000_02-invoices.csv",
                "20211201_053700000_02-payments.csv"
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
    let data_files = common::example_data_files(vec!("03-invoices.csv", "03-payments.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_03_net_with_tolerance_match_from_examples/", &data_files);
    common::assert_n_files_in(2, "inbox", &base_dir);

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(2, "waiting", &base_dir);
    common::assert_n_files_in(2, "archive/jetwash", &base_dir);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(0, "waiting", &base_dir);
    common::assert_n_files_in(2, "archive/jetwash", &base_dir);
    common::assert_n_files_in(2, "archive/celerity", &base_dir);
    common::assert_n_files_in(0, "unmatched", &base_dir);
    common::assert_n_files_in(1, "matched", &base_dir);

    // Check the output files.
    let matched = common::get_match_job_file(&base_dir);

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
                "20211201_053700000_03-invoices.csv",
                "20211201_053700000_03-payments.csv"
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
    let data_files = common::example_data_files(vec!("04-invoices.csv", "04-payments.csv", "04-receipts.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_04_3_way_match_from_examples", &data_files);
    common::assert_n_files_in(3, "inbox", &base_dir);

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(3, "waiting", &base_dir);
    common::assert_n_files_in(3, "archive/jetwash", &base_dir);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(0, "waiting", &base_dir);
    common::assert_n_files_in(3, "archive/jetwash", &base_dir);
    common::assert_n_files_in(3, "archive/celerity", &base_dir);
    common::assert_n_files_in(0, "unmatched", &base_dir);
    common::assert_n_files_in(1, "matched", &base_dir);

    // Check the output files.
    let matched = common::get_match_job_file(&base_dir);

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
                "20211201_053700000_04-invoices.csv",
                "20211201_053700000_04-payments.csv",
                "20211201_053700000_04-receipts.csv"
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
    let data_files = common::example_data_files(vec!("05-2-stage.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_05_2_stage_match_from_examples/", &data_files);
    common::assert_n_files_in(1, "inbox", &base_dir);

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(1, "waiting", &base_dir);
    common::assert_n_files_in(1, "archive/jetwash", &base_dir);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(0, "waiting", &base_dir);
    common::assert_n_files_in(1, "archive/jetwash", &base_dir);
    common::assert_n_files_in(1, "archive/celerity", &base_dir);
    common::assert_n_files_in(0, "unmatched", &base_dir);
    common::assert_n_files_in(1, "matched", &base_dir);

    // Check the output files.
    let matched = common::get_match_job_file(&base_dir);

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
            "files": [ "20211201_053700000_05-2-stage.csv" ]
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
    let mut data_files = common::example_data_files(vec!("07-invoices.csv", "07-payments-a.csv"));

    // Copy the test data files into a temporary working folder.
    let base_dir = common::init_test_from_examples("tests/examples/test_07_unmatched_data_from_examples/", &data_files);
    common::assert_n_files_in(2, "inbox", &base_dir);

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(2, "waiting", &base_dir);
    common::assert_n_files_in(2, "archive/jetwash", &base_dir);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(0, "waiting", &base_dir);
    common::assert_n_files_in(2, "archive/jetwash", &base_dir);
    common::assert_n_files_in(2, "archive/celerity", &base_dir);
    common::assert_n_files_in(2, "unmatched", &base_dir);
    common::assert_n_files_in(1, "matched", &base_dir); // TODO: Can we use a nicer api for these folder asserts?

    // Check the matched file contains the correct groupings.
    let matched = common::get_match_job_file(&base_dir);
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
                "20211201_053700000_07-invoices.csv",
                "20211201_053700000_07-payments-a.csv"
            ]
        },
        {
            "groups": [ [[0,4],[1,4]] ]
        },
        {
          "changesets": [],
          "unmatched": [
            {
                "file": "20211201_053700000_07-invoices.unmatched.csv",
                "rows": 1
            },
            {
                "file": "20211201_053700000_07-payments-a.unmatched.csv",
                "rows": 1
            }
          ]
        }
    ]));


    // Compare the unmatched invoices.
    let expected = r#""OpenRecStatus","OpenRecId","Invoice No","InvoiceRef","Invoice Date","InvoiceAmount"
"IN","ID","IN","ST","DT","DE"
"0","00000000-0000-0000-0000-000000000001","0001","INV0001","2021-11-25T04:36:08.000Z","1050.99"
"#;
    common::assert_file_contents(&base_dir.join("unmatched").join("20211201_053700000_07-invoices.unmatched.csv"), expected);

    // Compare the unmatched payments.
    let expected = r#""OpenRecStatus","OpenRecId","PaymentId","PaymentRef","PaymentAmount","Payment Date"
"IN","ID","ST","ST","DE","DT"
"0","00000000-0000-0000-0000-000000000003","P1","INV0001","50.99","2021-11-27T04:36:08.000Z"
"#;
    common::assert_file_contents(&base_dir.join("unmatched").join("20211201_053700000_07-payments-a.unmatched.csv"), expected);

    // Now copy payments-b in and run the match charter again.
    data_files.push(common::copy_example_data_file("07-payments-b.csv", &base_dir));
    common::assert_n_files_in(1, "inbox", &base_dir);
    common::assert_n_files_in(0, "waiting", &base_dir);

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(1, "waiting", &base_dir);
    common::assert_n_files_in(3, "archive/jetwash", &base_dir);
    common::assert_n_files_in(2, "archive/celerity", &base_dir);
    common::assert_n_files_in(2, "unmatched", &base_dir);
    common::assert_n_files_in(1, "matched", &base_dir);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_n_files_in(0, "inbox", &base_dir);
    common::assert_n_files_in(0, "waiting", &base_dir);
    common::assert_n_files_in(3, "archive/jetwash", &base_dir);
    common::assert_n_files_in(3, "archive/celerity", &base_dir);
    common::assert_n_files_in(0, "unmatched", &base_dir);
    common::assert_n_files_in(1, "matched", &base_dir); // 2 in real life - but we've fixed the TS.

    /*
    // TODO: Make this assert use this style api.
    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (0, "waiting"),
        (3, "archive/jetwash"),
        (3, "archive/celerity"),
        (0, "unmatched"),
        (1, "matched")));
    */

    // Check the matched file contains the correct groupings.
    let matched = common::get_match_job_file(&base_dir);

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
                "20211201_053700000_07-invoices.unmatched.csv",
                "20211201_053700000_07-payments-a.unmatched.csv",
                "20211201_053700000_07-payments-b.csv"
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