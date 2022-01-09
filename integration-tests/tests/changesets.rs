use crate::common::{self, FIXED_JOB_ID, function};
use serde_json::json;

#[test]
fn test_changesets_can_release_unmatched_data_and_are_recorded() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Write 4 transactions, each T1 should match a T2 - but initially the second pair won't match -
    // due to an incorrect amount for T2.
    common::write_file(&base_dir.join("inbox/"), "transactions.csv",
r#""TransId","Date","Amount","Type"
"0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0002","2021-12-19T08:29:00.000Z","100.00","T2"
"0003","2021-12-18T08:29:00.000Z","100.00","T1"
"0004","2021-12-18T08:29:00.000Z","1000.00","T2"
"#);
    common::assert_n_files_in(1, "inbox", &base_dir);

    // Write a charter file.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: changeset test
version: 1
jetwash:
    source_files:
     - pattern: ^transactions\.csv$
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
            rhs: record["Type"] == "T2""#);

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (1, "waiting"),
        (1, "archive/jetwash")));

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check the files have been processed into the correct folders.
    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (0, "waiting"),
        (1, "archive/jetwash"),
        (1, "archive/celerity"),
        (1, "unmatched"),
        (1, "matched")));

    // Check we have two unmatched records.
    common::assert_file_contents(&base_dir.join("unmatched/20211201_053700000_transactions.unmatched.csv"),
r#""OpenRecStatus","OpenRecId","TransId","Date","Amount","Type"
"IN","ID","IN","DT","DE","ST"
"0","00000000-0000-0000-0000-000000000003","0003","2021-12-18T08:29:00.000Z","100.00","T1"
"0","00000000-0000-0000-0000-000000000004","0004","2021-12-18T08:29:00.000Z","1000.00","T2"
"#);

    // Check the other matched records were recorded.
    let matched = common::get_match_job_file(&base_dir);
    common::assert_matched_contents(matched, json!(
    [
        {
            "charter": {
                "name": "changeset test",
                "version": 1,
                "file": base_dir.join("charter.yaml").canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
            "files": [ "20211201_053700000_transactions.csv" ]
        },
        {
            "groups": [ [[0,3],[0,4]] ]
        },
        {
            "changesets": [],
            "unmatched": [ { "file": "20211201_053700000_transactions.unmatched.csv", "rows": 2 } ]
        }
    ]));

    // Write a changeset to amend the unmatched record.
    let changeset =
r#"    [
{
    "id": "53c4674e-60a7-11ec-a5fb-00155ddc3c4d",
    "change": {
        "type": "UpdateFields",
        "updates": [ { "field": "Amount", "value": "100.00" } ],
        "lua_filter": "record[\"TransId\"] == 4"
    },
    "timestamp": "2021-12-20T06:18:00.000Z"
}
]"#;

    // Create a changeset to modify the unmatched data.
    common::write_file(&base_dir.join("waiting/"), "20211220_061800000_changeset.json", changeset);

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (1, "waiting"),
        (1, "archive/jetwash"),
        (1, "archive/celerity"),
        (1, "unmatched"),
        (1, "matched")));

    // Run the match again to apply the changes to correct the unmatched data.
    celerity::run_charter(&charter, &base_dir).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (0, "waiting"),
        (1, "archive/jetwash"),
        (2, "archive/celerity"),
        (0, "unmatched"),
        (1, "matched"))); // Would be 2 in real life - but fixed TS overwrites same file.

    // Check the changeset is recorded and moved to the matched folder. Note, the fixed timestamp in tests
    // means the original match job file is overwritten with the second match job file.
    common::assert_file_contents(&base_dir.join("archive/celerity/20211220_061800000_changeset.json"), changeset);
    common::assert_matched_contents(base_dir.join("matched/20211201_053700000_matched.json"), json!(
    [
        {
            "charter": {
                "name": "changeset test",
                "version": 1,
                "file": base_dir.join("charter.yaml").canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
            "files": [ "20211201_053700000_transactions.unmatched.csv" ]
        },
        {
            "groups": [ [[0,3],[0,4]] ]
        },
        {
            "unmatched": [],
            "changesets": [
                {
                    "file": "20211220_061800000_changeset.json",
                    "updated": 1,
                    "ignored": 0
                }
            ]
        }
    ]));
}


#[test]
fn test_changesets_affect_unmatched_and_new_data() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Write 2 transactions which wont match until they are updated.
    common::write_file(&base_dir.join("inbox/"), "transactions.csv",
r#""TransId","Date","Amount","Type"
"0001","2021-12-19T08:29:00.000Z","100.01","T1"
"0002","2021-12-19T08:29:00.000Z","100.00","T2"
"#);

    // Write 2 un-matched transactions which wont match until they are updated.
    common::write_file(&base_dir.join("unmatched/"), "20211218_082900000_transactions.unmatched.csv",
r#""OpenRecStatus","OpenRecId","TransId","Date","Amount","Type"
"IN","ID","IN","DT","DE","ST"
"0","00000000-0000-0000-0000-000000000003","0003","2021-12-18T08:29:00.000Z","100.00","T1"
"0","00000000-0000-0000-0000-000000000004","0004","2021-12-18T08:29:00.000Z","1000.00","T2"
"#);

    // Write a changeset which should update a record in each file so everything matched.
    let changeset =
r#"    [
{
    "id": "53c4674e-60a7-11ec-a5fb-00155ddc3c4d",
    "change": {
        "type": "UpdateFields",
        "updates": [ { "field": "Amount", "value": "100.00" } ],
        "lua_filter": "record[\"TransId\"] == 1"
    },
    "timestamp": "2021-12-20T06:18:00.000Z"
},
{
    "id": "c5ffa24c-631d-11ec-a3f7-00155ddc360a",
    "change": {
        "type": "UpdateFields",
        "updates": [ { "field": "Amount", "value": "100.00" } ],
        "lua_filter": "record[\"OpenRecId\"] == \"00000000-0000-0000-0000-000000000004\""
    },
    "timestamp": "2021-12-20T06:18:00.000Z"
}
]"#;

    // Create a changeset to modify the unmatched data.
    common::write_file(&base_dir.join("inbox/"), "20211220_061800000_changeset.json", changeset);

    // Write a charter file.
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
  instructions:
    - group:
        by: ['Date']
        match_when:
        - nets_to_zero:
            column: Amount
            lhs: record["Type"] == "T1"
            rhs: record["Type"] == "T2""#);

    common::assert_files_in_folders(&base_dir, vec!(
        (2, "inbox"),
        (1, "unmatched")));

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (2, "waiting"),
        (2, "archive/jetwash"),
        (1, "unmatched")));

    // Run a match job.
    celerity::run_charter(&charter, &base_dir).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (0, "waiting"),
        (2, "archive/jetwash"),
        (3, "archive/celerity"),  // transactions.csv_01 contains post-changeset changes.
        (0, "unmatched"),
        (1, "matched")));

    // Check everything is matched.
    common::assert_matched_contents(base_dir.join("matched/20211201_053700000_matched.json"), json!(
    [
        {
            "charter": {
                "name": "changeset test",
                "version": 1,
                "file": base_dir.join("charter.yaml").canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
            "files": [
                "20211201_053700000_transactions.csv",
                "20211218_082900000_transactions.unmatched.csv"
            ]
        },
        {
            "groups": [ [[1,3],[1,4]], [[0,3],[0,4]] ]
        },
        {
            "unmatched": [],
            "changesets": [
                {
                    "file": "20211220_061800000_changeset.json",
                    "updated": 2,
                    "ignored": 0
                }
            ]
        }
    ]));
}


#[test]
fn test_changsets_are_applied_in_order() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 2 transactions which will only match if the last changeset has been applied.
    common::write_file(&base_dir.join("inbox/"), "transactions.csv",
r#""TransId","Date","Amount","Type"
"0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0002","2021-12-19T08:29:00.000Z","444.00","T2"
"#);

    // Write a charter file.
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
  instructions:
    - group:
        by: ['Date']
        match_when:
        - nets_to_zero:
            column: Amount
            lhs: record["Type"] == "T1"
            rhs: record["Type"] == "T2""#);

    // Create 2 changeset files with 2 changes in each - have them set the same field to different values.
    let changeset_1 =
r#"    [
{
    "id": "53c4674e-60a7-11ec-a5fb-00155ddc3c4d",
    "change": {
        "type": "UpdateFields",
        "updates": [ { "field": "Amount", "value": "111.00" } ],
        "lua_filter": "record[\"TransId\"] == 1"
    },
    "timestamp": "2021-12-20T06:18:00.000Z"
},
{
    "id": "c5ffa24c-631d-11ec-a3f7-00155ddc360a",
    "change": {
        "type": "UpdateFields",
        "updates": [ { "field": "Amount", "value": "222.00" } ],
        "lua_filter": "record[\"TransId\"] == 1"
    },
    "timestamp": "2021-12-20T06:18:00.000Z"
}
]"#;

    let changeset_2 =
r#"    [
{
    "id": "f3377a6c-6324-11ec-bc4d-00155ddc3e05",
    "change": {
        "type": "UpdateFields",
        "updates": [ { "field": "Amount", "value": "333.00" } ],
        "lua_filter": "record[\"TransId\"] == 1"
    },
    "timestamp": "2021-12-20T06:18:00.000Z"
},
{
    "id": "f3916ea0-6324-11ec-a8e6-00155ddc3e05",
    "change": {
        "type": "UpdateFields",
        "updates": [ { "field": "Amount", "value": "444.00" } ],
        "lua_filter": "record[\"TransId\"] == 1"
    },
    "timestamp": "2021-12-20T06:18:00.000Z"
}
]"#;

    common::write_file(&base_dir.join("inbox/"), "20211220_061800000_changeset.json", changeset_1);
    common::write_file(&base_dir.join("inbox/"), "20211221_061800000_changeset.json", changeset_2);

    common::assert_files_in_folders(&base_dir, vec!(
        (3, "inbox"),
        (0, "unmatched")));

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (3, "waiting"),
        (3, "archive/jetwash"),
        (0, "unmatched")));

    // Run a match job.
    celerity::run_charter(&charter, &base_dir).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (0, "waiting"),
        (3, "archive/jetwash"),
        (4, "archive/celerity"),  // transactions.csv_01 contains post-changeset changes.
        (0, "unmatched"),
        (1, "matched")));

    // Check everything is matched.
    common::assert_matched_contents(base_dir.join("matched/20211201_053700000_matched.json"), json!(
    [
        {
            "charter": {
                "name": "changeset test",
                "version": 1,
                "file": base_dir.join("charter.yaml").canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
            "files": [ "20211201_053700000_transactions.csv" ]
        },
        {
            "groups": [ [[0,3],[0,4]] ]
        },
        {
            "unmatched": [],
            "changesets": [
                {
                    "file": "20211220_061800000_changeset.json",
                    "updated": 2,
                    "ignored": 0
                },
                {
                    "file": "20211221_061800000_changeset.json",
                    "updated": 2,
                    "ignored": 0
                }
            ]
        }
    ]));
}


#[test]
fn test_changesets_can_ignore_records() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 2 transactions which will only match if the 3rd isn't present.
    common::write_file(&base_dir.join("inbox/"), "transactions.csv",
r#""TransId","Date","Amount","Type"
"0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0002","2021-12-19T08:29:00.000Z","100.00","T2"
"0003","2021-12-19T08:29:00.000Z","50.00","T2"
"#);

    // Write a charter file.
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
  instructions:
    - group:
        by: ['Date']
        match_when:
        - nets_to_zero:
            column: Amount
            lhs: record["Type"] == "T1"
            rhs: record["Type"] == "T2""#);

    common::assert_files_in_folders(&base_dir, vec!(
        (1, "inbox"),
        (0, "unmatched")));

    // Run the data-import.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (1, "waiting"),
        (1, "archive/jetwash"),
        (0, "unmatched")));

    // Run a match job. There should be unmatched data at the end.
    celerity::run_charter(&charter, &base_dir).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (0, "waiting"),
        (1, "archive/jetwash"),
        (1, "archive/celerity"),
        (1, "unmatched"),
        (1, "matched")));

    // Create a changeset to ignore the dodgey record.
    let changeset =
r#"    [
{
    "id": "f3377a6c-6324-11ec-bc4d-00155ddc3e05",
    "change": {
        "type": "IgnoreRecords",
        "lua_filter": "record[\"Amount\"] == decimal(50)"
    },
    "timestamp": "2021-12-20T06:18:00.000Z"
}
]"#;

    common::write_file(&base_dir.join("inbox/"), "20211220_061800000_changeset.json", changeset);

    // Run a match job again. There should be no more unmatched data at the end.
    jetwash::run_charter(&charter, &base_dir, Some(1)).unwrap();
    celerity::run_charter(&charter, &base_dir).unwrap();

    common::assert_files_in_folders(&base_dir, vec!(
        (0, "inbox"),
        (0, "waiting"),
        (2, "archive/jetwash"),
        (2, "archive/celerity"),
        (0, "unmatched"),
        (1, "matched"))); // Would be 2 in real life - but fixed TS means only one file.

    // Check the matched job file indicates the record was ignored.
    common::assert_matched_contents(base_dir.join("matched/20211201_053700000_matched.json"), json!(
    [
        {
            "charter": {
                "name": "changeset test",
                "version": 1,
                "file": base_dir.join("charter.yaml").canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
            "files": [ "20211201_053700000_transactions.unmatched.csv" ]
        },
        {
            "groups": [ [[0,3],[0,4]] ]
        },
        {
            "unmatched": [],
            "changesets": [
                {
                    "file": "20211220_061800000_changeset.json",
                    "updated": 0,
                    "ignored": 1
                }
            ]
        }
    ]));
}