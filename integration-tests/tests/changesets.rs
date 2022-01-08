use crate::common::{self, FIXED_JOB_ID, function};
use fs_extra::dir::get_dir_content;
use serde_json::json;

#[test]
fn test_changesets_are_recorded() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Write 4 transactions, each T1 should match a T2 - but initially the second pair won't match -
    // due to an incorrect amount for T2.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0","0002","2021-12-19T08:29:00.000Z","100.00","T2"
"0","0003","2021-12-18T08:29:00.000Z","100.00","T1"
"0","0004","2021-12-18T08:29:00.000Z","1000.00","T2"
"#);

    // Write a charter file.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: changeset test
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
            rhs: record["Type"] == "T2""#);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    // Check we have two unmatched records.
    common::assert_file_contents(&base_dir.join("unmatched/20211219_082900000_transactions.unmatched.csv"),
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0003","2021-12-18T08:29:00.000Z","100.00","T1"
"0","0004","2021-12-18T08:29:00.000Z","1000.00","T2"
"#);

    // Check the other matched records were recorded.
    common::assert_matched_contents(base_dir.join("matched/20211201_053700000_matched.json"), json!(
    [
        {
            "charter": {
                "name": "changeset test",
                "version": 1,
                "file": base_dir.join("charter.yaml").canonicalize().unwrap().to_string_lossy()
            },
            "job_id": FIXED_JOB_ID,
            "files": [ "20211219_082900000_transactions.csv" ]
        },
        {
            "groups": [ [[0,3],[0,4]] ]
        },
        {
            "changesets": [],
            "unmatched": [ { "file": "20211219_082900000_transactions.unmatched.csv", "rows": 2 } ]
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

    // Run the match again to apply the changes to correct the unmatched data.
    celerity::run_charter(&charter, &base_dir).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0,
        "The unmatched folder should be empty after the changeset was applied");

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
            "files": [ "20211219_082900000_transactions.unmatched.csv" ]
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
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T08:29:00.000Z","100.01","T1"
"0","0002","2021-12-19T08:29:00.000Z","100.00","T2"
"#);

    // Write 2 un-matched transactions which wont match until they are updated.
    common::write_file(&base_dir.join("unmatched/"), "20211218_082900000_transactions.unmatched.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0003","2021-12-18T08:29:00.000Z","100.00","T1"
"0","0004","2021-12-18T08:29:00.000Z","1000.00","T2"
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
        "lua_filter": "record[\"TransId\"] == 4"
    },
    "timestamp": "2021-12-20T06:18:00.000Z"
}
]"#;

    // Create a changeset to modify the unmatched data.
    common::write_file(&base_dir.join("waiting/"), "20211220_061800000_changeset.json", changeset);

    // Write a charter file.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: changeset test
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
            rhs: record["Type"] == "T2""#);

    // Run a match job.
    celerity::run_charter(&charter, &base_dir).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0,
        "The unmatched folder should be empty after the changeset was applied");

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
                "20211218_082900000_transactions.unmatched.csv",
                "20211219_082900000_transactions.csv"
            ]
        },
        {
            "groups": [ [[0,3],[0,4]], [[1,3],[1,4]] ]
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
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0","0002","2021-12-19T08:29:00.000Z","444.00","T2"
"#);

    // Write a charter file.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: changeset test
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

    // Create a changeset to modify the unmatched data.
    common::write_file(&base_dir.join("waiting/"), "20211220_061800000_changeset.json", changeset_1);
    common::write_file(&base_dir.join("waiting/"), "20211221_061800000_changeset.json", changeset_2);

    // Run a match job.
    celerity::run_charter(&charter, &base_dir).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0,
        "The unmatched folder should be empty after the changeset was applied");

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
            "files": [ "20211219_082900000_transactions.csv" ]
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
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0","0002","2021-12-19T08:29:00.000Z","100.00","T2"
"0","0003","2021-12-19T08:29:00.000Z","50.00","T2"
"#);

    // Write a charter file.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: changeset test
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
            rhs: record["Type"] == "T2""#);

    // Run a match job. There should be unmatched data at the end.
    celerity::run_charter(&charter, &base_dir).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 1,
        "The unmatched folder should have an unmatched file");

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

    // Create a changeset to modify the unmatched data.
    common::write_file(&base_dir.join("waiting/"), "20211220_061800000_changeset.json", changeset);

    // Run a match job again. There should be no more unmatched data at the end.
    celerity::run_charter(&charter, &base_dir).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0,
        "The unmatched folder should be empty");

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
            "files": [ "20211219_082900000_transactions.unmatched.csv" ]
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