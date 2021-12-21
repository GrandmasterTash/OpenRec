use crate::common;
use fs_extra::dir::get_dir_content;
use serde_json::json;

#[test]
fn test_changesets_are_recorded() {

    let base_dir = common::init_test("tests/test_changesets_are_recorded/");

    // Write 4 transactions, each T1 should match a T2 - but initially the second pair won't match -
    // due to an incorrect amount for T2.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""TransId","Date","Amount","Type"
"IN","DT","DE","ST"
"0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0002","2021-12-19T08:29:00.000Z","100.00","T2"
"0003","2021-12-18T08:29:00.000Z","100.00","T1"
"0004","2021-12-18T08:29:00.000Z","1000.00","T2"
"#);

    // Write a charter file.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: changeset test
version: 1
file_patterns: ['.*.csv']
use_field_prefixes: false
instructions:
    - match_groups:
        group_by: ['Date']
        constraints:
        - nets_to_zero:
            column: Amount
            lhs: record["Type"] == "T1"
            rhs: record["Type"] == "T2""#);

    // Run the match.
    matcher::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    // Check we have two unmatched records.
    common::assert_file_contents(&base_dir.join("unmatched/20211219_082900000_transactions.unmatched.csv"),
r#""TransId","Date","Amount","Type"
"IN","DT","DE","ST"
"0003","2021-12-18T08:29:00.000Z","100.00","T1"
"0004","2021-12-18T08:29:00.000Z","1000.00","T2"
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
            "files": [ "20211219_082900000_transactions.csv" ]
        },
        {
            "groups": [ [[0,3],[0,4]] ]
        },
        {
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
    matcher::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0,
        "The unmatched folder should be empty after the changeset was applied");

    // Check the changeset is recorded and moved to the matched folder. Note, the fixed timestamp in tests
    // means the original match job file is overwritten with the second match job file.
    common::assert_file_contents(&base_dir.join("matched/20211220_061800000_changeset.json"), changeset);
    common::assert_matched_contents(base_dir.join("matched/20211201_053700000_matched.json"), json!(
    [
        {
            "charter": {
                "name": "changeset test",
                "version": 1,
                "file": base_dir.join("charter.yaml").canonicalize().unwrap().to_string_lossy()
            },
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

// fn test_changesets_affect_unmatched_and_new_data() {
// }

// fn test_changsets_are_applied_in_order() {
// }