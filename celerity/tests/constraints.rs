use crate::common::{self, function};
use assert_json_diff::assert_json_eq;
use fs_extra::dir::get_dir_content;
use serde_json::json;

#[test]
fn test_decimal_net_to_zero_constraint() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 3 transactions, with a 1:2 cardinality.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T00:00:00.000Z","100.00","T1"
"0","0002","2021-12-19T00:00:00.000Z","75.00","T2"
"0","0003","2021-12-19T00:00:00.000Z","25.00","T2"
"0","0004","2021-01-20T00:00:00.000Z","90.00","T1"
"0","0005","2021-01-20T00:00:00.000Z","75.00","T2"
"0","0006","2021-01-20T00:00:00.000Z","25.00","T2"
"#);

    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: count aggregate test
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
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 1);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
    assert_json_eq!(common::get_matched_groups(&base_dir), json!([ [[0,3], [0,4], [0,5]] ]));
}


#[test]
fn test_decimal_net_with_tolerance_constraint() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 3 transactions, with a 1:2 cardinality.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T00:00:00.000Z","99.00","T1"
"0","0002","2021-12-19T00:00:00.000Z","75.00","T2"
"0","0003","2021-12-19T00:00:00.000Z","25.00","T2"
"0","0004","2021-01-20T00:00:00.000Z","98.99","T1"
"0","0005","2021-01-20T00:00:00.000Z","75.00","T2"
"0","0006","2021-01-20T00:00:00.000Z","25.00","T2"
"#);

    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: count aggregate test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - group:
        by: ['Date']
        match_when:
        - nets_with_tolerance:
            column: Amount
            lhs: record["Type"] == "T1"
            rhs: record["Type"] == "T2"
            tol_type: Amount
            tolerance: 1.0
"#);

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 1);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
    assert_json_eq!(common::get_matched_groups(&base_dir), json!([ [[0,3], [0,4], [0,5]] ]));
}


#[test]
fn test_decimal_net_with_tolerance_percent_constraint() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 3 transactions, with a 1:2 cardinality.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T00:00:00.000Z","99.99","T1"
"0","0002","2021-12-19T00:00:00.000Z","75.00","T2"
"0","0003","2021-12-19T00:00:00.000Z","25.00","T2"
"0","0004","2021-01-20T00:00:00.000Z","58.99","T1"
"0","0005","2021-01-20T00:00:00.000Z","75.00","T2"
"0","0006","2021-01-20T00:00:00.000Z","25.00","T2"
"#);

    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: count aggregate test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - group:
        by: ['Date']
        match_when:
        - nets_with_tolerance:
            column: Amount
            lhs: record["Type"] == "T1"
            rhs: record["Type"] == "T2"
            tol_type: Percent
            tolerance: 1.0
"#);

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 1);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
    assert_json_eq!(common::get_matched_groups(&base_dir), json!([ [[0,3], [0,4], [0,5]] ]));
}


#[test]
fn test_integer_net_to_zero_constraint() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 3 transactions, with a 1:2 cardinality.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","IN","ST"
"0","0001","2021-12-19T00:00:00.000Z","100","T1"
"0","0002","2021-12-19T00:00:00.000Z","75","T2"
"0","0003","2021-12-19T00:00:00.000Z","25","T2"
"0","0004","2021-01-20T00:00:00.000Z","90","T1"
"0","0005","2021-01-20T00:00:00.000Z","75","T2"
"0","0006","2021-01-20T00:00:00.000Z","25","T2"
"#);

    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: count aggregate test
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
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 1);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
    assert_json_eq!(common::get_matched_groups(&base_dir), json!([ [[0,3], [0,4], [0,5]] ]));
}


#[test]
fn test_integer_net_with_tolerance_constraint() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 3 transactions, with a 1:2 cardinality.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","IN","ST"
"0","0001","2021-12-19T00:00:00.000Z","99","T1"
"0","0002","2021-12-19T00:00:00.000Z","75","T2"
"0","0003","2021-12-19T00:00:00.000Z","25","T2"
"0","0004","2021-01-20T00:00:00.000Z","98","T1"
"0","0005","2021-01-20T00:00:00.000Z","75","T2"
"0","0006","2021-01-20T00:00:00.000Z","25","T2"
"#);

    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: count aggregate test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - group:
        by: ['Date']
        match_when:
        - nets_with_tolerance:
            column: Amount
            lhs: record["Type"] == "T1"
            rhs: record["Type"] == "T2"
            tol_type: Amount
            tolerance: 1
"#);

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 1);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
    assert_json_eq!(common::get_matched_groups(&base_dir), json!([ [[0,3], [0,4], [0,5]] ]));
}

#[test]
fn test_integer_net_with_tolerance_percent_constraint() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 3 transactions, with a 1:2 cardinality.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","IN","ST"
"0","0001","2021-12-19T00:00:00.000Z","99","T1"
"0","0002","2021-12-19T00:00:00.000Z","75","T2"
"0","0003","2021-12-19T00:00:00.000Z","25","T2"
"0","0004","2021-01-20T00:00:00.000Z","58","T1"
"0","0005","2021-01-20T00:00:00.000Z","75","T2"
"0","0006","2021-01-20T00:00:00.000Z","25","T2"
"#);

    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: count aggregate test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - group:
        by: ['Date']
        match_when:
        - nets_with_tolerance:
            column: Amount
            lhs: record["Type"] == "T1"
            rhs: record["Type"] == "T2"
            tol_type: Percent
            tolerance: 1.1
"#);

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 1);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
    assert_json_eq!(common::get_matched_groups(&base_dir), json!([ [[0,3], [0,4], [0,5]] ]));
}

#[test]
fn test_custom_constraint_with_count() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 3 transactions, with a 1:2 cardinality.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0","0002","2021-12-19T08:29:00.000Z","75.00","T2"
"0","0003","2021-12-19T08:29:00.000Z","25.00","T2"
"#);

    // Create a charter with a custom constraint.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: count aggregate test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - group:
        by: ['Date']
        match_when:
        - custom:
            script: |
              local t1s = function (record) return record["Type"] == "T1" end
              local t2s = function (record) return record["Type"] == "T2" end

              return count(t1s, records) == 1 and count(t2s, records) == 2
"#);

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
}

#[test]
fn test_custom_constraint_with_sum_and_sum_int() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 3 transactions, with a 1:2 cardinality.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type","IntAmount"
"IN","IN","DT","DE","ST","IN"
"0","0001","2021-12-19T08:29:00.000Z","100.00","T1","550"
"0","0002","2021-12-19T08:29:00.000Z","75.00","T2","300"
"0","0003","2021-12-19T08:29:00.000Z","25.00","T2","250"
"#);

    // Create a charter with a custom constraint.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: count aggregate test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - group:
        by: ['Date']
        match_when:
        - custom:
            script: |
              local t1 = function (record) return record["Type"] == "T1" end
              local t2 = function (record) return record["Type"] == "T2" end

              return sum("Amount", t1, records) == decimal(100.00)
                and sum("Amount", t2, records) == decimal(100.00)
                and sum_int("IntAmount", t1, records) == 550
                and sum_int("IntAmount", t2, records) == 550
"#);

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
}


#[test]
fn test_custom_constraint_with_max_and_max_int() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 3 transactions, with a 1:2 cardinality.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type","IntAmount"
"IN","IN","DT","DE","ST","IN"
"0","0001","2021-12-19T08:29:00.000Z","100.00","T1","550"
"0","0002","2021-12-19T08:29:00.000Z","75.00","T2","300"
"0","0003","2021-12-19T08:29:00.000Z","25.00","T2","250"
"#);

    // Create a charter with a custom constraint.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: count aggregate test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - group:
        by: ['Date']
        match_when:
        - custom:
            script: |
              local t1 = function (record) return record["Type"] == "T1" end
              local t2 = function (record) return record["Type"] == "T2" end

              return max("Amount", t1, records) == decimal(100.00)
                and max("Amount", t2, records) == decimal(75.00)
                and max_int("IntAmount", t1, records) == 550
                and max_int("IntAmount", t2, records) == 300
"#);

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
}

#[test]
fn test_custom_constraint_with_min_and_min_int() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    // Create 3 transactions, with a 1:2 cardinality.
    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type","IntAmount"
"IN","IN","DT","DE","ST","IN"
"0","0001","2021-12-19T08:29:00.000Z","100.00","T1","550"
"0","0002","2021-12-19T08:29:00.000Z","75.00","T2","300"
"0","0003","2021-12-19T08:29:00.000Z","25.00","T2","250"
"#);

    // Create a charter with a custom constraint.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: count aggregate test
version: 1
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - group:
        by: ['Date']
        match_when:
        - custom:
            script: |
              local t1 = function (record) return record["Type"] == "T1" end
              local t2 = function (record) return record["Type"] == "T2" end

              return min("Amount", t1, records) == decimal(100.00)
                and min("Amount", t2, records) == decimal(25.00)
                and min_int("IntAmount", t1, records) == 550
                and min_int("IntAmount", t2, records) == 250
"#);

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
}