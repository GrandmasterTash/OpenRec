use fs_extra::dir::get_dir_content;
use crate::common::{function, self};

#[test]
fn test_global_lua_in_projections() {

    let base_dir = common::init_test(format!("tests/{}", function!()));

    common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T00:00:00.000Z","100.00","T1"
"0","0002","2021-12-19T00:00:00.000Z","75.00","CHANGE_ME2"
"0","0003","2021-12-19T00:00:00.000Z","25.00","CHANGE_ME2"
"0","0004","2021-01-20T00:00:00.000Z","100.00","T1"
"0","0005","2021-01-20T00:00:00.000Z","75.00","CHANGE_ME2"
"0","0006","2021-01-20T00:00:00.000Z","25.00","CHANGE_ME2"
"#);

    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: global lua projection test
version: 1
global_lua: |
  function starts_with(str, start)
    return str:sub(1, #start) == start
  end
matching:
  use_field_prefixes: false
  source_files:
    - pattern: .*.csv
  instructions:
    - project:
        column: NewType
        as_a: String
        from: |
            if starts_with(record["Type"], "CHANGE_ME") then
              return "T2"
            else
              return "T1"
            end
    - group:
        by: ['Date']
        match_when:
        - nets_to_zero:
            column: Amount
            lhs: record["NewType"] == "T1"
            rhs: record["NewType"] == "T2"
"#);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
}


#[test]
fn test_global_lua_in_constraints() {

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
r#"name: global lua constraint test
version: 1
global_lua: |
  only_t1s = function (record) return record["Type"] == "T1" end
  only_t2s = function (record) return record["Type"] == "T2" end
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
              return sum("Amount", only_t1s, records) == decimal(100.00)
                and sum("Amount", only_t2s, records) == decimal(100.00)
                and sum_int("IntAmount", only_t1s, records) == 550
                and sum_int("IntAmount", only_t2s, records) == 550
"#);

    // Run the match.
    celerity::run_charter(&charter, &base_dir).unwrap();

    assert_eq!(get_dir_content(base_dir.join("unmatched")).unwrap().files.len(), 0);
    assert_eq!(get_dir_content(base_dir.join("matched")).unwrap().files.len(), 1);
}