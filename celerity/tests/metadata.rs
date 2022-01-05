use crate::common;

#[test]
fn test_all_meta_present_in_lua() {

    let base_dir = common::init_test("tests/test_all_meta_present_in_lua/");

    // Match two records, with a constraint rule that requires certain metadata fields are present.
    let _data_file = common::write_file(&base_dir.join("waiting/"), "20211219_082900000_transactions.csv",
r#""OpenRecStatus","TransId","Date","Amount","Type"
"IN","IN","DT","DE","ST"
"0","0001","2021-12-19T08:29:00.000Z","100.00","T1"
"0","0002","2021-12-19T08:29:00.000Z","100.00","T2"
"#);

    // Write a charter file.
    let charter = common::write_file(&base_dir, "charter.yaml",
r#"name: lua metadata test
version: 1
matching:
  use_field_prefixes: true
  source_files:
    - pattern: .*.csv
      field_prefix: TRN
  instructions:
    - group:
        by: ['TRN.Date']
        match_when:
        - custom:
            script: |
              for idx, record in ipairs(records) do
                -- There must be a prefix metadata field for each record.
                if record["META.prefix"] ~= "TRN" then
                  print("Record meta prefix field was not set in Lua script test will fail as no match.")
                  print("EXPECTED: record['META.prefix'] -> TRN")
                  print("ACTUAL:   record['META.prefix'] -> " .. record["META.prefix"])
                  return false
                end

                -- There must be a filename metadata field for each record.
                if record["META.filename"] ~= "20211219_082900000_transactions.csv" then
                  print("Record meta filename field was not set in Lua script test will fail as no match.")
                  print("EXPECTED: record['META.filename'] -> 20211219_082900000_transactions.csv")
                  print("ACTUAL:   record['META.filename'] -> " .. record["META.filename"])
                  return false
                end

                -- There must be a timestamp metadata field for each record. This is taken
                -- from the file prefix ('20211219_082900000' in this case) and then turned
                -- into a unix timestamp.

                if record["META.timestamp"] ~= 1639902540000 then
                  print("Record meta timestamp field was not set in Lua script test will fail as no match.")
                  print("EXPECTED: record['META.timestamp'] -> 1639902540000")
                  print("ACTUAL:   record['META.timestamp'] -> " .. record["META.timestamp"])
                  return false
                end
              end

              print("Well done, all the metadata is present in the Lua script")
              return true
            "#);

    // Run the match.
    celerity::run_charter(&charter.to_string_lossy(), &base_dir.to_string_lossy()).unwrap();

    // Check the output files.
    let _matched = common::assert_matched_ok(&vec!(), &base_dir);
}