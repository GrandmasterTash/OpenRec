mod common;

// TODO: Write a test for each of the examples.

#[test]
fn test01_basic_match_from_examples() {

    // Ensure files are created with a fixed timestamp in their filename. This makes testing easier.
    std::env::set_var("OPENREC_FIXED_TS", "20211201_053700000");

    // Copy the test data files into a temporary working folder.
    let base_dir = format!("{}/tests/test01", env!("CARGO_TARGET_TMPDIR"));
    common::init_base_dir(vec!(
        &format!("{}/../examples/data/20211129_043300000_01-invoices.csv", env!("CARGO_MANIFEST_DIR")),
        &format!("{}/../examples/data/20211129_043300000_01-payments.csv", env!("CARGO_MANIFEST_DIR"))),
        &base_dir);

    // Run the match.
    matcher::run_charter("../examples/01-Basic-Match.yaml", base_dir).unwrap()

    // TODO: Test the match groups and un-matched folder is empty.

    // TODO: Assert the exact file contents of ./tmp/01


}