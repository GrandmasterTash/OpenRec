use clap::{App, Arg, ArgMatches};
use generator::Options;

mod data_type;
mod column;
mod schema;
mod group;
mod generator;

const ABOUT: &str = r#"Utility to generate psuedo-realistic random CSV transaction files that reference each other. Multiple transaction types are created. Invoices, Payments and receipts. Each file can be given a column schema or left to be randomly generated.

A column schema is a CSV string (no spaces) of data types for the generated rows, eg. ST,ST,ID,DT,DE. Each value represents the data-type of the column in the generated file.
Allowed values are: -
  BO - Boolean (1 or 0)                                 IN - Integer (-2^31 <-> 2^31-1)
  BY - Byte (-128 <-> 127)                              LO - Long Integer (-2^63 <-> 2^63-1)
  CH - Char (single unicode character)                  SH - Short Integer (-32,768 <-> 32,767)
  DA - Date (milliseconds since epoch - midnight time)  ST - String (UTF-8)
  DT - Datetime (number of milliseconds since epoch)    ID - UUID (v4 hyphenated)
  DE - Decimal (precise decimals)

Example Usage: -
   generator --invoice-schema ST,DT --payment-columns 12 --rows 15

This will create 15 invoices with a random string and datetime column, a random number of payments associated to the invoices with 12 random columns and a random number of receipts associated to the payments with 10 (default) random columns."#;

fn main() {
    // Parse the command-line args.
    let matches = App::new("CSV Data Generator")
        .version("1.0")
        .about(ABOUT)
        .arg(Arg::with_name("OUTPUT_DIR")
            .help("The output folder to create data in")
            .required(true)
            .short("o")
            .long("output")
            .takes_value(true))
        .arg(Arg::with_name("INVOICE_SCHEMA")
            .help("The schema string for the invoice file.")
            .required(false)
            .short("i")
            .long("invoice-schema")
            .takes_value(true))
        .arg(Arg::with_name("INVOICE_COLUMNS")
            .help("The number of columns for the invoice file. Used if invoice-schema not used. Defaults to 10.")
            .required(false)
            .long("invoice-columns")
            .takes_value(true))
        .arg(Arg::with_name("RECEIPT_SCHEMA")
            .help("The schema string for the receipt file.")
            .required(false)
            .short("r")
            .long("receipt-schema")
            .takes_value(true))
        .arg(Arg::with_name("RECEIPT_COLUMNS")
            .help("The number of columns for the receipt file. Used if receipt-schema not used. Defaults to 10.")
            .required(false)
            .long("receipt-columns")
            .takes_value(true))
        .arg(Arg::with_name("PAYMENT_SCHEMA")
            .help("The schema string for the payment file.")
            .required(false)
            .short("p")
            .long("payment-schema")
            .takes_value(true))
        .arg(Arg::with_name("PAYMENT_COLUMNS")
            .help("The number of columns for the payment file. Used if payment-schema not used. Defaults to 10.")
            .required(false)
            .long("payment-columns")
            .takes_value(true))
        .arg(Arg::with_name("ROWS")
            .help("An (optional) number of invoices to generate. Otherwise 10 will be created with varying numbers of payments and receipts.")
            .required(false)
            .long("rows")
            .takes_value(true))
        .arg(Arg::with_name("SEED")
            .help("An (optional) unsigned long-integer used to seed the random number generator. Passing the same value should always yield repropduceable output")
            .required(false)
            .short("s")
            .long("seed")
            .takes_value(true))
        .get_matches();

    generator::generate(matches.into()).expect("Failed to generate data")
}

///
/// Parse value into T (if specified) or return None if not, panic if value cant parse.
///
fn parse<T: std::str::FromStr>(value: Option<&str>, msg: &str) -> Option<T> {
    match value {
        None => None,
        Some(value) => {
            match value.parse::<T>() {
                Ok(value) => Some(value),
                Err(_err) => panic!("{} if specified, must be numeric", msg),
            }
        }
    }
}

impl From<ArgMatches<'static>> for Options {
    fn from(matches: ArgMatches<'static>) -> Self {
        Self {
            output: matches.value_of("OUTPUT_DIR").map(str::to_string),
            inv_schema: matches.value_of("INVOICE_SCHEMA").map(str::to_string),
            rec_schema: matches.value_of("RECEIPT_SCHEMA").map(str::to_string),
            pay_schema: matches.value_of("PAYMENT_SCHEMA").map(str::to_string),
            inv_columns: parse(matches.value_of("INVOICE_COLUMNS"), "invoice-columns"),
            pay_columns: parse(matches.value_of("PAYMENT_COLUMNS"), "payment-columns"),
            rec_columns: parse(matches.value_of("RECEIPT_COLUMNS"), "receipt-columns"),
            rows: parse(matches.value_of("ROWS"), "rows"),
            rnd_seed: parse(matches.value_of("SEED"), "seed"),
        }
    }
}