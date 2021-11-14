use uuid::Uuid;
use csv::QuoteStyle;
use self::prelude::*;
use std::time::Instant;
use rust_decimal::prelude::*;
use humantime::format_duration;
use num_format::{Locale, ToFormattedString};
use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use rand::{Rng, SeedableRng, prelude::{SliceRandom, StdRng}};
use crate::{column::*, data_type::DataType, schema::Schema};


pub mod prelude {
    // Snaphot of ISO currency codes.
    pub const CURRENCIES: [&str; 162] = ["AED", "AFN", "ALL", "AMD", "ANG", "AOA", "ARS", "AUD", "AWG", "AZN", "BAM", "BBD", "BDT", "BGN", "BHD", "BIF", "BMD", "BND", "BOB", "BRL", "BSD", "BTN", "BWP", "BYN", "BZD", "CAD", "CDF", "CHF", "CLP", "CNY", "COP", "CRC", "CUC", "CUP", "CVE", "CZK", "DJF", "DKK", "DOP", "DZD", "EGP", "ERN", "ETB", "EUR", "FJD", "FKP", "GBP", "GEL", "GGP", "GHS", "GIP", "GMD", "GNF", "GTQ", "GYD", "HKD", "HNL", "HRK", "HTG", "HUF", "IDR", "ILS", "IMP", "INR", "IQD", "IRR", "ISK", "JEP", "JMD", "JOD", "JPY", "KES", "KGS", "KHR", "KMF", "KPW", "KRW", "KWD", "KYD", "KZT", "LAK", "LBP", "LKR", "LRD", "LSL", "LYD", "MAD", "MDL", "MGA", "MKD", "MMK", "MNT", "MOP", "MRU", "MUR", "MVR", "MWK", "MXN", "MYR", "MZN", "NAD", "NGN", "NIO", "NOK", "NPR", "NZD", "OMR", "PAB", "PEN", "PGK", "PHP", "PKR", "PLN", "PYG", "QAR", "RON", "RSD", "RUB", "RWF", "SAR", "SBD", "SCR", "SDG", "SEK", "SGD", "SHP", "SLL", "SOS", "SPL", "SRD", "STN", "SVC", "SYP", "SZL", "THB", "TJS", "TMT", "TND", "TOP", "TRY", "TTD", "TVD", "TWD", "TZS", "UAH", "UGX", "USD", "UYU", "UZS", "VEF", "VND", "VUV", "WST", "XAF", "XCD", "XDR", "XOF", "XPF", "YER", "ZAR", "ZMW", "ZWD"];
    pub const RANDOM_ALPHANUMERIC: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    pub const RANDOM_ALPHA: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    pub const RANDOM_NUMERIC: &str = "0123456789";
    pub const RANDOM_SEPARATORS: &str = "-/_ ";
}

pub struct Options {
    pub inv_schema: Option<String>, // Schemas should be provided OR the number of columns to generate.
    pub rec_schema: Option<String>, // Schemas take precedence of the number of columns.
    pub pay_schema: Option<String>,
    pub inv_columns: Option<usize>,
    pub rec_columns: Option<usize>,
    pub pay_columns: Option<usize>,
    pub rows: Option<u64>,
    pub rnd_seed: Option<u64>
}

// TODO: Look at 1-n record generation.

///
/// A utility to generate some related CSV data.
///
pub fn generate(options: Options) -> Result<(), csv::Error> {

    let start = Instant::now();

    let rnd_seed = options.rnd_seed.unwrap_or(1234567890u64);
    let mut rng = StdRng::seed_from_u64(rnd_seed);
    let inv_schema = column_schema(options.inv_schema, options.inv_columns, &mut rng);
    let pay_schema = column_schema(options.pay_schema, options.pay_columns, &mut rng);
    let rec_schema = column_schema(options.rec_schema, options.rec_columns, &mut rng);

    // Turn the ID,ST,DT,DE type strings into real schemas with some randomness to field lengths.
    let inv_schema = Schema::new(&inv_schema, &mut rng);
    let pay_schema = Schema::new(&pay_schema, &mut rng);
    let rec_schema = Schema::new(&rec_schema, &mut rng);

    // Output the column headers to both files.
    let mut inv_wtr = csv::WriterBuilder::new().quote_style(QuoteStyle::NonNumeric).from_path("./tmp/invoice.csv")?;
    inv_wtr.write_record(inv_schema.header_vec())?;

    let mut pay_wtr = csv::WriterBuilder::new().quote_style(QuoteStyle::NonNumeric).from_path("./tmp/payments.csv")?;
    pay_wtr.write_record(pay_schema.header_vec())?;

    let mut rec_wtr = csv::WriterBuilder::new().quote_style(QuoteStyle::NonNumeric).from_path("./tmp/receipts.csv")?;
    rec_wtr.write_record(rec_schema.header_vec())?;

    // Initialise some counters.
    let (mut invoices, mut receipts, mut payments) = (0, 0, 0);

    // Generate some random CSV rows.
    for _row in 1..=options.rows.unwrap_or(10) {
        // Generate a common reference that links this row in both files.
        let foreign_key = format!("REF-{}", generate_ref(&mut rng, &SegmentMeta::default()));

        inv_wtr.write_record(
            generate_row(&inv_schema, &foreign_key, "INV", &mut rng)
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>())?;
        invoices +=1;

        pay_wtr.write_record(
            generate_row(&pay_schema,&foreign_key, "PAY", &mut rng)
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>())?;
        payments += 1;

        rec_wtr.write_record(
            generate_row(&rec_schema, &foreign_key, "REC", &mut rng)
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>())?;
        receipts += 1;
    }

    println!("Generated data in {dur} using seed {seed}\n  {inv} invoices\n  {pay} payments\n  {rec} receipts",
        inv = invoices.to_formatted_string(&Locale::en),
        pay = payments.to_formatted_string(&Locale::en),
        rec = receipts.to_formatted_string(&Locale::en),
        dur = format_duration(start.elapsed()),
        seed = rnd_seed
    );

    // Generate a 1-to-n pair of output files (invoice and receiptes).
    inv_wtr.flush()?;
    pay_wtr.flush()?;
    rec_wtr.flush()?;
    Ok(())
}

///
/// Use the schema specified or generate a random one.
///
fn column_schema(schema: Option<String>, columns: Option<usize>, rng: &mut StdRng) -> String {
    match schema {
        Some(schema) => schema.into(),
        None => random_schema(columns.unwrap_or(10), rng),
    }
}

///
/// Generate a random set of column data-types.
///
fn random_schema(cols: usize, rng: &mut StdRng) -> String {
    let mut schema = String::new();

    for col in 1..=cols {
        schema += match rng.gen_range(1..=11) {
            1  => DataType::BOOLEAN.into(),
            2  => DataType::BYTE.into(),
            3  => DataType::CHAR.into(),
            4  => DataType::DATE.into(),
            5  => DataType::DATETIME.into(),
            6  => DataType::DECIMAL.into(),
            7  => DataType::INTEGER.into(),
            8  => DataType::LONG.into(),
            9  => DataType::SHORT.into(),
            10 => DataType::STRING.into(),
            _  => DataType::UUID.into(),
        };

        if col < cols {
            schema += ",";
        }
    }

    schema
}

///
/// Generate a random reference string used to link records in different files.
///
fn generate_ref(rng: &mut StdRng, meta: &SegmentMeta) -> String {
    let mut reference = String::new();

    for segment in 0..meta.segment_types().len() {
        for _idx in 0..meta.segment_lens()[segment] {
            let slice = match meta.segment_types()[segment] {
                SegmentType::ALPHA        => RANDOM_ALPHA,
                SegmentType::NUMERIC      => RANDOM_NUMERIC,
                SegmentType::ALPHANUMERIC => RANDOM_ALPHANUMERIC,
            };

            reference += &rand_char(slice, rng).to_string();
        }

        if segment < (meta.segment_types().len()-1) {
            reference += &meta.separators()[segment].to_string();
        }
    }

    reference
}

///
/// Generate a random row of data using the Schema and meta data provided.
///
fn generate_row(schema: &Schema, foreign_key: &str, record_type: &str, rng: &mut StdRng) -> Vec<String> {

    schema.columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            match idx {
                0 => record_type.to_string(),
                1 => foreign_key.to_string(),
                _ => match col.data_type() {
                    DataType::UNKNOWN  => panic!("Unknown data type encountered at position {}", idx),
                    DataType::BOOLEAN  => generate_boolean(rng),
                    DataType::BYTE     => generate_byte(rng),
                    DataType::CHAR     => generate_char(rng),
                    DataType::DATE     => generate_date(rng),
                    DataType::DATETIME => generate_datetime(rng),
                    DataType::DECIMAL  => generate_decimal(rng, col.meta()),
                    DataType::INTEGER  => generate_integer(rng, col.meta()),
                    DataType::LONG     => generate_long(rng, col.meta()),
                    DataType::SHORT    => generate_short(rng),
                    DataType::STRING   => generate_string(rng, col.meta()),
                    DataType::UUID     => generate_uuid(),
                }
            }
        })
        .collect()
}

///
/// Generate a random reference, currency or gibberish depending on the column generation metadata.
///
fn generate_string(rng: &mut StdRng, meta: &ColumnMeta) -> String {

    if let Some(ref_meta) = &meta.segments() {
        return generate_ref(rng, ref_meta)
    }

    if let Some(cur_meta) = &meta.currency() {
        match &cur_meta.code() {
            Some(code) => return code.clone(),
            None => return rand_currency(),
        }
    }

    // Return some random gibberish.
    rand_chars(rng.gen_range(0..10), RANDOM_ALPHANUMERIC, rng)
}

///
/// Generate a v4 UUID hyphenated string.
///
fn generate_uuid() -> String {
    Uuid::new_v4().to_hyphenated().to_string()
}

///
/// Generate a random 2-byte integer.
///
fn generate_short(rng: &mut StdRng) -> String {
    format!("{}", rng.gen_range(-32767..32767))
}

///
/// Generate a random long up to the precision length in the metadata.
///
fn generate_long(rng: &mut StdRng, meta: &ColumnMeta) -> String {
    let precision = meta.long().as_ref().unwrap().precision() as usize;
    rand_chars(precision, RANDOM_NUMERIC, rng)
}

///
/// Generate a random integer up to the precision length in the metadata.
///
fn generate_integer(rng: &mut StdRng, meta: &ColumnMeta) -> String {
    let precision = meta.integer().as_ref().unwrap().precision() as usize;
    rand_chars(precision, RANDOM_NUMERIC, rng)
}

///
/// Generate a random decimal number using the column's precision and scale to determine how many digits.
///
fn generate_decimal(rng: &mut StdRng, meta: &ColumnMeta) -> String {
    let meta = meta.decimal().as_ref().unwrap();
    let precision = meta.precision() as usize;
    let scale = meta.scale() as u32;
    let rnd_nums = rand_chars(precision, RANDOM_NUMERIC, rng);
    let decimal = Decimal::new(
        rnd_nums.parse::<i64>().expect(&format!("Bad number {}", rnd_nums)),
        scale);

    format!("{}", decimal)
}

///
/// Generate a random date from 1 year ago to 1 years time (loosly).
///
fn generate_date(rng: &mut StdRng) -> String {
    let y = Utc::now().year() - 2 + rng.gen_range(0..3);  // Generate a year within 1 year of the current.
    let m = rng.gen_range(1..13);                         // Generate a random month.
    let d = rng.gen_range(1..days_in_month(y, m) as u32); // Generate a random day from this month.
    let dt = Utc.ymd(y, m, d).and_hms(0, 0, 0);
    format!("{}", dt.timestamp_millis())
}

///
/// Generate a random date from 1 year ago to 1 years time (loosly).
///
fn generate_datetime(rng: &mut StdRng) -> String {
    let y = Utc::now().year() - 2 + rng.gen_range(0..3);  // Generate a year within 1 year of the current.
    let m = rng.gen_range(1..13);                         // Generate a random month.
    let d = rng.gen_range(1..days_in_month(y, m) as u32); // Generate a random day from this month.
    let h = rng.gen_range(0..24);
    let mi = rng.gen_range(0..60);
    let s = rng.gen_range(0..60);

    let dt = Utc.ymd(y, m, d).and_hms(h, mi, s);
    format!("{}", dt.timestamp_millis())
}

///
/// Return the number of days in the specified month.
///
fn days_in_month(year: i32, month: u32) -> i64 {
    NaiveDate::from_ymd(
        match month {
            12 => year + 1,
            _ => year,
        },
        match month {
            12 => 1,
            _ => month + 1,
        },
        1,
    )
    .signed_duration_since(NaiveDate::from_ymd(year, month, 1))
    .num_days()
}

///
/// Generate a random character from the alpha-numeric range.
///
fn generate_char(rng: &mut StdRng) -> String {
    rand_char(RANDOM_ALPHANUMERIC, rng).to_string()
}

///
/// Generate a positive random short.
///
fn generate_byte(rng: &mut StdRng) -> String {
    format!("{}", rng.gen_range(0..128))
}

///
/// Generate a random boolean
///
fn generate_boolean(rng: &mut StdRng) -> String {
    let result = match rng.gen_bool(1. / 2.) {
        true  => 1,
        false => 0,
    };
    format!("{}", result)
}

///
/// Select a random currency code.
///
pub fn rand_currency() -> String {
    CURRENCIES.choose(&mut rand::thread_rng()).unwrap().to_string()
}

///
/// Select a random char from the &str slice and clone it.
///
pub fn rand_char(slice: &str, rng: &mut StdRng) -> char {
    slice.chars()
        .collect::<Vec<char>>()
        .choose(rng)
        .unwrap()
        .clone()
}

///
/// Select a number of random chars from the &str slice and clone it.
///
fn rand_chars(amount: usize, slice: &str, rng: &mut StdRng) -> String {
    slice.chars()
        .collect::<Vec<char>>()
        .choose_multiple(rng, amount)
        .cloned()
        .collect()
}

