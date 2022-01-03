use uuid::Uuid;
use csv::QuoteStyle;
use self::prelude::*;
use std::time::Instant;
use rust_decimal::prelude::*;
use humantime::format_duration;
use num_format::{Locale, ToFormattedString};
use chrono::{Datelike, NaiveDate, TimeZone, Utc, SecondsFormat};
use rand::{Rng, SeedableRng, prelude::{SliceRandom, StdRng}};
use crate::{column::*, data_type::DataType, group::Group, schema::Schema};

// BUG: Missing OpenRecStatus column
// BUG: Amounts don't seem to add. Just use x2 payments and x1 receipt not random....

pub mod prelude {
    // Snaphot of ISO currency codes.
    pub const CURRENCIES: [&str; 162] = ["AED", "AFN", "ALL", "AMD", "ANG", "AOA", "ARS", "AUD", "AWG", "AZN", "BAM", "BBD", "BDT", "BGN", "BHD", "BIF", "BMD", "BND", "BOB", "BRL", "BSD", "BTN", "BWP", "BYN", "BZD", "CAD", "CDF", "CHF", "CLP", "CNY", "COP", "CRC", "CUC", "CUP", "CVE", "CZK", "DJF", "DKK", "DOP", "DZD", "EGP", "ERN", "ETB", "EUR", "FJD", "FKP", "GBP", "GEL", "GGP", "GHS", "GIP", "GMD", "GNF", "GTQ", "GYD", "HKD", "HNL", "HRK", "HTG", "HUF", "IDR", "ILS", "IMP", "INR", "IQD", "IRR", "ISK", "JEP", "JMD", "JOD", "JPY", "KES", "KGS", "KHR", "KMF", "KPW", "KRW", "KWD", "KYD", "KZT", "LAK", "LBP", "LKR", "LRD", "LSL", "LYD", "MAD", "MDL", "MGA", "MKD", "MMK", "MNT", "MOP", "MRU", "MUR", "MVR", "MWK", "MXN", "MYR", "MZN", "NAD", "NGN", "NIO", "NOK", "NPR", "NZD", "OMR", "PAB", "PEN", "PGK", "PHP", "PKR", "PLN", "PYG", "QAR", "RON", "RSD", "RUB", "RWF", "SAR", "SBD", "SCR", "SDG", "SEK", "SGD", "SHP", "SLL", "SOS", "SPL", "SRD", "STN", "SVC", "SYP", "SZL", "THB", "TJS", "TMT", "TND", "TOP", "TRY", "TTD", "TVD", "TWD", "TZS", "UAH", "UGX", "USD", "UYU", "UZS", "VEF", "VND", "VUV", "WST", "XAF", "XCD", "XDR", "XOF", "XPF", "YER", "ZAR", "ZMW", "ZWD"];
    pub const RANDOM_ALPHANUMERIC: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    pub const RANDOM_ALPHA: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    pub const RANDOM_NUMERIC: &str = "0123456789";
    pub const RANDOM_SEPARATORS: &str = "-/_ ";

    // Always-present, column names.
    pub const AMOUNT: &str = "Amount";
    pub const CURRENCY: &str = "Currency";
    pub const FX_RATE: &str = "FXRate";
    pub const INVOICE_REF: &str = "InvoiceRef";
    pub const PAYMENT_DATE: &str = "PaymentDate";
    pub const PAYMENT_REF: &str = "PaymentRef";
    pub const RECEIPT_DATE: &str = "ReceiptDate";
    pub const RECEIPT_REF: &str = "ReceiptRef";
    pub const RECORD_TYPE: &str = "RecordType";
    pub const REFERENCE: &str = "Reference";
    pub const SETTLEMENT_DATE: &str = "SettlementDate";
    pub const TOTAL_AMOUNT: &str = "TotalAmount";
    pub const TRADE_DATE: &str = "TradeDate";
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

///
/// A utility to generate some related CSV data.
///
pub fn generate(options: Options) -> Result<(), csv::Error> {

    let start = Instant::now();

    let rnd_seed = options.rnd_seed.unwrap_or(1234567890u64);
    let mut rng = StdRng::seed_from_u64(rnd_seed); // TODO: Mung next 3 lines into Options somehow.
    let inv_schema = column_schema(&options.inv_schema, &options.inv_columns, &mut rng);
    let pay_schema = column_schema(&options.pay_schema, &options.pay_columns, &mut rng);
    let rec_schema = column_schema(&options.rec_schema, &options.rec_columns, &mut rng);

    // Turn the ID,ST,DT,DE type strings into real schemas with some randomness to field lengths.
    let inv_schema = Schema::new(&inv_schema, &mut rng, &mut fixed_inv_columns());
    let pay_schema = Schema::new(&pay_schema, &mut rng, &mut fixed_pay_columns());
    let rec_schema = Schema::new(&rec_schema, &mut rng, &mut fixed_rec_columns());

    let prefix = Utc::now().format("%Y%m%d_%H%M%S%3f_").to_string();
    let inv_path = format!("./tmp/{}invoices.csv", prefix);
    let pay_path = format!("./tmp/{}payments.csv", prefix);
    let rec_path = format!("./tmp/{}receipts.csv", prefix);

    let inv_path = std::path::Path::new(&inv_path);
    let pay_path = std::path::Path::new(&pay_path);
    let rec_path = std::path::Path::new(&rec_path);

    // Create parent dirs if required.
    let parent = inv_path.parent().unwrap();
    std::fs::create_dir_all(parent).unwrap();

    // Output the column headers to both files.
    let mut inv_wtr = csv::WriterBuilder::new().quote_style(QuoteStyle::Always).from_path(inv_path)?;
    inv_wtr.write_record(inv_schema.header_vec())?;
    inv_wtr.write_record(inv_schema.schema_vec())?;

    let mut pay_wtr = csv::WriterBuilder::new().quote_style(QuoteStyle::Always).from_path(pay_path)?;
    pay_wtr.write_record(pay_schema.header_vec())?;
    pay_wtr.write_record(pay_schema.schema_vec())?;

    let mut rec_wtr = csv::WriterBuilder::new().quote_style(QuoteStyle::Always).from_path(rec_path)?;
    rec_wtr.write_record(rec_schema.header_vec())?;
    rec_wtr.write_record(rec_schema.schema_vec())?;

    // Initialise some counters.
    let (mut invoices, mut receipts, mut payments) = (0, 0, 0);

    // Generate some random CSV rows.
    for _row in 1..=options.rows.unwrap_or(10) {
        // Generate number of records which should match into a group.
        let group = Group::new(&inv_schema, &pay_schema, &rec_schema, &mut rng);

        // Write the group to the approriate file.
        inv_wtr.write_record(group.invoice())?;
        invoices +=1;

        for payment in group.payments() {
            // TODO: Split payments into 2 files.
            pay_wtr.write_record(payment)?;
            payments += 1
        }

        for receipt in group.receipts() {
            rec_wtr.write_record(receipt)?;
            receipts += 1
        }
    }

    println!("Generated data in {dur} using seed {seed}\n  {inv} invoices exported to {inv_p}\n  {pay} payments exported to {pay_p}\n  {rec} receipts exported to {rec_p}",
        inv = invoices.to_formatted_string(&Locale::en),
        pay = payments.to_formatted_string(&Locale::en),
        rec = receipts.to_formatted_string(&Locale::en),
        inv_p = inv_path.canonicalize().unwrap().into_os_string().into_string().unwrap(),
        pay_p = pay_path.canonicalize().unwrap().into_os_string().into_string().unwrap(),
        rec_p = rec_path.canonicalize().unwrap().into_os_string().into_string().unwrap(),
        dur = format_duration(start.elapsed()),
        seed = rnd_seed
    );

    // Generate a 1-to-n pair of output files (invoice and receiptes).
    inv_wtr.flush()?; // TODO: Perf issue flushing?
    pay_wtr.flush()?;
    rec_wtr.flush()?;
    Ok(())
}

///
/// Add some fixed columns which are always present regardless of other random junk.
///
fn fixed_inv_columns() -> Vec<Column> {
    vec!(
        Column::new(DataType::STRING, RECORD_TYPE.into(), ColumnMeta::default()),
        Column::new(DataType::STRING, REFERENCE.into(), ColumnMeta::new_reference(vec!((3, SegmentType::ALPHA), (5, SegmentType::NUMERIC)))),
        Column::new(DataType::STRING, INVOICE_REF.into(), ColumnMeta::new_reference(vec!((3, SegmentType::ALPHA), (5, SegmentType::NUMERIC)))),
        Column::new(DataType::DATETIME, TRADE_DATE.into(), ColumnMeta::default()),
        Column::new(DataType::DATETIME, SETTLEMENT_DATE.into(), ColumnMeta::default()),
        Column::new(DataType::DECIMAL, TOTAL_AMOUNT.into(), ColumnMeta::new_decimal(12, 6)),
        Column::new(DataType::STRING, CURRENCY.into(), ColumnMeta::new_currency(Some("GBP".into()))),
        Column::new(DataType::DECIMAL, FX_RATE.into(), ColumnMeta::new_decimal(12, 6)),
    )
}

///
/// Add some fixed columns which are always present regardless of other random junk.
///
fn fixed_pay_columns() -> Vec<Column> {
    vec!(
        Column::new(DataType::STRING, RECORD_TYPE.into(), ColumnMeta::default()),
        Column::new(DataType::STRING, REFERENCE.into(), ColumnMeta::new_reference(vec!((3, SegmentType::ALPHA), (5, SegmentType::NUMERIC)))),
        Column::new(DataType::STRING, PAYMENT_REF.into(), ColumnMeta::new_reference(vec!((3, SegmentType::ALPHA), (5, SegmentType::NUMERIC)))),
        Column::new(DataType::DATETIME, PAYMENT_DATE.into(), ColumnMeta::default()),
        Column::new(DataType::DECIMAL, AMOUNT.into(), ColumnMeta::new_decimal(12, 6)),
        Column::new(DataType::STRING, CURRENCY.into(), ColumnMeta::new_currency(Some("GBP".into()))),
        Column::new(DataType::DECIMAL, FX_RATE.into(), ColumnMeta::new_decimal(12, 6)),
    )
}

///
/// Add some fixed columns which are always present regardless of other random junk.
///
fn fixed_rec_columns() -> Vec<Column> {
    vec!(
        Column::new(DataType::STRING, RECORD_TYPE.into(), ColumnMeta::default()),
        Column::new(DataType::STRING, REFERENCE.into(), ColumnMeta::new_reference(vec!((3, SegmentType::ALPHA), (5, SegmentType::NUMERIC)))),
        Column::new(DataType::STRING, RECEIPT_REF.into(), ColumnMeta::new_reference(vec!((3, SegmentType::ALPHA), (5, SegmentType::NUMERIC)))),
        Column::new(DataType::DATETIME, RECEIPT_DATE.into(), ColumnMeta::default()),
        Column::new(DataType::DECIMAL, AMOUNT.into(), ColumnMeta::new_decimal(12, 6)),
        Column::new(DataType::STRING, PAYMENT_REF.into(), ColumnMeta::new_reference(vec!((3, SegmentType::ALPHA), (5, SegmentType::NUMERIC)))),
        Column::new(DataType::STRING, CURRENCY.into(), ColumnMeta::new_currency(Some("GBP".into()))),
        Column::new(DataType::DECIMAL, FX_RATE.into(), ColumnMeta::new_decimal(12, 6)),
    )
}

///
/// Use the schema specified or generate a random one.
///
fn column_schema(schema: &Option<String>, columns: &Option<usize>, rng: &mut StdRng) -> String {
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
        schema += match rng.gen_range(1..=6) {
            1  => DataType::BOOLEAN.into(),
            2  => DataType::DATETIME.into(),
            3  => DataType::DECIMAL.into(),
            4  => DataType::INTEGER.into(),
            5  => DataType::STRING.into(),
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
pub fn generate_ref(rng: &mut StdRng, meta: &SegmentMeta) -> String {
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
pub fn generate_row(schema: &Schema, foreign_key: &str, record_type: &str, rng: &mut StdRng) -> Vec<String> {

    schema.columns()
        .iter()
        .map(|col| {
            match col.header() {
                RECORD_TYPE => record_type.to_string(),
                REFERENCE   => foreign_key.to_string(),
                _ => match col.data_type() {
                    DataType::UNKNOWN  => panic!("Unknown data type encountered for column {}", col.header()),
                    DataType::BOOLEAN  => generate_boolean(rng),
                    DataType::DATETIME => generate_datetime(rng),
                    DataType::DECIMAL  => generate_decimal(rng, col.meta()),
                    DataType::INTEGER  => generate_integer(rng, col.meta()),
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
/// Generate a random integer up to the precision length in the metadata.
///
fn generate_integer(rng: &mut StdRng, meta: &ColumnMeta) -> String {
    let precision = meta.integer().as_ref().unwrap().precision() as usize;
    rand_chars(precision, RANDOM_NUMERIC, rng)
}

///
/// Generate a random decimal number using the column's precision and scale to determine how many digits.
///
pub fn generate_decimal(rng: &mut StdRng, meta: &ColumnMeta) -> String {
    let meta = meta.decimal().as_ref().unwrap();
    let precision = meta.precision() as usize;
    let scale = meta.scale() as u32;
    let rnd_nums = format!("{}000", rand_chars(precision - 3, /* Dont use all of the scale otherwise not all match groups will not net to zero due to max scale precision rounding issues */
        RANDOM_NUMERIC, rng));
    let decimal = Decimal::new(
        rnd_nums.parse::<i64>().expect(&format!("Bad number {}", rnd_nums)),
        scale);

    format!("{}", decimal)
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
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
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

