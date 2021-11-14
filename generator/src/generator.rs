use uuid::Uuid;
use csv::QuoteStyle;
use clap::{Arg, App};
use rust_decimal::prelude::*;
use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use rand::{Rng, prelude::{ThreadRng, SliceRandom}};
use crate::{column::Column, data_type::DataType, schema::Schema};


#[derive(Copy, Clone, Debug)]
enum SegmentType {
    ALPHA,
	NUMERIC,
	ALPHANUMERIC,
}

///
/// Additional details the generator needs, per column to generate psuedo-realistic data.
///
#[derive(Debug, Default)]
struct ColumnMeta {
    segments: Option<SegmentMeta>,  // String reference fields have a defined format.
    currency: Option<CurrencyMeta>, // If a string field isn't a reference it may be a currency.
    integer: Option<IntegerMeta>,   // If the column is an integer, ensure all values use a consistant length.
    long: Option<LongMeta>,         // If the column is a long, ensure all values use a consistant length.
}

///
/// Reference strings are made out of Segments. eg. xxx-123-abc.
///
#[derive(Debug)]
struct SegmentMeta {
    segment_lens: Vec<u8>,           // The length of each segments in a string field. eg, [2, 6]. 'ab-EFG123'
    segment_types: Vec<SegmentType>, // The type of each Segment in the reference.
    separators: Vec<char>            // The character between the segments. eg, 'abc/A123-B' would be ['/', '-']
}

///
/// Currency strings either use the same code for the column or a random code-per-row.
///
#[derive(Debug)]
struct CurrencyMeta {
    code: Option<String>, // All values will use this currency, otherwise a weighted random currency will be used.
}

#[derive(Debug)]
struct IntegerMeta {
    precision: u8 // The number of digits in each row.
}

#[derive(Debug)]
struct LongMeta {
    precision: u8 // The number of digits in each row.
}

// Snaphot of ISO currency codes.
const CURRENCIES: [&str; 162] = ["AED", "AFN", "ALL", "AMD", "ANG", "AOA", "ARS", "AUD", "AWG", "AZN", "BAM", "BBD", "BDT", "BGN", "BHD", "BIF", "BMD", "BND", "BOB", "BRL", "BSD", "BTN", "BWP", "BYN", "BZD", "CAD", "CDF", "CHF", "CLP", "CNY", "COP", "CRC", "CUC", "CUP", "CVE", "CZK", "DJF", "DKK", "DOP", "DZD", "EGP", "ERN", "ETB", "EUR", "FJD", "FKP", "GBP", "GEL", "GGP", "GHS", "GIP", "GMD", "GNF", "GTQ", "GYD", "HKD", "HNL", "HRK", "HTG", "HUF", "IDR", "ILS", "IMP", "INR", "IQD", "IRR", "ISK", "JEP", "JMD", "JOD", "JPY", "KES", "KGS", "KHR", "KMF", "KPW", "KRW", "KWD", "KYD", "KZT", "LAK", "LBP", "LKR", "LRD", "LSL", "LYD", "MAD", "MDL", "MGA", "MKD", "MMK", "MNT", "MOP", "MRU", "MUR", "MVR", "MWK", "MXN", "MYR", "MZN", "NAD", "NGN", "NIO", "NOK", "NPR", "NZD", "OMR", "PAB", "PEN", "PGK", "PHP", "PKR", "PLN", "PYG", "QAR", "RON", "RSD", "RUB", "RWF", "SAR", "SBD", "SCR", "SDG", "SEK", "SGD", "SHP", "SLL", "SOS", "SPL", "SRD", "STN", "SVC", "SYP", "SZL", "THB", "TJS", "TMT", "TND", "TOP", "TRY", "TTD", "TVD", "TWD", "TZS", "UAH", "UGX", "USD", "UYU", "UZS", "VEF", "VND", "VUV", "WST", "XAF", "XCD", "XDR", "XOF", "XPF", "YER", "ZAR", "ZMW", "ZWD"];
const RANDOM_ALPHANUMERIC: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const RANDOM_ALPHA: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const RANDOM_NUMERIC: &str = "0123456789";
const RANDOM_SEPARATORS: &str = "-/_ ";

// TODO: Single rng passed around.
// TODO: Use rnd number for seed, and print in output - and allow replay.
// TODO: Look at 1-n record generation.

///
/// A utility to generate some related CSV data.
///
pub fn generate() -> Result<(), csv::Error> {
    // Parse the command-line args.
    let matches = App::new("CSV Generator")
        .version("1.0")
        .about("Utility to generate psuedo-realistic random CSV transaction files that reference each other.")
        .arg(Arg::with_name("SCHEMA")
            .help("A CSV string (no spaces) of data types for the generated rows, eg. ST,ST,ID,DT,DE")
            .required(true)
            .index(1))
        .get_matches();


    // TODO: Allow independant schemas to be specified for each file type.
    // TODO: Allow random schemas, with num columns as an option.

    // Turn the ID,ST,DT,DE type string into a real schema with some randomness to field lengths.
    let mut rng = rand::thread_rng();
    let schema: Schema = matches.value_of("SCHEMA").unwrap().into();
    let meta = generate_meta(&schema, &mut rng);

    // Output the column headers to both files.
    let mut inv_wtr = csv::WriterBuilder::new().quote_style(QuoteStyle::NonNumeric).from_path("./tmp/invoice.csv")?;
    inv_wtr.write_record(schema.header_vec())?;

    let mut rec_wtr = csv::WriterBuilder::new().quote_style(QuoteStyle::NonNumeric).from_path("./tmp/receipts.csv")?;
    rec_wtr.write_record(schema.header_vec())?;

    // Generate some random CSV rows.
    for _row in 1..=1000 {
        // Generate a common reference that links this row in both files.
        let foreign_key = format!("INV-{}", generate_ref(&mut rng, &SegmentMeta::default()));

        inv_wtr.write_record(
            generate_row(&schema, &meta, &foreign_key, "INV", &mut rng)
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>())?;

                rec_wtr.write_record(
            generate_row(&schema, &meta, &foreign_key, "REC", &mut rng)
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>())?;
    }

    // TODO: Output the final schemas for the files (i.e. include the record type and reference columns).

    // Generate a 1-to-n pair of output files (invoice and receiptes).
    inv_wtr.flush()?;
    rec_wtr.flush()?;
    Ok(())
}

///
/// Generate any metadata for columns the generator may need to ensure value format consistency in the random output.
///
fn generate_meta(schema: &Schema, rng: &mut ThreadRng) -> Vec<ColumnMeta> {
    schema.columns()
        .iter()
        .map(|col| ColumnMeta::new(col, rng))
        .collect::<Vec<ColumnMeta>>()
}

///
/// Generate a random reference string used to link records in different files.
///
fn generate_ref(rng: &mut ThreadRng, meta: &SegmentMeta) -> String {
    let mut reference = String::new();

    for segment in 0..meta.segment_types.len() {
        for _idx in 0..meta.segment_lens[segment] {
            let slice = match meta.segment_types[segment] {
                SegmentType::ALPHA        => RANDOM_ALPHA,
                SegmentType::NUMERIC      => RANDOM_NUMERIC,
                SegmentType::ALPHANUMERIC => RANDOM_ALPHANUMERIC,
            };

            reference += &rand_char(slice, rng).to_string();
        }

        if segment < (meta.segment_types.len()-1) {
            reference += &meta.separators[segment].to_string();
        }
    }

    reference
}

///
/// Generate a random row of data using the Schema and meta data provided.
///
fn generate_row(schema: &Schema, meta: &[ColumnMeta], foreign_key: &str, record_type: &str, rng: &mut ThreadRng)
    -> Vec<String> {

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
                    DataType::DECIMAL  => generate_decimal(rng, col),
                    DataType::INTEGER  => generate_integer(rng, &meta[idx]),
                    DataType::LONG     => generate_long(rng, &meta[idx]),
                    DataType::SHORT    => generate_short(rng),
                    DataType::STRING   => generate_string(rng, &meta[idx]),
                    DataType::UUID     => generate_uuid(),
                }
            }
        })
        .collect()
}

///
/// Generate a random reference, currency or gibberish depending on the column generation metadata.
///
fn generate_string(rng: &mut ThreadRng, meta: &ColumnMeta) -> String {

    if let Some(ref_meta) = &meta.segments {
        return generate_ref(rng, ref_meta)
    }

    if let Some(cur_meta) = &meta.currency {
        match &cur_meta.code {
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
fn generate_short(rng: &mut ThreadRng) -> String {
    format!("{}", rng.gen_range(-32767..32767))
}

///
/// Generate a random long up to the precision length in the metadata.
///
fn generate_long(rng: &mut ThreadRng, meta: &ColumnMeta) -> String {
    let precision = meta.long.as_ref().unwrap().precision as usize;
    rand_chars(precision, RANDOM_NUMERIC, rng)
}

///
/// Generate a random integer up to the precision length in the metadata.
///
fn generate_integer(rng: &mut ThreadRng, meta: &ColumnMeta) -> String {
    let precision = meta.integer.as_ref().unwrap().precision as usize;
    rand_chars(precision, RANDOM_NUMERIC, rng)
}

///
/// Generate a random decimal number using the column's precision and scale to determine how many digits.
///
fn generate_decimal(rng: &mut ThreadRng, col: &Column) -> String {
    let precision = col.precision().unwrap() as usize;
    let scale = col.scale().unwrap() as u32;
    let rnd_nums = rand_chars(precision, RANDOM_NUMERIC, rng);
    let decimal = Decimal::new(
        rnd_nums.parse::<i64>().expect(&format!("Bad number {}", rnd_nums)),
        scale);

    format!("{}", decimal)
}

///
/// Generate a random date from 1 year ago to 1 years time (loosly).
///
fn generate_date(rng: &mut ThreadRng) -> String {
    let y = Utc::now().year() - 2 + rng.gen_range(0..3);  // Generate a year within 1 year of the current.
    let m = rng.gen_range(1..13);                         // Generate a random month.
    let d = rng.gen_range(1..days_in_month(y, m) as u32); // Generate a random day from this month.
    let dt = Utc.ymd(y, m, d).and_hms(0, 0, 0);
    format!("{}", dt.timestamp_millis())
}

///
/// Generate a random date from 1 year ago to 1 years time (loosly).
///
fn generate_datetime(rng: &mut ThreadRng) -> String {
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
fn generate_char(rng: &mut ThreadRng) -> String {
    rand_char(RANDOM_ALPHANUMERIC, rng).to_string()
}

///
/// Generate a positive random short.
///
fn generate_byte(rng: &mut ThreadRng) -> String {
    format!("{}", rng.gen_range(0..128))
}

///
/// Generate a random boolean
///
fn generate_boolean(rng: &mut ThreadRng) -> String {
    format!("{}", rng.gen_bool(1. / 2.))
}

///
/// Select a random currency code.
///
fn rand_currency() -> String {
    CURRENCIES.choose(&mut rand::thread_rng()).unwrap().to_string()
}

///
/// Select a random char from the &str slice and clone it.
///
fn rand_char(slice: &str, rng: &mut ThreadRng) -> char {
    slice.chars()
        .collect::<Vec<char>>()
        .choose(rng)
        .unwrap()
        .clone()
}

///
/// Select a number of random chars from the &str slice and clone it.
///
fn rand_chars(amount: usize, slice: &str, rng: &mut ThreadRng) -> String {
    slice.chars()
        .collect::<Vec<char>>()
        .choose_multiple(rng, amount)
        .cloned()
        .collect()
}


impl ColumnMeta {
    fn new(col: &Column, rng: &mut ThreadRng) -> Self {
        match col.data_type() {
            DataType::STRING => {
                match rng.gen_range(1..=100) {
                    1..=60 => ColumnMeta { segments: Some(SegmentMeta::new(None)), ..Default::default() }, // 60% of string columns are a reference code.
                    _      => ColumnMeta { currency: Some(CurrencyMeta::new()), ..Default::default() },    // 40% of string columns are an ISO currency code.
                }
            },
            DataType::INTEGER => ColumnMeta { integer: Some(IntegerMeta::new()), ..Default::default() },
            DataType::LONG => ColumnMeta { long: Some(LongMeta::new()), ..Default::default() },
            _ => ColumnMeta::default(),
        }
    }
}


impl SegmentMeta {
    fn new(segments: Option<u8>) -> Self {
        let mut rng = rand::thread_rng();

        // Generate 1-4 segments.
        let segments = match segments {
            Some(count) => count,
            None => rng.gen_range(1..=4),
        };

        // Each segment is 1 to 6 characters long.
        let mut segment_lens: Vec<u8> = vec!();
        for _idx in 1..=segments {
            segment_lens.push(rng.gen_range(1..=6));
        }

        // Generate segments-1 separators.
        let mut separators = vec!();
        for _idx in 1..segments {
            separators.push(rand_char(RANDOM_SEPARATORS, &mut rng));
        }

        // Generate a type for each segment.
        let mut segment_types = vec!();
        for _idx in 1..=segments {
            segment_types.push(match rng.gen_range(1..=3) {
                1 => SegmentType::NUMERIC,
                2 => SegmentType::ALPHA,
                _ => SegmentType::ALPHANUMERIC,
            })
        }

        Self { segment_lens, segment_types, separators }
    }
}


impl Default for SegmentMeta {
    fn default() -> Self {
        Self {
            segment_lens: vec!(3, 3),
            segment_types: vec!(SegmentType::ALPHA, SegmentType::NUMERIC),
            separators: vec!('-')
        }
    }
}


impl CurrencyMeta {
    fn new() -> Self {
        let mut rng = rand::thread_rng();
        let currency = match rng.gen_range(1..=100) {
            1..=70 => Some(rand_currency()), // All values in this column will use this randomly selected currency.
            _      => None,                        // Each value in this column will be a random currency.
        };

        Self { code: currency }
    }
}


impl IntegerMeta {
    fn new() -> Self {
        let mut rng = rand::thread_rng();
        Self { precision: rng.gen_range(1..=5) }
    }
}

impl LongMeta {
    fn new() -> Self {
        let mut rng = rand::thread_rng();
        Self { precision: rng.gen_range(1..=10) }
    }
}


///
/// Parse a comma-separated string of data-type short-codes and turn into a schema with generated column header names
/// and randomly sized numbers.
///
impl From<&str> for Schema {
    fn from(schema: &str) -> Self {
        let mut rng = rand::thread_rng();
        let mut columns = vec!();
        let mut idx = 1u16;

        // Add a column for the record-type.
        columns.push(Column::new(DataType::STRING, "RecordType".into(), None, None));

        // Add a column for the foreign-key.
        columns.push(Column::new(DataType::STRING, "Reference".into(), None, None));

        // Parse all the other columns.
        for dt in schema.split(",") {
            let parsed: DataType = dt.into();

            // Generate a random(ish), column definition.
            let (precision, scale) = match parsed {
                DataType::UNKNOWN => panic!("Unknown data type '{}'", dt),

                DataType::BOOLEAN  |
                DataType::CHAR     |
                DataType::DATE     |
                DataType::DATETIME |
                DataType::STRING   |
                DataType::BYTE     |
                DataType::INTEGER  |
                DataType::LONG     |
                DataType::SHORT    |
                DataType::UUID => (None, None),

                DataType::DECIMAL => {
                    let scale = 2 + rng.gen_range(0..=5);
                    (Some(scale + 1 + rng.gen_range(0..=5)), Some(scale))
                },
            };

            columns.push(Column::new(
                parsed,
                format!("Column_{}", idx),
                precision,
                scale));
            idx += 1;
        }

        Self::new(columns)
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ok() {
        let schema: Schema = "ID,BO,BY,CH,DA,DT,DE,IN,LO,SH,ST,ID,BO".into();

        for idx in 0..13 {
            assert_eq!(schema.columns()[idx].header(), format!("Column_{}", idx+1));
        }

        assert_eq!(schema.columns()[0].data_type(), DataType::UUID);
        assert_eq!(schema.columns()[1].data_type(), DataType::BOOLEAN);
        assert_eq!(schema.columns()[2].data_type(), DataType::BYTE);
        assert_eq!(schema.columns()[3].data_type(), DataType::CHAR);
        assert_eq!(schema.columns()[4].data_type(), DataType::DATE);
        assert_eq!(schema.columns()[5].data_type(), DataType::DATETIME);
        assert_eq!(schema.columns()[6].data_type(), DataType::DECIMAL);
        assert_eq!(schema.columns()[7].data_type(), DataType::INTEGER);
        assert_eq!(schema.columns()[8].data_type(), DataType::LONG);
        assert_eq!(schema.columns()[9].data_type(), DataType::SHORT);
        assert_eq!(schema.columns()[10].data_type(), DataType::STRING);
        assert_eq!(schema.columns()[11].data_type(), DataType::UUID);
        assert_eq!(schema.columns()[12].data_type(), DataType::BOOLEAN);

    }

    #[test]
    #[should_panic(expected = "Unknown data type 'x'")]
    fn test_parse_err() {
        let _schema: Schema = "ID,BO,BY,x".into();
    }
}