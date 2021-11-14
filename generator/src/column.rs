use rand::{Rng, prelude::StdRng};
use crate::{data_type::DataType, generator::{self, prelude::*}};

#[derive(Debug)]
pub struct Column {
    data_type: DataType,
    header: String,
    meta: ColumnMeta
}

impl Column {
    pub fn new(data_type: DataType, header: String, meta: ColumnMeta) -> Self {
        Column {
            data_type,
            header,
            meta
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn header(&self) -> &str {
        &self.header
    }

    pub fn meta(&self) -> &ColumnMeta {
        &self.meta
    }
}


#[derive(Copy, Clone, Debug)]
pub enum SegmentType {
    ALPHA,
	NUMERIC,
	ALPHANUMERIC,
}


///
/// Additional details the generator needs, per column to generate psuedo-realistic data.
///
#[derive(Debug, Default)]
pub struct ColumnMeta {
    segments: Option<SegmentMeta>,  // String reference fields have a defined format.
    currency: Option<CurrencyMeta>, // If a string field isn't a reference it may be a currency.
    integer: Option<IntegerMeta>,   // If the column is an integer, ensure all values use a consistant length.
    long: Option<LongMeta>,         // If the column is a long, ensure all values use a consistant length.
    decimal: Option<DecimalMeta>,   // If the column is a decimal, ensure all values use a consistant length.
}

impl ColumnMeta {
    ///
    /// Generate a random set of column properties based on the data-type.
    ///
    pub fn new(data_type: DataType, rng: &mut StdRng) -> Self {
        match data_type {
            DataType::STRING => {
                match rng.gen_range(1..=100) {
                    1..=60 => ColumnMeta { segments: Some(SegmentMeta::new(None, rng)), ..Default::default() }, // 60% of string columns are a reference code.
                    _      => ColumnMeta { currency: Some(CurrencyMeta::new(rng)), ..Default::default() },      // 40% of string columns are an ISO currency code.
                }
            },
            DataType::INTEGER => ColumnMeta { integer: Some(IntegerMeta::new(rng)), ..Default::default() },
            DataType::LONG    => ColumnMeta { long: Some(LongMeta::new(rng)), ..Default::default() },
            DataType::DECIMAL => ColumnMeta { decimal: Some(DecimalMeta::new(rng)), ..Default::default() },
            _ => ColumnMeta::default(),
        }
    }

    pub fn segments(&self) -> &Option<SegmentMeta> {
        &self.segments
    }

    pub fn currency(&self) -> &Option<CurrencyMeta> {
        &self.currency
    }

    pub fn integer(&self) -> &Option<IntegerMeta> {
        &self.integer
    }

    pub fn long(&self) -> &Option<LongMeta> {
        &self.long
    }

    pub fn decimal(&self) -> &Option<DecimalMeta> {
        &self.decimal
    }
}


///
/// Reference strings are made out of Segments. eg. xxx-123-abc.
///
#[derive(Debug)]
pub struct SegmentMeta {
    segment_lens: Vec<u8>,           // The length of each segments in a string field. eg, [2, 6]. 'ab-EFG123'
    segment_types: Vec<SegmentType>, // The type of each Segment in the reference.
    separators: Vec<char>            // The character between the segments. eg, 'abc/A123-B' would be ['/', '-']
}

impl SegmentMeta {
    fn new(segments: Option<u8>, rng: &mut StdRng) -> Self {
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
            separators.push(generator::rand_char(RANDOM_SEPARATORS, rng));
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

    pub fn segment_lens(&self) -> &[u8] {
        &self.segment_lens
    }

    pub fn segment_types(&self) -> &[SegmentType] {
        &self.segment_types
    }

    pub fn separators(&self) -> &[char] {
        &self.separators
    }
}

impl Default for SegmentMeta {
    fn default() -> Self {
        Self {
            segment_lens: vec!(3, 5),
            segment_types: vec!(SegmentType::ALPHA, SegmentType::NUMERIC),
            separators: vec!('-')
        }
    }
}


///
/// Currency strings either use the same code for the column or a random code-per-row.
///
#[derive(Debug)]
pub struct CurrencyMeta {
    code: Option<String>, // All values will use this currency, otherwise a weighted random currency will be used.
}

impl CurrencyMeta {
    fn new(rng: &mut StdRng) -> Self {
        let currency = match rng.gen_range(1..=100) {
            1..=70 => Some(generator::rand_currency()), // All values in this column will use this randomly selected currency.
            _      => None,                  // Each value in this column will be a random currency.
        };

        Self { code: currency }
    }

    pub fn code(&self) -> &Option<String> {
        &self.code
    }
}


#[derive(Debug)]
pub struct IntegerMeta {
    precision: u8 // The number of digits in each row.
}

impl IntegerMeta {
    fn new(rng: &mut StdRng) -> Self {
        Self { precision: rng.gen_range(1..=5) }
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }
}


#[derive(Debug)]
pub struct LongMeta {
    precision: u8 // The number of digits in each row.
}

impl LongMeta {
    fn new(rng: &mut StdRng) -> Self {
        Self { precision: rng.gen_range(6..=10) }
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }
}


#[derive(Debug)]
pub struct DecimalMeta {
    precision: u8, // The number of digits in each row.
    scale: u8,     // The number of digits after dotty.
}

impl DecimalMeta {
    fn new(rng: &mut StdRng) -> Self {
        let scale = 2 + rng.gen_range(0..=5);
        let precision = scale + 1 + rng.gen_range(0..=5);
        Self { precision, scale }
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> u8 {
        self.scale
    }
}