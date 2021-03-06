use std::cmp::{max, min};
use rust_decimal_macros::dec;
use rand::{Rng, prelude::StdRng};
use chrono::{DateTime, Utc, SecondsFormat};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use crate::{column::{ColumnMeta, SegmentMeta}, generator::{self, prelude::*}, schema::Schema};

type Record = Vec<String>;

///
/// Represents a group of invoice, payments and receipts that should match together.
///
/// There will typically be 1-n cardinality between invoices and payments (respectively).
/// There will typically be 1-n cardinality between receipts and payments (respectively).
/// There will typically be no more than 3 receipts per invoice.
///
/// All records in the group will be given a 'GRP-' reference code to help debugging - it is
/// not intended to be used for matching.
///
#[derive(Debug)]
pub struct Group {
    invoice: Record,
    payments: Vec<Record>,
    receipts: Vec<Record>
}

impl Group {
    pub fn new(inv_schema: &Schema, pay_schema: &Schema, rec_schema: &Schema, rng: &mut StdRng) -> Self {

        let foreign_key = format!("GRP-{}", generator::generate_ref(rng, &SegmentMeta::default()));
        let mut invoice = generator::generate_row(&inv_schema, &foreign_key, "INV", rng);

        let settlement_date = get_date(SETTLEMENT_DATE, &invoice, inv_schema);
        let fx_rate = generator::generate_decimal(rng, &ColumnMeta::new_decimal(12, 6)).parse().unwrap();
        set_decimal(FX_RATE, fx_rate, &mut invoice, inv_schema);

        let payments = generate_payments(&invoice, inv_schema, pay_schema, &foreign_key, fx_rate, settlement_date, rng);
        let receipts = generate_receipts(&payments, rec_schema, pay_schema, &foreign_key, fx_rate, settlement_date, rng);

        Self { invoice, payments, receipts }
    }

    pub fn invoice(&self) -> &[String] {
        &self.invoice
    }

    pub fn payments(&self) -> &[Vec<String>] {
        &self.payments
    }

    pub fn receipts(&self) -> &[Vec<String>] {
        &self.receipts
    }
}

///
/// Generate 1 to 6 payments for the invoice. Allocate the invoice's total amount amongst the payments.
///
fn generate_payments(
    invoice: &Record,
    inv_schema: &Schema,
    pay_schema: &Schema,
    foreign_key: &str,
    _fx_rate: Decimal,
    settlement_date: DateTime<Utc>,
    rng: &mut StdRng) -> Vec<Record> {

    let mut payments = (1..=rng.gen_range(1..=6))
        .map(|_idx| generator::generate_row(&pay_schema, &foreign_key, "PAY", rng))
        .collect::<Vec<Record>>();

    // Get the total invoice amount - we'll allocate it amongst the payments.
    let tot_amount = dec!(2.0) * get_decimal(TOTAL_AMOUNT, &invoice, inv_schema);
    allocate_decimal(AMOUNT, tot_amount, &mut payments, pay_schema, rng);

    payments.iter_mut().for_each(|payment| {
        set_date(PAYMENT_DATE, settlement_date, payment, pay_schema);

        // Set the FXRate to the same value for all payments. We'll double the amount and use .5 FXRate.
        set_decimal(FX_RATE, dec!(0.5), payment, pay_schema);
    });

    payments
}

///
/// Ensure each receipt has at least one payment and a payment has 1 receipt.
///
fn generate_receipts(
    payments: &Vec<Record>,
    rec_schema: &Schema,
    pay_schema: &Schema,
    foreign_key: &str,
    _fx_rate: Decimal,
    settlement_date: DateTime<Utc>,
    rng: &mut StdRng) -> Vec<Record> {

    // Generate some template receipts (1:2 ratio with payments).
    let mut receipts = vec!();
    let receipt_count = rng.gen_range(1..=(max(1, (payments.len() as f64 / 2.) as usize)));

    let master_receipts = (1..=receipt_count)
        .map(|_idx| generator::generate_row(&rec_schema, &foreign_key, "REC", rng))
        .collect::<Vec<Vec<String>>>();

    // Now 'assign' a payment to one of the templated receipts.
    for (idx, payment) in payments.iter().enumerate() {
        // Then start cloning them and just changing the paymentref.
        let mut receipt = master_receipts[idx % master_receipts.len()].clone();

        // Link the payment date and amount to the receipt.
        set_string(PAYMENT_REF, get_string(PAYMENT_REF, payment, pay_schema), &mut receipt, rec_schema);
        set_decimal(AMOUNT, get_decimal(AMOUNT, payment, pay_schema), &mut receipt, rec_schema);
        set_date(RECEIPT_DATE, settlement_date, &mut receipt, rec_schema);

        // Set the FXRate to the same value for all receipts.
        set_decimal(FX_RATE, dec!(0.5), &mut receipt, rec_schema);

        receipts.push(receipt);
    }

    receipts
}

///
/// Locate the column in the schema by positional index.
///
fn column_idx(field: &str, schema: &Schema) -> usize {
    schema.header_vec()
        .iter()
        .enumerate()
        .find(|(_ix,hdr)| **hdr == field)
        .map( |(idx,_hr)| idx)
        .unwrap()
}

///
/// Get the named field from the record as a UTC Date.
///
fn get_date(field: &str, record: &Record, schema: &Schema) -> DateTime<Utc> {
    let raw = &record[column_idx(field, schema)];
    // let timestamp = raw.parse::<i64>().unwrap();
    // DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp / 1000 /* ts is microsec */, 0), Utc)
    raw.parse::<DateTime<Utc>>().unwrap()
}

///
/// Set the field to the String value specified.
///
fn set_string(field: &str, value: String, record: &mut Record, schema: &Schema) {
    record[column_idx(field, schema)] = value;
}

///
/// Get the named field from the record as a String type.
///
fn get_string(field: &str, record: &Record, schema: &Schema) -> String {
    record[column_idx(field, schema)].clone()
}

///
/// Set the field to the Date<Utc> value specified.
///
fn set_date(field: &str, date: DateTime<Utc>, record: &mut Record, schema: &Schema) {
    record[column_idx(field, schema)] = date.to_rfc3339_opts(SecondsFormat::Millis, true)
}

///
/// Get the named field from the record as a Decimal type.
///
fn get_decimal(field: &str, record: &Record, schema: &Schema) -> Decimal {
    let raw = &record[column_idx(field, schema)];
    raw.parse().unwrap()
}

///
/// Set the field to the Decimal value specified.
///
fn set_decimal(field: &str, amount: Decimal, record: &mut Record, schema: &Schema) {
    record[column_idx(field, schema)] = amount.to_string();
}

///
/// Allocate the total amount amongst the records specified in the field specified.
///
fn allocate_decimal(field: &str, tot_amount: Decimal, records: &mut Vec<Record>, schema: &Schema, rng: &mut StdRng) {
    let mut remaining = tot_amount;
    let mut allocation = tot_amount / Decimal::from(records.len());
    allocation.rescale(8); // Curb huge scales.

    records.iter_mut().for_each(|record| {
        // Allow a payment amount to vary by up to -/+50% of an uniform allocation.
        let half = allocation.to_f64().unwrap() / 2.;
        let jitter: Decimal = format!("{}", rng.gen_range(-half..half)).parse().unwrap();
        let jitter = max(Decimal::ZERO, allocation + jitter); // No negative payments!
        let jitter = min(jitter, remaining);                  // No over-payments.

        // Note: The above may create some zero payments with an amount of zero. Not too fussed about these.
        set_decimal(field, jitter, record, schema);
        remaining -= jitter;
    });

    // Allocate any remainder to the first record.
    if remaining > Decimal::ZERO {
        let amount = get_decimal(field, &records[0], schema);
        set_decimal(field, amount + remaining, &mut records[0], schema);
    }

    // Sanity check.
    let mut allocated = Decimal::new(0, 0);
    records.iter().for_each(|record| allocated += get_decimal(field, record, schema));
    if allocated != tot_amount {
        println!("allocation {} != total_amount {}", allocated, tot_amount);
    }
}