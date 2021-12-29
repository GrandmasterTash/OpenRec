use regex::Regex;
use chrono::DateTime;
use lazy_static::lazy_static;
use core::data_type::DataType;
use crate::error::JetwashError;

///
/// This analysis uses an ordered heirachy to the data-types. Ranging from the 'most specific' to the
/// most general (broadly speaking).
///
///    BOOLEAN
///    DATETIME
///    INTEGER
///    DECIMAL
///    UUID
///    STRING
///
/// i.e. if we deduce every value in a column is either a '1' or '0' we can presume the column is a
/// boolean. However if we find a value 2.... then maybe the column is a datetime? So the list above
/// is the order of types we try to coerce a column into and if we fail, we try the next type in the
/// list, and so on, until we simple fall-back on a string type.
///
const SEQUENCE: [DataType; 6] = [
	DataType::Boolean,
	DataType::Datetime,
	DataType::Integer,
	DataType::Decimal,
	DataType::Uuid,
	DataType::String
];

const BOOLEAN_TRUES: [&'static str; 4] = [ "yes", "true", "1", "y" ];
const BOOLEAN_FALSES: [&'static str; 4] = [ "no", "false", "0", "n" ];

lazy_static! {
	static ref INTEGER_REGEX: Regex = Regex::new(r"^[-+]?[0-9]{1,19}$").expect("invalid integer regex");
    static ref DECIMAL_REGEX: Regex = Regex::new(r"^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$").expect("invalid decimal regex");
	static ref UUID_REGEX: Regex = Regex::new(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$").expect("invalid uuid regex");
}

///
/// Iterate each column and deduce the cell's type - track the data-type being used for each column.
///
/// The data_types passed in are the current best-guesses for the column data-types. These will be refined if required
/// based on the current record passed in.
///
pub fn analyse_types(data_types: &mut [DataType], csv_record: &csv::ByteRecord) -> Result<(), JetwashError> {

	for (col_idx, value) in csv_record.iter().enumerate() {
		// Ensure the value is a valid UTF8.
		let value = std::str::from_utf8(&value)?;

		if !value.is_empty() {
			// for data_type in SEQUENCE {
			for idx in type_position(data_types[col_idx])..SEQUENCE.len() {
				let data_type = SEQUENCE[idx];

				if is_type(value, data_type) {
					if is_more_general(data_type, data_types[col_idx]) {
						data_types[col_idx] = data_type;
					}
					break;
				}
			}
		}
	}

	Ok(())
}

fn type_position(data_type: DataType) -> usize {
	SEQUENCE.iter().position(|dt| *dt == data_type).unwrap_or_default()
}

///
/// STRING for example is more 'general' than DATETIME.
///
fn is_more_general(type_1: DataType, type_2: DataType) -> bool {

	if type_1 == type_2 {
		return false
	}

	if type_2 == DataType::Unknown {
		return true // type_1 will always be a known type.
	}

	type_position(type_1) > type_position(type_2)
}


fn is_type(value: &str, data_type: DataType) -> bool {
	let result = match data_type {
		DataType::Unknown => false, // This won't be called.
		DataType::Boolean => is_boolean(value),
		DataType::Datetime => is_datetime(value),
		DataType::Decimal => is_decimal(value),
		DataType::Integer => is_integer(value),
		DataType::String => true, // Everything can be a string.
		DataType::Uuid => is_uuid(value),
	};

	// match result {
	// 	true => println!("{} is a {:?}", value, data_type),
	// 	false => println!("{} is NOT a {:?}", value, data_type),
	// }

	result
}

fn is_boolean(value: &str) -> bool {
	BOOLEAN_TRUES.contains(&value) || BOOLEAN_FALSES.contains(&value)
}

fn is_decimal(value: &str) -> bool {
	DECIMAL_REGEX.is_match(value)
}

fn is_integer(value: &str) -> bool {
	INTEGER_REGEX.is_match(value)
}

fn is_uuid(value: &str) -> bool {
	UUID_REGEX.is_match(value)
}

///
/// RFC 3339 ISO 8601 UTC only.
///
fn is_datetime(value: &str) -> bool {
	DateTime::parse_from_rfc3339(value).is_ok()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_analyse_all_types() {
		let record = csv::ByteRecord::from(vec!(
			"I'm a string",                           // 0 - String
			"no",                                     // 1 - Boolean
			"2021/12/29T03:39:00Z",                   // 2 - String (wrong seperators)
			"2021-12-29T03:39:00Z",                   // 3 - Datetime
			"2021/12/29",                             // 4 - String (not rfc3339)
			"2014-11-28T21:00:09+09:00",              // 5 - Datetime
			"Fri Nov 28 12:00:09 2014",               // 6 - String (not rfc3339)
			"1234567",                                // 7 - Integer
			"1.234567",                               // 8 - Decimal
			"2cc22618-6859-11ec-9ee6-00155dd152c4",   // 9 - UUID
		));
		let mut data_types = vec![DataType::Unknown; record.len()];

		analyse_types(&mut data_types, &record).unwrap();
		assert_eq!(vec!(
			DataType::String,
			DataType::Boolean,
			DataType::String,
			DataType::Datetime,
			DataType::String,
			DataType::Datetime,
			DataType::String,
			DataType::Integer,
			DataType::Decimal,
			DataType::Uuid), data_types);
	}

	#[test]
	fn test_broadest_type_takes_precedence_order_1() {
		let mut records = vec!(
			csv::ByteRecord::from(vec!( "0", "2021-12-29T03:39:00Z", "1234567", "test" )));
		let mut data_types = vec![DataType::Unknown; records[0].len()];

		analyse_types(&mut data_types, &records[0]).unwrap();
		assert_eq!(vec!(
			DataType::Boolean,
			DataType::Datetime,
			DataType::Integer,
			DataType::String), data_types, "initial types incorrect");

		records.push(
			csv::ByteRecord::from(vec!( "10", "wibble", "123.4567", "2021-12-29T03:39:00Z" )));

		analyse_types(&mut data_types, &records[1]).unwrap();
		assert_eq!(vec!(
			DataType::Integer,
			DataType::String,
			DataType::Decimal,
			DataType::String), data_types, "updated types incorrect");
	}

	#[test]
	fn test_broadest_type_takes_precedence_order_2() {
		let mut records = vec!(
			csv::ByteRecord::from(vec!( "10", "wibble", "123.4567", "2021-12-29T03:39:00Z" )));
		let mut data_types = vec![DataType::Unknown; records[0].len()];

		analyse_types(&mut data_types, &records[0]).unwrap();
		assert_eq!(vec!(
			DataType::Integer,
			DataType::String,
			DataType::Decimal,
			DataType::Datetime), data_types, "initial types incorrect");

		records.push(
			csv::ByteRecord::from(vec!( "0", "2021-12-29T03:39:00Z", "1234567", "test" )));

		analyse_types(&mut data_types, &records[1]).unwrap();
		assert_eq!(vec!(
			DataType::Integer,
			DataType::String,
			DataType::Decimal,
			DataType::String), data_types, "updated types incorrect");
	}

	#[test]
	fn test_blanks_have_no_effect() {
		let records = vec!(
			csv::ByteRecord::from(vec!( "0", "2021-12-29T03:39:00Z", "1234567", "test" )),
			csv::ByteRecord::from(vec!( "", "", "", "" )),
			csv::ByteRecord::from(vec!( "1", "2021-12-29T03:39:00Z", "7654321", "another test" )));
		let mut data_types = vec![DataType::Unknown; records[0].len()];

		analyse_types(&mut data_types, &records[0]).unwrap();
		analyse_types(&mut data_types, &records[1]).unwrap();
		analyse_types(&mut data_types, &records[2]).unwrap();
		assert_eq!(vec!(
			DataType::Boolean,
			DataType::Datetime,
			DataType::Integer,
			DataType::String), data_types);
	}

	#[test]
	fn test_re_analysing_has_no_effect() {
		let record = csv::ByteRecord::from(vec!( "0", "2021-12-29T03:39:00Z", "1234567", "test" ));
		let mut data_types = vec![DataType::Unknown; record.len()];

		analyse_types(&mut data_types, &record).unwrap();
		assert_eq!(vec!(
			DataType::Boolean,
			DataType::Datetime,
			DataType::Integer,
			DataType::String), data_types, "initial types incorrect");

		analyse_types(&mut data_types, &record).unwrap();
		assert_eq!(vec!(
			DataType::Boolean,
			DataType::Datetime,
			DataType::Integer,
			DataType::String), data_types, "updated types incorrect");
	}

	#[test]
	fn test_non_utf8_errors() {
		let mut record = csv::ByteRecord::new();
		record.push_field(b"0");
		record.push_field(b"1234567");
		record.push_field(b"test");
		record.push_field(&vec![0, 159, 146, 150, 255]); // Some Non UTF bytes.

		let mut data_types = vec![DataType::Unknown; record.len()];

		let result = analyse_types(&mut data_types, &record);
		match result.unwrap_err() {
    		JetwashError::Utf8Error(_) => {},
			_ => panic!("Wrong error - expected UTF8 error"),
		}
	}
}