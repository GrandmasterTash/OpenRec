use std::{collections::HashMap, fs};
use crate::{data_type::DataType, error::MatcherError};

#[derive(Debug)]
pub struct Schema {
    headers: Vec<String>,
    type_map: HashMap<String, DataType>
}

impl Schema {
    ///
    /// Build a hashmap of column header to parsed data-types. The data types should be on the first
    /// csv row after the headers.
    ///
    pub fn new(rdr: &mut csv::Reader<fs::File>) -> Result<Self, MatcherError> {
        let mut type_record = csv::StringRecord::new();

        if let Err(source) = rdr.read_record(&mut type_record) {
            return Err(MatcherError::NoSchemaRow { source })
        }

        let hdrs = rdr.headers()
            .map_err(|source| MatcherError::CannotReadHeaders { source })?;
        let mut type_map = HashMap::new();
        let mut headers = Vec::new();

        for (idx, hdr) in hdrs.iter().enumerate() {
            let data_type = match type_record.get(idx) {
                Some(raw_type) => {
                    let parsed = raw_type.into();
                    if parsed == DataType::UNKNOWN {
                        return Err(MatcherError::UnknownDataTypeInColumn { column: idx })
                    }
                    parsed
                },
                None => return Err(MatcherError::NoSchemaTypeForColumn { column: idx }),
            };

            headers.push(hdr.into());
            type_map.insert(hdr.into(), data_type);
        }

        Ok(Self { headers, type_map })
    }

    pub fn headers(&self) -> &[String] {
        &self.headers
    }
}