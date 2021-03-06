use core::data_type::DataType;
use crate::{error::MatcherError, model::grid::Grid};

///
/// Ensure each source column exists in the grid and has the same datatype.
///
pub fn validate(source: &[String], grid: &mut Grid) -> Result<DataType, MatcherError> {

    let mut data_type = DataType::Unknown;

    for header in source {
        if !grid.schema().headers().iter().any(|h| h == header) {
            continue // If a source column isn't present move on, it could be that there is no file present with that source.
        }

        match grid.schema().data_type(header) {
            Some(dt) => {
                if data_type == DataType::Unknown {
                    data_type = *dt;

                } else if data_type != *dt {
                    return Err(MatcherError::InvalidSourceDataType { header: header.into(), this_type: *dt, other_type: data_type })
                }
            },
            None => return Err(MatcherError::MissingSourceColumn { header: header.into() }),
        }
    }

    if data_type == DataType::Unknown {
        return Ok(DataType::String)
        // return Err(MatcherError::NoValidSourceColumns {})
    }

    Ok(data_type)
}