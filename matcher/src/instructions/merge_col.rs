use crate::{error::MatcherError, model::{data_type::DataType, grid::Grid, data_accessor::DataAccessor, record::Record}};

///
/// Create a new column whose value comes from the first non-empty source column specified.
///
pub fn merge_cols(source: &[String], record: &Record, accessor: &mut DataAccessor) -> Result<(), MatcherError> {
    record.merge_col_from(source, accessor)?; // TODO: Can probably remove this now and call directly?

    Ok(())
}

///
/// Ensure each source column exists in the grid and has the same datatype.
///
pub fn validate(source: &[String], grid: &mut Grid) -> Result<DataType, MatcherError> {

    let mut data_type = DataType::UNKNOWN;

    for header in source {
        if !grid.schema().headers().iter().any(|h| h == header) {
            return Err(MatcherError::MissingSourceColumn { header: header.into() })
        }

        match grid.schema().data_type(header) {
            Some(dt) => {
                if data_type == DataType::UNKNOWN {
                    data_type = *dt;

                } else if data_type != *dt {
                    return Err(MatcherError::InvalidSourceDataType { header: header.into(), this_type: *dt, other_type: data_type })
                }
            },
            None => return Err(MatcherError::MissingSourceColumn { header: header.into() }),
        }
    }

    if data_type == DataType::UNKNOWN {
        return Err(MatcherError::NoValidSourceColumns {})
    }

    Ok(data_type)
}