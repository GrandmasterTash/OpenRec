use crate::{data_type::DataType, error::MatcherError, grid::Grid, schema::Column};

///
/// Create a new column whose value comes from the first non-empty source column specified.
///
pub fn merge_cols(name: &str, source: &[String], grid: &mut Grid) -> Result<(), MatcherError> {

    log::info!("Merging columns into {}", name);

    // Validate - ensure all source columns exist and have the same data-type.
    let data_type = validate(source, grid)?;
    // BUG: Errors if no rows exist - should skip. Fix when optimised to stream through the data.
    // BUG: If column doesn't exist error is very weak.

    // Add the projected column to the schema.
    grid.schema_mut().add_merged_column(Column::new(name.into(), data_type))?;

    // Snapshot the schema so we can iterate mutable records in a mutable grid.
    let schema = grid.schema().clone();

    for record in grid.records_mut() {
        record.merge_from(source, &schema);
    }

    Ok(())
}

///
/// Ensure each source column exists in the grid and has the same datatype.
///
fn validate(source: &[String], grid: &mut Grid) -> Result<DataType, MatcherError> {

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