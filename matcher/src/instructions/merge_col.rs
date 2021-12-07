use std::time::Instant;
use crate::{error::MatcherError, model::{data_type::DataType, grid::Grid, schema::Column, data_accessor::DataAccessor, record::Record}, formatted_duration_rate, blue};

///
/// Create a new column whose value comes from the first non-empty source column specified.
///
// pub fn merge_cols(name: &str, source: &[String], grid: &mut Grid, accessor: &mut DataAccessor) -> Result<(), MatcherError> {

//     let start = Instant::now();

//     log::info!("Merging columns into {}", name);

//     // Validate - ensure all source columns exist and have the same data-type.
//     let data_type = validate(source, grid)?;
//     // BUG: Errors if no rows exist - should skip. Fix when optimised to stream through the data.
//     // BUG: If column doesn't exist error is very weak.
//     // merge column not working look at output debug (or output debug not working)

//     // Add the projected column to the schema.
//     grid.schema_mut().add_merged_column(Column::new(name.into(), None, data_type))?;

//     // // Snapshot the schema so we can iterate mutable records in a mutable grid.
//     // let schema = grid.schema().clone();

//     // // Get readers for the source data.
//     // let mut rdrs = grid.readers();

//     for record in grid.records_mut() {
//         // record.merge_col_from(source, &schema, &mut rdrs[record.file_idx()])?;
//         record.merge_col_from(source, accessor)?;
//     }

//     let (duration, _rate) = formatted_duration_rate(1, start.elapsed());
//     log::info!("Merging took {}", blue(&duration));

//     Ok(())
// }
pub fn merge_cols(name: &str, source: &[String], record: &Record, accessor: &mut DataAccessor) -> Result<(), MatcherError> {

    // let start = Instant::now();

    // log::info!("Merging columns into {}", name);

    // // Validate - ensure all source columns exist and have the same data-type.
    // let data_type = validate(source, grid)?;
    // // BUG: Errors if no rows exist - should skip. Fix when optimised to stream through the data.
    // // BUG: If column doesn't exist error is very weak.
    // // merge column not working look at output debug (or output debug not working)

    // // Add the projected column to the schema.
    // grid.schema_mut().add_merged_column(Column::new(name.into(), None, data_type))?;

    // // Snapshot the schema so we can iterate mutable records in a mutable grid.
    // let schema = grid.schema().clone();

    // // Get readers for the source data.
    // let mut rdrs = grid.readers();

    // for record in grid.records_mut() {
        // record.merge_col_from(source, &schema, &mut rdrs[record.file_idx()])?;
        record.merge_col_from(source, accessor)?;
    // }

    // let (duration, _rate) = formatted_duration_rate(1, start.elapsed());
    // log::info!("Merging took {}", blue(&duration));

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