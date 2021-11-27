use crate::{datafile::DataFile, error::MatcherError, folders::{self, ToCanoncialString}, grid::Grid, record::Record, schema::FileSchema};

pub fn source_data(file_patterns: &[String], grid: &mut Grid) -> Result<(), MatcherError> {

    for pattern in file_patterns {
        log::info!("Sourcing data with pattern [{}]", pattern);

        // Track schema's added for this source instruction - if any do not equal, return a validation error.
        // Because all files of the same record type will need the same schema for any single match run.
        let mut last_schema_idx = None;

        // TODO: Include .unmatched.csv files in this sourcing.

        for file in folders::files_in_matching(pattern)? {
            log::info!("Reading file {}", file.path().to_string_lossy());

            // For now, just count all the records in a file and log them.
            let mut count = 0;

            let mut rdr = csv::ReaderBuilder::new()
                .from_path(file.path())
                .map_err(|source| MatcherError::CannotOpenCsv { source, path: file.path().to_canoncial_string() })?;

            // Build a schema from the file's header rows.
            let schema = FileSchema::new(folders::entry_shortname(&file), &mut rdr)?;

            // Use an existing schema from the grid, if there is one, otherwise add this one.
            let schema_idx = grid.schema_mut().add_file_schema(schema.clone());
            last_schema_idx = validate_schema(schema_idx, &last_schema_idx, &schema, &grid, pattern)?;

            // Register the data file with the grid.
            let file_idx = grid.add_file(DataFile::new(&file, schema_idx)?);

            // Load the data as bytes into memory.
            for result in rdr.byte_records() {
                let record = result
                    .map_err(|source| MatcherError::CannotParseCsvRow { source, path: file.path().to_canoncial_string() })?;

                count += 1;
                grid.add_record(Record::new(file_idx, schema_idx, count + /* 2 header rows */ 2, record));
            }

            log::info!("{} records read from file {}", count, file.file_name().to_string_lossy());
        }
    }

    Ok(())
}

fn validate_schema(schema_idx: usize, last_schema_idx: &Option<usize>, schema: &FileSchema, grid: &Grid, filename: &str)
    -> Result<Option<usize>, MatcherError> {

    if let Some(last) = last_schema_idx {
        if *last != schema_idx {
            let existing: &FileSchema = &grid.schema().file_schemas()[*last];
            return Err(MatcherError::SchemaMismatch { filename: filename.into(), first: existing.to_short_string(), second: schema.to_short_string() })
        }
    }

    Ok(Some(schema_idx))
}