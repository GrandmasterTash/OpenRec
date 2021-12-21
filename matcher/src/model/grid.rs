use ubyte::ToByteUnit;
use std::{fs::DirEntry, time::Instant};
use rlua::Error as LuaError;
use crate::{error::MatcherError, folders::{self, ToCanoncialString}, model::{datafile::DataFile, record::Record, schema::{FileSchema, GridSchema}}, Context, blue, data_accessor::DataAccessor, formatted_duration_rate};


///
/// Represents a virtual grid of data from one or more CSV files.
///
/// As data is loaded from additional files, it's column and rows are appended to the grid. So for example,
/// if we had two files invoice I and payments P, the grid may look like this: -
///
/// i.Ref i.Amount p.Number p.Amount
/// ABC   10.99    -------- --------    << Invoice
/// DEF   11.00    -------- --------    << Invoice
/// ----- -------- 123456   100.00      << Payment
/// ----- -------- 323232   250.50      << Payment
///
/// A Charter can be used to manipulate the grid. For example to merge two columns together. For example, if
/// we had an Instruction::MERGE_COLUMN { name: "AMOUNT", source: ["i.Amount", "p.Amount"]} then the grid above
/// would look like this: -
///
/// i.Ref i.Amount p.Number p.Amount AMOUNT
/// ABC      10.99 -------- --------  10.99
/// DEF      11.00 -------- --------  11.00
/// ----- --------   123456   100.00 100.00
/// ----- --------   323232   250.50 250.50
///
/// Note: No memory is allocted for the empty cells shown above.
///
pub struct Grid {
    records: Vec<Box<Record>>,  // Represents each row from one of the sourced files.
    schema: GridSchema,         // Represents the column structure of the grid and maps headers to the underlying record columns.
}

impl Grid {
    pub fn schema(&self) -> &GridSchema {
        &self.schema
    }

    pub fn schema_mut(&mut self) -> &mut GridSchema {
        &mut self.schema
    }

    pub fn remove_deleted(&mut self) {
        self.records.retain(|r| !r.deleted())
    }

    pub fn records(&self) -> &Vec<Box<Record>> {
        &self.records
    }

    pub fn records_mut(&mut self) -> Vec<&mut Box<Record>> {
        self.records.iter_mut().collect()
    }

    ///
    /// Return how much memory all the ByteRecords are using.
    ///
    pub fn memory_usage(&self) -> usize {
        memory_usage(self.records())
    }

    ///
    /// Load data into the grid.
    ///
    pub fn load(ctx: &Context) -> Result<Self, MatcherError> {

        let mut records = vec!();
        let mut grid_schema = GridSchema::default();

        // Load and index al pending records.
        for (idx, pattern) in ctx.charter().file_patterns().iter().enumerate() {
            log::info!("Sourcing data with pattern [{}]", pattern);
            // TODO: Validate the source path is canonicalised in the rec base.

            // Track schema's added for this source instruction - if any do not equal, return a validation error.
            // Because all files of the same record type will need the same schema for any single match run.
            let mut last_schema_idx = None;

            for file in folders::files_in_matching(ctx, pattern)? {
                let started = Instant::now();
                log::debug!("Reading file {} ({})", file.path().to_string_lossy(), file.metadata().unwrap().len().bytes());

                // For now, just count all the records in a file and log them.
                let mut count = 0;

                let mut rdr = csv::ReaderBuilder::new()
                    .from_path(file.path())
                    .map_err(|source| MatcherError::CannotOpenCsv { source, path: file.path().to_canoncial_string() })?;

                // Build a schema from the file's header rows.
                let prefix = field_prefix(ctx, &file, idx, pattern)
                    .map_err(LuaError::external)?;

                let schema = FileSchema::new(prefix, &mut rdr)
                    .map_err(|source| MatcherError::BadSourceFile { path: file.path().to_canoncial_string(), description: source.to_string() })?;

                // Use an existing schema from the grid, if there is one, otherwise add this one.
                let schema_idx = grid_schema.add_file_schema(schema.clone());
                last_schema_idx = validate_schema(&grid_schema, schema_idx, &last_schema_idx, &schema, pattern)?;

                // Register the data file with the grid.
                let file_idx = grid_schema.add_file(DataFile::new(&file, schema_idx)?);

                // Create an in-memory index for each sourced record.
                for result in rdr.byte_records() {
                    let csv_record = result // Ensure we can read the record - but ignore it at this point.
                        .map_err(|source| MatcherError::CannotParseCsvRow { source, path: file.path().to_canoncial_string() })?;

                    let record = Box::new(Record::new(file_idx as u16, &csv_record.position()
                        .expect("No position for a record in a file?").clone()));

                    records.push(record);
                    count += 1;
                }

                let (duration, _rate) = formatted_duration_rate(count, started.elapsed());

                log::info!("  {} records read from file {} in {}. Memory Usage {}.",
                    count,
                    file.file_name().to_string_lossy(),
                    blue(&duration),
                    blue(&format!("{:.0}", memory_usage(&records).bytes())));
            }
        }

        Ok(Grid { records, schema: grid_schema })
    }

    ///
    /// Writes all the grid's data to a file at this point
    ///
    pub fn debug_grid(&self, ctx: &Context, filename: &str, accessor: &mut DataAccessor) {
        let output_path = folders::debug_path(ctx).join(filename);
        log::debug!("Creating grid debug file {}...", output_path.to_canoncial_string());

        let mut wtr = csv::WriterBuilder::new().quote_style(csv::QuoteStyle::Always).from_path(&output_path).expect("Unable to build a debug writer");
        wtr.write_record(self.schema().headers()).expect("Unable to write the debug headers");

        for record in &self.records {
            wtr.write_record(record.as_strings(self.schema(), accessor)).expect("Unable to write record");
        }

        wtr.flush().expect("Unable to flush the debug file");
        log::debug!("...{} rows written to {}", self.records.len(), output_path.to_canoncial_string());
    }
}


fn field_prefix(ctx: &Context, file: &DirEntry, pattern_idx: usize, pattern: &str) -> Result<Option<String>, MatcherError> {
    Ok(match ctx.charter().use_field_prefixes() {
        true => {
            match ctx.charter().field_aliases() {
                Some(aliases) => match aliases.get(pattern_idx) {
                    Some(alias) => Some(alias.clone()),
                    None => return Err(MatcherError::CharterValidationError { reason: format!("No alias for file pattern {} idx {}", pattern, pattern_idx)}),
                },
                None => Some(folders::entry_shortname(file)),
            }
        },
        false => None,
    })
}

// TODO: This seems like it should be part of add_file_schema in GridSchema.....
fn validate_schema(grid_schema: &GridSchema, schema_idx: usize, last_schema_idx: &Option<usize>, schema: &FileSchema, filename: &str)
    -> Result<Option<usize>, MatcherError> {

    if let Some(last) = last_schema_idx {
        if *last != schema_idx {
            let existing: &FileSchema = &grid_schema.file_schemas()[*last];
            return Err(MatcherError::SchemaMismatch { filename: filename.into(), first: existing.to_short_string(), second: schema.to_short_string() })
        }
    }

    Ok(Some(schema_idx))
}

///
/// Return how much memory all the ByteRecords are using.
///
pub fn memory_usage(records: &[Box<Record>]) -> usize {
    records.iter().map(|r| r.memory_usage()).sum()
}