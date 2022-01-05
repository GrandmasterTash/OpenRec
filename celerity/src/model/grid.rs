use ubyte::ToByteUnit;
use anyhow::Context as ErrContext;
use super::grid_iter::GridIterator;
use core::charter::MatchingSourceFile;
use std::{fs::DirEntry, time::Instant};
use crate::{error::{MatcherError, here}, folders::{self, ToCanoncialString}, model::{datafile::DataFile, schema::{FileSchema, GridSchema}}, Context, blue, formatted_duration_rate, utils};

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
    count: usize,
    schema: GridSchema,         // Represents the column structure of the grid and maps headers to the underlying record columns.
}

impl Grid {
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn schema(&self) -> &GridSchema {
        &self.schema
    }

    pub fn schema_mut(&mut self) -> &mut GridSchema {
        &mut self.schema
    }

    // TODO: Allow matched handle to call and decrement the count.
    // pub fn remove_deleted(&mut self) {
    //     // self.records.retain(|r| !r.deleted())
    // }

    pub fn iter(&self, ctx: &Context) -> GridIterator {
        GridIterator::new(ctx, self)
    }

    ///
    /// Load data into the grid.
    ///
    pub fn load(ctx: &Context) -> Result<Self, MatcherError> {

        let mut grid_schema = GridSchema::default();
        let mut total_count = 0;

        // Load and index all pending records.
        for source_file in ctx.charter().source_files() {
            log::info!("Sourcing data with pattern [{}]", source_file.pattern());
            // TODO: Validate the source path is canonicalised in the rec base.

            // Track schema's added for this source instruction - if any do not equal, return a validation error.
            // Because all files of the same record type will need the same schema for any single match run.
            let mut last_schema_idx = None;

            for file in folders::files_in_matching(ctx, source_file.pattern())? {
                let (count, last) = load_file(&file, source_file, &mut grid_schema, last_schema_idx)?;
                last_schema_idx = last;
                total_count += count;
            }
        }

        log::info!("Scanned {} record - ready to match", blue(&format!("{}", total_count)));

        Ok(Grid { count: total_count, schema: grid_schema })
    }

    ///
    /// Writes all the grid's data to a file at this point
    ///
    pub fn debug_grid(&self, ctx: &Context, sequence: usize) {
        if ctx.charter().debug() {
            let output_path = folders::debug_path(ctx)
                .join(format!("{timestamp}_{phase_num}_{phase_name:?}_{sequence}.debug.csv",
                phase_num = ctx.phase().ordinal(),
                phase_name = ctx.phase(),
                sequence = sequence,
                timestamp = ctx.ts()
            ));

            log::debug!("Creating grid debug file {}...", output_path.to_canoncial_string());

            let mut writer = utils::writer(&output_path);
            writer.write_record(self.schema().headers()).expect("Unable to write the debug headers");

            let mut count = 0;
            for record in self.iter(ctx) {
                writer.write_record(record.as_strings()).expect("Unable to write record");
                count += 1;
            }

            writer.flush().expect("Unable to flush the debug file");
            log::debug!("...{} rows written to {}", count, output_path.to_canoncial_string());
        }
    }

    // ///
    // /// Writes all the grid's data to a file at this point
    // ///
    // pub fn start_debug_records(&self, ctx: &Context, sequence: usize) -> Option<csv::Writer<File>> {
    //     if ctx.charter().debug() {
    //         let output_path = folders::debug_path(ctx)
    //             .join(format!("{timestamp}_{phase_num}_{phase_name:?}_{sequence}.debug.csv",
    //             phase_num = ctx.phase().ordinal(),
    //             phase_name = ctx.phase(),
    //             sequence = sequence,
    //             timestamp = ctx.ts()
    //         ));

    //         log::debug!("Creating grid debug file {}...", output_path.to_canoncial_string());

    //         let mut wtr = csv::WriterBuilder::new()
    //             .quote_style(csv::QuoteStyle::Always)
    //             .buffer_capacity(*CSV_BUFFER)
    //             .from_path(&output_path)
    //             .expect("Unable to build a debug writer");
    //         wtr.write_record(self.schema().headers()).expect("Unable to write the debug headers");
    //         return Some(wtr)
    //     }
    //     None
    // }

    // ///
    // /// Writes all the data specified to a file at this point
    // ///
    // pub fn debug_records(&self, wtr: &mut Option<csv::Writer<File>>, records: &[&Record]) {
    //     if let Some(wtr) = wtr {
    //         for record in records {
    //             wtr.write_record(record.as_strings()).expect("Unable to write record");
    //         }
    //     }
    // }

    // ///
    // /// Ensure all debug data is written.
    // ///
    // pub fn finish_debug_records(&self, wtr: Option<csv::Writer<File>>) {
    //     if let Some(mut wtr) = wtr {
    //         wtr.flush().expect("Unable to flush the debug file");
    //     }
    // }
}

///
/// Parse each csv row in the file to ensure it's parseable. Count the rows and ensure no two files loaded from the same pattern,
/// have different column schemas.
///
fn load_file(file: &DirEntry, source_file: &MatchingSourceFile, grid_schema: &mut GridSchema, last_schema_idx: Option<usize>)
    -> Result<(usize /* record count */, Option<usize> /* last_schema_idx */), MatcherError> {

    let started = Instant::now();
    log::debug!("Reading file {path} ({len})",
        path = file.path().to_string_lossy(),
        len = file.metadata().with_context(|| format!("Unable to get metadata for {}{}", file.path().to_canoncial_string(), here!()))?.len().bytes());

    // For now, just count all the records in a file and log them.
    let mut count = 0;

    let mut rdr = utils::reader(file.path(), false);

    let schema = FileSchema::new(source_file.field_prefix(), &mut rdr)
        .map_err(|source| MatcherError::BadSourceFile { path: file.path().to_canoncial_string(), description: source.to_string() })?;

    // Use an existing schema from the grid, if there is one, otherwise add this one.
    let schema_idx = grid_schema.add_file_schema(schema.clone());
    let last_schema_idx = validate_schema(&grid_schema, schema_idx, &last_schema_idx, &schema, source_file.pattern())?;

    // Register the data file with the grid.
    let _file_idx = grid_schema.add_file(DataFile::new(&file, schema_idx)?);

    // Validate each record can be parsed okay.
    for result in rdr.byte_records() {
        let _csv_record = result // Ensure we can read the record - but ignore it at this point.
            .map_err(|source| MatcherError::CannotParseCsvRow { source, path: file.path().to_canoncial_string() })?;

        count += 1;
    }

    let (duration, _rate) = formatted_duration_rate(count, started.elapsed());

    log::info!("  {} records read from file {} in {}",
        count,
        file.file_name().to_string_lossy(),
        blue(&duration));

    Ok((count, last_schema_idx))
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