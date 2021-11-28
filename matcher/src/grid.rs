use crate::{datafile::DataFile, error::MatcherError, folders::{self, ToCanoncialString}, record::Record, schema::{FileSchema, GridSchema}};

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
    files: Vec<DataFile>,      // All of the files used to source records.
    records: Vec<Box<Record>>, // Represents each row from each of the above files.
    schema: GridSchema,        // Represents the column structure of the grid and maps headers to the underlying record columns.
}

impl Grid {
    pub fn new() -> Self {
        Self {
            files: vec!(),
            records: vec!(),
            schema: GridSchema::new(),
        }
    }

    pub fn files(&self) -> &[DataFile] {
        &self.files
    }

    pub fn schema(&self) -> &GridSchema {
        &self.schema
    }

    pub fn schema_mut(&mut self) -> &mut GridSchema {
        &mut self.schema
    }

    pub fn add_file(&mut self, file: DataFile) -> usize {
        self.files.push(file);
        self.files.len() - 1
    }

    pub fn add_record(&mut self, record: Record) {
        self.records.push(Box::new(record));
    }

    pub fn remove_matched(&mut self) {
        self.records.retain(|r| !r.matched())
    }

    pub fn records(&self) -> &Vec<Box<Record>> {
        &self.records
    }

    pub fn records_mut(&mut self) -> Vec<&mut Box<Record>> {
        self.records.iter_mut().collect()
    }

    pub fn record_data<'a>(&self, record: &'a Record) -> Vec<Option<&'a [u8]>> {
        let mut data = Vec::with_capacity(self.schema().headers().len());

        for header in self.schema().headers() {
            if let Some(col) = self.schema().position_in_record(header, record) {
                if let Some(value) = record.inner().get(*col) {
                    data.push(Some(value));
                    continue;
                }
            }
            data.push(None);
        }

        data
    }

    ///
    /// Dump the record to a string
    ///
    pub fn record_as_string(&self, idx: usize) -> Option<String> {
        let record = match self.records.get(idx) {
            Some(rec) => rec,
            None => return None,
        };

        let mut data = Vec::with_capacity(self.schema().headers().len());

        for header in self.schema().headers() {
            if let Some(col) = self.schema().position_in_record(header, record) {
                if let Some(value) = record.inner().get(*col) {
                    data.push(format!("{}: \"{}\"", header, String::from_utf8_lossy(value)));
                    continue;
                }
            }
            data.push(format!("{}: --", header));
        }

        Some(data.join(", "))
    }

    ///
    /// Return how much memory all the ByteRecords are using.
    ///
    pub fn memory_usage(&self) -> usize {
        self.records.iter().map(|r| r.inner().as_slice().len()).sum()
    }

    ///
    /// Load data into the grid.
    ///
    pub fn source_data(&mut self, file_patterns: &[String], field_prefixes: bool) -> Result<(), MatcherError> {

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
                let prefix = match field_prefixes {
                    true => Some(folders::entry_shortname(&file)),
                    false => None,
                };
                let schema = FileSchema::new(prefix, &mut rdr)?;

                // Use an existing schema from the grid, if there is one, otherwise add this one.
                let schema_idx = self.schema.add_file_schema(schema.clone());
                last_schema_idx = self.validate_schema(schema_idx, &last_schema_idx, &schema, pattern)?;

                // Register the data file with the grid.
                let file_idx = self.add_file(DataFile::new(&file, schema_idx)?);

                // Load the data as bytes into memory.
                for result in rdr.byte_records() {
                    let record = result
                        .map_err(|source| MatcherError::CannotParseCsvRow { source, path: file.path().to_canoncial_string() })?;

                    count += 1;
                    self.add_record(Record::new(file_idx, schema_idx, count + /* 2 header rows */ 2, record));
                }

                log::info!("{} records read from file {}", count, file.file_name().to_string_lossy());
            }
        }

        Ok(())
    }

    fn validate_schema(&self, schema_idx: usize, last_schema_idx: &Option<usize>, schema: &FileSchema, filename: &str)
        -> Result<Option<usize>, MatcherError> {

        if let Some(last) = last_schema_idx {
            if *last != schema_idx {
                let existing: &FileSchema = &self.schema().file_schemas()[*last];
                return Err(MatcherError::SchemaMismatch { filename: filename.into(), first: existing.to_short_string(), second: schema.to_short_string() })
            }
        }

        Ok(Some(schema_idx))
    }

    ///
    /// Writes all the grid's data to a file at this point
    ///
    pub fn debug_grid(&self, filename: &str) {
        let output_path = folders::debug_path().join(filename);
        // let output_path = folders::debug_path().join(format!("{}output.csv", folders::new_timestamp()));
        log::info!("Creating grid debug file {}...", output_path.to_canoncial_string());

        let mut wtr = csv::WriterBuilder::new().quote_style(csv::QuoteStyle::Always).from_path(&output_path).expect("Unable to build a debug writer");
        wtr.write_record(self.schema().headers()).expect("Unable to write the debug headers");

        for record in &self.records {
            // self.record_as_string(idx)
            let data: Vec<&[u8]> = self.record_data(record)
                .iter()
                .map(|v| v.unwrap_or(b""))
                .collect();
            wtr.write_byte_record(&data.into()).expect("Unable to write record");
        }

        wtr.flush().expect("Unable to flush the debug file");
        log::info!("...{} rows written to {}", self.records.len(), output_path.to_canoncial_string());
    }
}
