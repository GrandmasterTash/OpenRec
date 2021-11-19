use crate::{datafile::DataFile, record::Record, schema::Schema};


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
    files: Vec<DataFile>,       // All of the sources of records.
    schemas: Vec<Schema>,       // A file layout descriptor, multiple physical files can share the same layout.
    records: Vec<Box<Record>>,  // Represents each row from each of the above files.
    header_cache: Vec<String>,  // Combined headers from all added files.
}

impl Grid {
    pub fn new() -> Self {
        Self {
            files: vec!(),
            schemas: vec!(),
            records: vec!(),
            header_cache: vec!(),
        }
    }

    ///
    /// If the schema is already present, return the existing index, otherwise add the schema and return
    /// it's index.
    ///
    pub fn add_schema(&mut self, schema: Schema) -> usize {
        match self.schemas.iter().position(|s| *s == schema) {
            Some(position) => position,
            None => {
                self.schemas.push(schema);

                // Rebuild the column header cache.
                self.header_cache = self.schemas
                    .iter()
                    .flat_map(|s| s.headers())
                    .map(String::clone)
                    .collect::<Vec<String>>();

                self.schemas.len() - 1
            },
        }
    }

    pub fn add_file(&mut self, file: DataFile) {
        self.files.push(file);
    }

    pub fn add_record(&mut self, record: Record) {
        self.records.push(Box::new(record));
    }

    pub fn files(&self) -> &[DataFile] {
        &self.files
    }

    pub fn schemas(&self) -> &[Schema] {
        &self.schemas
    }

    pub fn records(&self) -> &Vec<Box<Record>> {
        &self.records
    }

    pub fn headers(&self) -> &[String] {
        &self.header_cache
    }

    pub fn record_data<'a>(&self, record: &'a Record) -> Vec<Option<&'a [u8]>> {
        let mut data = Vec::with_capacity(self.headers().len());

        let file = self.files.get(record.file_idx()).expect("Record's file not found"); // TODO: Make this fn return a result.

        for (s_idx, schema) in self.schemas.iter().enumerate() {
            for col in 0..schema.headers().len() {
                if s_idx == file.schema() {
                    data.push(Some(record.inner().get(col).expect("record has no value in column"))); // TODO: Better Result err msg.
                } else {
                    data.push(None);
                }
            }
        }

        data
    }

    ///
    /// Return how much memory all the ByteRecords are using.
    ///
    pub fn memory_usage(&self) -> usize {
        self.records.iter().map(|r| r.inner().as_slice().len()).sum()
    }
}