use crate::{datafile::DataFile, record::Record, schema::GridSchema};

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
    /// Return how much memory all the ByteRecords are using.
    ///
    pub fn memory_usage(&self) -> usize {
        self.records.iter().map(|r| r.inner().as_slice().len()).sum()
    }
}