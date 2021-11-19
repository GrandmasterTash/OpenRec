use crate::{datafile::DataFile, record::Record};


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
pub struct Grid {
    files: Vec<DataFile>,       // All of the sources of records.
    records: Vec<Box<Record>>,  // Represents each row from each of the above files.
    header_cache: Vec<String>,  // Combined headers from all added files.
}

impl Grid {
    pub fn new() -> Self {
        Self {
            files: vec!(),
            records: vec!(),
            header_cache: vec!(),
        }
    }

    pub fn add_file(&mut self, file: DataFile) {
        self.files.push(file);

        // Rebuild the column header cache.
        self.header_cache = self.files
            .iter()
            .flat_map(|f| {
                f.schema().headers()
            })
            .map(String::clone)
            .collect::<Vec<String>>();
    }

    pub fn add_record(&mut self, record: Record) {
        self.records.push(Box::new(record));
    }

    pub fn files(&self) -> &[DataFile] {
        &self.files
    }

    pub fn records(&self) -> &Vec<Box<Record>> {
        &self.records
    }

    pub fn headers(&self) -> &[String] {
        &self.header_cache
    }

    pub fn _get<'a>(&self, column: usize, record: &'a Record) -> Option<&'a [u8]> {
        println!("NOT TESTED");
        let mut tot_cols = 0;
        for file in &self.files {
            if column < (tot_cols + file.schema().headers().len()) {
                return record.inner().get(column - tot_cols)
            }
            tot_cols += file.schema().headers().len();
        }
        None
    }

    pub fn record_data<'a>(&self, record: &'a Record) -> Vec<Option<&'a [u8]>> {
        let mut data = Vec::with_capacity(self.headers().len());

        for (f_idx, file) in self.files.iter().enumerate() {
            for col in 0..file.schema().headers().len() {
                if f_idx == record.file_idx() {
                    data.push(Some(record.inner().get(col).unwrap()));
                } else {
                    data.push(None);
                }
            }
        }

        data
    }
}