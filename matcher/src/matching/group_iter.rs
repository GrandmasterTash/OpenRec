use std::{sync::Arc};
use super::prelude::*;
use crate::{error::MatcherError, model::{record::Record, schema::GridSchema}, folders::{self}, CsvReader, CsvReaders};

///
/// Iterate the file index.sorted.csv and use the merge-key to read entire groups of records.
///
pub struct GroupIterator {
    schema: Arc<GridSchema>,
    index_rdr: CsvReader,
    data_rdrs: CsvReaders,
    derived_rdrs: CsvReaders,
    current: Option<csv::ByteRecord>,
}

impl GroupIterator {
    pub fn new(ctx: &crate::Context, schema: &GridSchema) -> Self {
        Self {
            schema: Arc::new(schema.clone()),
            index_rdr: csv::ReaderBuilder::new()
                .has_headers(false)
                .from_path(folders::matching(ctx).join("index.sorted.csv"))
                .unwrap(),
            data_rdrs: schema.files()
                .iter()
                .map(|file| {
                    // Create a reader for each file - skipping the schema row.
                    let mut rdr = csv::ReaderBuilder::new().from_path(file.path()).unwrap(); // TODO: Don't unwrap any of these.
                    let mut ignored = csv::ByteRecord::new();
                    rdr.read_byte_record(&mut ignored).unwrap();
                    rdr
                })
                .collect(),
            derived_rdrs: schema.files()
                .iter()
                .map(|file| {
                    // Create a reader for each derived file - skipping the schema row.
                    let mut rdr = csv::ReaderBuilder::new().from_path(file.derived_path()).unwrap(); // TODO: Don't unwrap any of these.
                    let mut ignored = csv::ByteRecord::new();
                    rdr.read_byte_record(&mut ignored).unwrap();
                    rdr
                })
                .collect(),
            current: None
        }
    }

    fn new_group(&self, csv_record: &csv::ByteRecord) -> bool {
        match &self.current {
            None => false, // This isn't a NEW group this is the FIRST group.
            Some(current) => {
                current.get(COL_MERGE_KEY).unwrap() != csv_record.get(COL_MERGE_KEY).unwrap()
            },
        }
    }

    ///
    /// Use the file-base index to load the record data and derived data.
    ///
    fn load_record(&mut self, csv_record: &csv::ByteRecord) -> Result<Record, MatcherError> {

        let mut data_pos = csv::Position::new();
        data_pos.set_byte(csv_to_u64(csv_record.get(COL_DATA_BYTE)));
        data_pos.set_line(csv_to_u64(csv_record.get(COL_DATA_LINE)));

        let mut derived_pos = csv::Position::new();
        derived_pos.set_byte(csv_to_u64(csv_record.get(COL_DERIVED_BYTE)));
        derived_pos.set_line(csv_to_u64(csv_record.get(COL_DERIVED_LINE)));

        let file_idx = csv_to_u64(csv_record.get(COL_FILE_IDX)) as usize;

        // Read the real (csv)record using it's indexed position.
        let mut data_record = csv::ByteRecord::new();
        self.data_rdrs[file_idx].seek(data_pos)?;
        self.data_rdrs[file_idx].read_byte_record(&mut data_record)?;

        // Read the derived (csv)record using it's indexed position.
        let mut derived_record = csv::ByteRecord::new();
        self.derived_rdrs[file_idx].seek(derived_pos)?;
        self.derived_rdrs[file_idx].read_byte_record(&mut derived_record)?;

        // Construct a Record.
        Ok(Record::new(file_idx, self.schema.clone(), data_record, derived_record))
    }

    fn group_result(&self, group: Vec<Record>) -> Option<Result<Vec<Record>, MatcherError>> {
        match group.is_empty() {
            true => None,
            false => Some(Ok(group)),
        }
    }
}

pub fn csv_to_u64(bytes: Option<&[u8]>) -> u64 {
    String::from_utf8_lossy(&bytes.expect("Index usize field missing"))
        .parse()
        .expect("Unable to convert index field to usize")
}

impl Iterator for GroupIterator {
    type Item = Result<Vec<Record>, MatcherError>;

    ///
    /// Use the record indexes to look-up the full csv and derived csv data to construct each record in the group.
    ///
    fn next(&mut self) -> Option<Self::Item> {

        let mut group = Vec::new();

        // If we're starting a new group, load the record.
        if let Some(csv_record) = self.current.clone(/* load_record needs mut borrow of self */) {
            match self.load_record(&csv_record) {
                Ok(record) => group.push(record),
                Err(err) => return Some(Err(err)),
            }
        }

        // Read a record index.
        loop {
            let mut csv_record = csv::ByteRecord::new();
            match self.index_rdr.read_byte_record(&mut csv_record) {
                Ok(read) => match read {
                    true  => {
                        // If this new index belongs to a new group, track it and return the current group now.
                        if self.new_group(&csv_record) {
                            self.current = Some(csv_record);
                            return self.group_result(group)
                        }

                        // Otherwise keep appending records to the group.
                        match self.load_record(&csv_record) {
                            Ok(record) => group.push(record),
                            Err(err) => return Some(Err(err)),
                        }

                        // Track the current group.
                        self.current = Some(csv_record);
                    },
                    false => {
                        self.current = None;
                        return self.group_result(group)
                    },
                },
                Err(err) => return Some(Err(err.into())),
            }
        }
    }
}