use std::sync::Arc;
use super::grid::Grid;
use crate::{model::{record::Record, schema::GridSchema}, Context, utils::{self, csv::CsvReaders}};

const UNMATCHED: &str = "0"; // = 0 ascii.
const COL_STATUS: usize = 0;

///
/// Iterator allows iterating the record (indexes) in the grid.
///
pub struct GridIterator {
    pos: usize, // reader index.
    schema: Arc<GridSchema>,
    data_readers: CsvReaders,
    derived_readers: Option<CsvReaders>,
}

impl GridIterator {
    pub fn new(ctx: &Context, grid: &Grid) -> Self {
        let data_readers = grid.schema().files()
            .iter()
            .map(|file| utils::csv::reader(file.path(), true))
            .collect();

        let derived_readers = match ctx.phase() {
            crate::Phase::MatchAndGroup        |
            crate::Phase::ComleteAndArchive =>
                Some(grid.schema().files()
                    .iter()
                    .map(|file| utils::csv::reader(file.derived_path(), true))
                    .collect()),
            _ => None,
        };

        Self {
            pos: 0,
            schema: Arc::new(grid.schema().clone()),
            data_readers,
            derived_readers,
        }
    }
}

impl Iterator for GridIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we've reached the end of the last file, return None.
            if self.pos == self.data_readers.len() {
                return None
            }

            // Read a row from the csv file.
            match read_next(self.pos, &mut self.data_readers, true) {
                Ok(data) => {
                    if let Some(data) = data  {
                        // Read a row from the derived csv file, if applicable.
                        let derived = match &mut self.derived_readers {
                            Some(derived_readers) => {
                                match read_next(self.pos, derived_readers, false) {
                                    Ok(derived) => derived.unwrap_or_default(),
                                    Err(err) => panic!("Failed to read next derived record for group: {}", err),
                                }
                            },
                            None => csv::ByteRecord::new(),
                        };

                        return Some(Record::new(self.pos, self.schema.clone(), data, derived))
                    }

                    // If there was no data in the file, move onto the next file.
                    self.pos += 1;
                },
                Err(err) => panic!("Failed to read next record for group: {}", err),
            }
        }
    }
}


fn read_next(pos: usize, readers: &mut CsvReaders, filter_status: bool) -> Result<Option<csv::ByteRecord>, csv::Error> {
    let mut buffer = csv::ByteRecord::new();
    loop {
        match readers[pos].read_byte_record(&mut buffer) {
            Ok(result) => match result {
                true  => {
                    if !filter_status || String::from_utf8_lossy(buffer.get(COL_STATUS).expect("no status")) == UNMATCHED {
                        return Ok(Some(buffer))
                    }
                },
                false => return Ok(None),
            },
            Err(err) => return Err(err),
        }
    }
}