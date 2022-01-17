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
            match read_next(self.pos, &mut self.data_readers, &mut self.derived_readers, true) {
                Ok(result) => {
                    if let Some((data, derived)) = result {
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

///
/// Advances the data file reader (and if present, the derived file reader) by one record and returns the record(s) to the caller.
///
/// If the readers are at the end of file then returns None.
///
/// If the data reader encounters a matched record, then, if filter_status is true, returns None.
///
/// If an error is returned from either reader, then it is returned to the caller.
///
fn read_next(pos: usize, data_readers: &mut CsvReaders, derived_readers: &mut Option<CsvReaders>, filter_status: bool)
    -> Result<Option<(csv::ByteRecord /* record data */, csv::ByteRecord /* derived data */)>, csv::Error> {

    let mut data_buffer = csv::ByteRecord::new();
    let mut derived_buffer = csv::ByteRecord::new();

    loop {
        match data_readers[pos].read_byte_record(&mut data_buffer) {
            Ok(result) => {
                if let Some(derived_readers) = derived_readers {
                    let _ = derived_readers[pos].read_byte_record(&mut derived_buffer);
                }

                match result {
                    true  => {
                        if !filter_status || String::from_utf8_lossy(data_buffer.get(COL_STATUS).expect("no status")) == UNMATCHED {
                            return Ok(Some((data_buffer, derived_buffer)))
                        }
                    },
                    false => return Ok(None),
                }
            },
            Err(err) => return Err(err),
        }
    }
}