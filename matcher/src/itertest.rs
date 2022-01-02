use std::fs::File;
use crate::model::record::Record;

struct MiniGrid {
    files: Vec<String>, // Paths to files.
}

impl MiniGrid {
    pub fn new(files: Vec<String>) -> Self {
        Self { files }
    }

    pub fn files(&self) -> &[String] {
        &self.files
    }
}


struct GridIterator {
    pos: usize,
    record: csv::ByteRecord,
    readers: Vec<csv::Reader<File>>
}

impl GridIterator {
    pub fn new(grid: &MiniGrid) -> Self {
        Self {
            pos: 0,
            record: csv::ByteRecord::new(),
            readers: grid.files().iter().map(|file| {
                // Create a reader for each file - skip the schema row.
                let mut rdr = csv::ReaderBuilder::new().from_path(file).unwrap(); // TODO: Don't unwrap any of these.
                let mut ignored = csv::ByteRecord::new();
                rdr.read_byte_record(&mut ignored).unwrap();
                rdr
            })
            .collect()
        }
    }
}

impl Iterator for GridIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.pos == self.readers.len() {
                return None
            }

            // Attempt to read the record from the source csv file.
            match self.readers[self.pos].read_byte_record(&mut self.record) {
                Ok(result) => {
                    if result {
                        return Some(Record::new(self.pos as u16, self.record.position().expect("no position for record")))
                    }

                    self.pos += 1;
                },
                Err(_) => return None, // TODO: Log error.
            }
        }
    }
}

impl IntoIterator for MiniGrid {
    type Item = Record;
    type IntoIter = GridIterator;

    fn into_iter(self) -> Self::IntoIter {
        GridIterator::new(&self)
    }
}

fn try_it() {
    let grid = MiniGrid::new(vec!(
        "./tmp/20211129_043300000_04-invoices.csv".into(),
        "./tmp/20211129_043300000_04-payments.csv".into()));

    for record in grid.into_iter() {
        println!("RECORD: file {} row {}", record.file_idx(), record.row());
    }

/*
    INV PAYA PAYB REC
    Grid
        records
            > INV1
            > INV2
            > PAYA1
            > PAYA2
            > PAYB1
            > PAYB2
            > PAYB3
            > REC1
            > REC2


    Customer Iterator is a list of csv reader(iterators)
    Creates new Record each time.
    Can't delete/ignore records.
        Changesets will have to pre-process and exclude ignored records.
        Matching will change drastically anyway.

    INV PAYA PAYB REC
    Grid
        records
            > INV  > reader   (when next = None, use next iterator).
            > PAYA > reader
            > PAYB > reader
            > REC  > reader

*/

}


#[cfg(test)]
mod test {
    use std::fs::OpenOptions;
    use positioned_io::{WriteAt, ReadAt};
    use byteorder::{ByteOrder, LittleEndian, BigEndian};

    use super::*;

    #[test]
    fn test_it() {
        try_it();
    }


    #[test]
    fn test_random_access() {
        // // Read record positions with CSV reader.
        // let grid = MiniGrid::new(vec!("./tmp/randy.csv".into()));
        // let mut iter = GridIterator::new(&grid);
        // let record = iter.next().unwrap();
        // println!("REC 1: {:?}", record.pos());


        // // Random access read some bytes from record.
        // let file = File::open("./tmp/randy.csv").unwrap();
        // let mut buf = vec![0; 1];
        // let bytes_read = file.read_at(record.pos().byte()+1, &mut buf).unwrap();
        // println!("{:?}", buf);

        // // Modify exclude value via random access.

        // // 30 = 0, 31 = 1
        // // Put the integer in a buffer.
        // // let mut buf = vec![0; 2];
        // // LittleEndian::write_u16(&mut buf, 31);
        // let mut buf = vec!(0x31);
        // // let mut buf = vec!(0x30);

        // // Write it to the file.
        // let mut file = OpenOptions::new().write(true).open("./tmp/randy.csv").unwrap();
        // file.write_all_at(record.pos().byte() +/* Skip double-quotes */ 1, &buf).unwrap();

        // // Check other records can still be read.
        // let record = iter.next().unwrap();
        // println!("REC 2: {:?}", record.pos());

        // let record = iter.next().unwrap();
        // println!("REC 3: {:?}", record.pos());
    }
}