#[derive(Debug)]
pub struct Record {
    file_idx: usize,
    inner: csv::ByteRecord,
}

impl Record {
    pub fn new(file_idx: usize, inner: csv::ByteRecord) -> Self {
        Self { file_idx, inner }
    }

    pub fn file_idx(&self) -> usize {
        self.file_idx
    }

    pub fn inner(&self) -> &csv::ByteRecord {
        &self.inner
    }
}