use crate::data_type::DataType;

#[derive(Debug)]
pub struct Column {
    data_type: DataType,
    header: String,
    precision: Option<u8>, // For numerics
    scale: Option<u8>,     // For numerics
}

impl Column {
    pub fn new(data_type: DataType, header: String, precision: Option<u8>, scale: Option<u8>) -> Self {
        Column {
            data_type,
            header,
            precision,
            scale
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn precision(&self) -> Option<u8> {
        self.precision
    }

    pub fn scale(&self) -> Option<u8> {
        self.scale
    }

    pub fn header(&self) -> &str {
        &self.header
    }
}
