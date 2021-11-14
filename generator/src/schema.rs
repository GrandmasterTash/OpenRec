use crate::column::Column;

#[derive(Debug)]
pub struct Schema {
    columns: Vec<Column>
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn header_vec(&self) -> Vec<&str> {
        self.columns
            .iter()
            .map(Column::header)
            .collect::<Vec<&str>>()
    }
}


