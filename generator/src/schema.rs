use rand::prelude::StdRng;
use crate::{column::{Column, ColumnMeta}, data_type::DataType};

#[derive(Debug)]
pub struct Schema {
    columns: Vec<Column>
}

impl Schema {
    ///
    /// Parse a comma-separated string of data-type short-codes and turn into a schema with generated column header names
    /// and randomly sized numbers.
    ///
    pub fn new(raw: &str, rng: &mut StdRng, additional: &mut Vec<Column>) -> Self {
        let mut columns = vec!();
        let mut idx = 1u16;

        columns.append(additional);

        // Parse all the other columns.
        for dt in raw.split(",") {
            let data_type: DataType = dt.into();

            if data_type == DataType::UNKNOWN {
                panic!("Unknown data type '{}'", dt);
            }

            // Generate a random(ish), column definition.
            columns.push(Column::new(
                data_type,
                format!("Column_{}", idx),
                ColumnMeta::generate(data_type, rng)));

            idx += 1;
        }

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

    pub fn schema_vec(&self) -> Vec<&str> {
        self.columns
        .iter()
        .map(|c| c.data_type().into())
        .collect::<Vec<&str>>()
    }
}


#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::*;

    #[test]
    fn test_parse_ok() {
        let mut rng = StdRng::seed_from_u64(1234567890u64);
        let schema = Schema::new("ID,BO,BY,CH,DA,DT,DE,IN,LO,SH,ST,ID,BO", &mut rng, &mut vec!());

        // Parsed columns.
        for idx in 0..13 {
            assert_eq!(schema.columns()[idx].header(), format!("Column_{}", idx+1));
        }

        // Parsed columns.
        assert_eq!(schema.columns()[0].data_type(), DataType::UUID);
        assert_eq!(schema.columns()[1].data_type(), DataType::BOOLEAN);
        assert_eq!(schema.columns()[2].data_type(), DataType::BYTE);
        assert_eq!(schema.columns()[3].data_type(), DataType::CHAR);
        assert_eq!(schema.columns()[4].data_type(), DataType::DATE);
        assert_eq!(schema.columns()[5].data_type(), DataType::DATETIME);
        assert_eq!(schema.columns()[6].data_type(), DataType::DECIMAL);
        assert_eq!(schema.columns()[7].data_type(), DataType::INTEGER);
        assert_eq!(schema.columns()[8].data_type(), DataType::LONG);
        assert_eq!(schema.columns()[9].data_type(), DataType::SHORT);
        assert_eq!(schema.columns()[10].data_type(), DataType::STRING);
        assert_eq!(schema.columns()[11].data_type(), DataType::UUID);
        assert_eq!(schema.columns()[12].data_type(), DataType::BOOLEAN);

    }

    #[test]
    #[should_panic(expected = "Unknown data type 'x'")]
    fn test_parse_err() {
        let mut rng = StdRng::seed_from_u64(1234567890u64);
        let _schema = Schema::new("ID,BO,BY,x", &mut rng, &mut vec!());
    }
}