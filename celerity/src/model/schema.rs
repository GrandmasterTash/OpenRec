use itertools::Itertools;
use core::data_type::DataType;
use super::{datafile::DataFile};
use std::{collections::HashMap, fs, slice::IterMut};
use crate::{model::record::Record, error::MatcherError};

const STATUS: &str = "OpenRecStatus";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Column {
    header: String,            // For example, INV.Amount
    header_no_prefix: String,  // For example, Amount
    data_type: DataType,
}

///
/// The schema of a CSV data file. The GridSchema will be composed of these and projected and merged columns.
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSchema {
    prefix: Option<String>, // The prefix is appended to every header in this schema. So if the prefix is INV, 'INV.Amount'.
    columns: Vec<Column>,   // Column headers from this file only.
}

///
/// The Schema of the entire Grid of data.
///
/// The grid schema is built from sourced data files and projected columns. It can be used to get or set fields
/// on records in the grid.
///
#[derive(Clone, Debug, Default)]
pub struct GridSchema {
    // Cached column details. The position map is used to get to the correct grid column from a header for a
    // specific record - as records may have different underlying FileSchemas.
    headers: Vec<String>,
    col_map: HashMap<String, Column>,

    // A map of maps to resolve a column by it's header name and then resolve that for a specific sourced csv file.
    // Column positions can be positive or negative.
    //
    // Positive positions run left-to-right and start to the right of negative positions. They represent real CSV columns
    // and map 1-2-1 with the header position in the csv file. Indexes start at zero.
    //
    // Negative position run right-to-left and start to the left of positive positions. They represent dervice columns
    // created from col projections and column mergers. Indexes start at -1.
    //
    // Eg. | AA | BB | CC | DD | EE | FF |
    //     | -3 | -2 | -1 |  0 |  1 |  2 |
    //
    // In the above example AA, BB and CC are derived columns. DD, EE and FF represent real CSV columns.
    //
    position_map: HashMap<usize /* file_schema idx */, HashMap<String /* header */, isize /* column idx */>>,

    // All of the files used to source records.
    files: Vec<DataFile>,

    // Schemas from the files. Multiple files may use the same schema.
    file_schemas: Vec<FileSchema>,

    // Columns created from projection and merge instructions.
    derived_cols: Vec<Column>,
}

impl Column {
    pub fn new(header: String, prefix: Option<String>, data_type: DataType) -> Self {
        Self {
            header: match prefix {
                Some(prefix) => format!("{}.{}", prefix, header),
                None => header.clone(),
            },
            header_no_prefix: header,
            data_type
        }
    }

    pub fn header(&self) -> &str {
        &self.header
    }

    pub fn header_no_prefix(&self) -> &str {
        &self.header_no_prefix
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl FileSchema {
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }
}

impl GridSchema {
    pub fn add_file(&mut self, file: DataFile) -> usize {
        self.files.push(file);
        self.files.len() - 1
    }

    ///
    /// If the schema is already present, return the existing index, otherwise add the schema and return
    /// it's index.
    ///
    pub fn add_file_schema(&mut self, schema: FileSchema) -> Result<usize, MatcherError> {
        match self.file_schemas.iter().position(|s| *s == schema) {
            Some(position) => Ok(position),
            None => {
                // Ensure every column header is unique and won't clash with an existing one.
                for header in schema.columns().iter().map(|c| c.header()).collect::<Vec<&str>>() {
                    if self.headers().iter().any(|h| h == header) {
                        return Err(MatcherError::TwoSchemaWithDuplicateHeader { header: header.to_string() })
                    }
                }

                self.file_schemas.push(schema);
                self.rebuild_cache();
                Ok(self.file_schemas.len() - 1)
            },
        }
    }

    ///
    /// Added the projected column or error if it already exists.
    ///
    pub fn add_projected_column(&mut self, column: Column) -> Result<usize, MatcherError> {

        if self.headers().iter().any(|h| h == column.header()) {
            return Err(MatcherError::ProjectedColumnExists { header: column.header })
        }

        self.derived_cols.push(column);
        self.rebuild_cache();
        Ok(self.derived_cols.len() - 1)
    }

    ///
    /// Added the merged column or error if it already exists.
    ///
    pub fn add_merged_column(&mut self, column: Column) -> Result<usize, MatcherError> {

        if self.headers().iter().any(|h| h == column.header()) {
            return Err(MatcherError::MergedColumnExists { header: column.header })
        }

        self.derived_cols.push(column);
        self.rebuild_cache();
        Ok(self.derived_cols.len() - 1)
    }

    pub fn files(&self) -> &[DataFile] {
        &self.files
    }

    pub fn files_mut(&mut self) -> IterMut<'_, DataFile> {
        self.files.iter_mut()
    }

    pub fn file_schemas(&self) -> &[FileSchema] {
        &self.file_schemas
    }

    pub fn headers(&self) -> &[String] {
        &self.headers
    }

    pub fn column(&self, header: &str) -> Option<&Column> {
        self.col_map.get(header)
    }

    pub fn columns(&self) -> Vec<&Column> {
        self.col_map.values().collect()
    }

    pub fn derived_columns(&self) -> Vec<&Column> {
        self.derived_cols.iter().collect_vec()
    }

    pub fn data_type(&self, header: &str) -> Option<&DataType> {
        match self.col_map.get(header) {
            Some(col) => Some(col.data_type()),
            None => None,
        }
    }

    pub fn position_in_record(&self, header: &str, record: &Record) -> Option<&isize> {
        match self.position_map.get(&self.files[record.file_idx()].schema_idx()) {
            Some(position_map) => position_map.get(header),
            None => None,
        }
    }

    fn rebuild_cache(&mut self) {
        let mut headers = Vec::new();
        let mut col_map = HashMap::new();
        let mut position_map = HashMap::new();

        // Initialise the position map of maps.
        self.file_schemas
            .iter()
            .enumerate()
            .for_each(|(idx, _fsc)| { position_map.insert(idx, HashMap::new()); });

        // Cache all the projected columns.
        self.derived_cols
            .iter()
            .enumerate()
            .for_each(|(c_idx, col)| {
                headers.push(col.header.clone());
                col_map.insert(col.header.clone(), col.clone());
                self.file_schemas
                    .iter()
                    .enumerate()
                    .for_each(|(fs_idx, _fsc)| {
                        position_map
                            .get_mut(&fs_idx)
                            .unwrap_or_else(|| panic!("file {} missing from position map", fs_idx))
                            .insert(col.header.clone(), -((c_idx + 1) as isize)); // Derived columns start at -1
                    });
            });

        // Cache all the file schema columns.
        self.file_schemas
            .iter()
            .enumerate()
            .for_each(|(fs_idx, fsc)| {
                fsc.columns()
                    .iter()
                    .enumerate()
                    .for_each(|(c_idx, col)| {
                        headers.push(col.header.clone());
                        col_map.insert(col.header.clone(), col.clone());
                         position_map
                            .get_mut(&fs_idx)
                            .unwrap_or_else(|| panic!("file {} missing from position map", fs_idx))
                            .insert(col.header.clone(), c_idx as isize);
                    } );
            } );

        self.headers = headers;
        self.col_map = col_map;
        self.position_map = position_map;
    }
}

impl FileSchema {
    ///
    /// Build a hashmap of column header to parsed data-types. The data types should be on the first
    /// csv row after the headers.
    ///
    pub fn new(prefix: &Option<String>, rdr: &mut csv::Reader<fs::File>) -> Result<Self, MatcherError> {
        let mut type_record = csv::StringRecord::new();

        if let Err(source) = rdr.read_record(&mut type_record) {
            return Err(MatcherError::NoSchemaRow { source })
        }

        let hdrs = rdr.headers()
            .map_err(|source| MatcherError::CannotReadHeaders { source })?;

        let mut columns = Vec::new();

        if hdrs.get(0).expect("No header columns") != STATUS {
            return Err(MatcherError::StatusColumnMissing)
        }

        for (idx, hdr) in hdrs.iter().enumerate() {
            let data_type = match type_record.get(idx) {
                Some(raw_type) => raw_type.into(),
                None => return Err(MatcherError::NoSchemaTypeForColumn { column: idx }),
            };

            columns.push(Column::new(hdr.into(), prefix.clone(), data_type));
        }

        Ok(Self { prefix: prefix.clone(), columns })
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn to_short_string(&self) -> String {
        self.columns
            .iter()
            .map(|col| col.data_type.as_str())
            .collect::<Vec<&str>>()
            .join(",")
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers_from_files_with_prefixes_cant_clash() {
        let fs_1 = FileSchema {
            prefix: Some("FS1".into()),
            columns: vec!(
                Column { header: "FS1.COLA".into(), header_no_prefix: "COLA".into(), data_type: DataType::String },
                Column { header: "FS1.COLB".into(), header_no_prefix: "COLB".into(), data_type: DataType::String })
        };

        let fs_2 = FileSchema {
            prefix: Some("FS2".into()),
            columns: vec!(
                Column { header: "FS2.COLA".into(), header_no_prefix: "COLA".into(), data_type: DataType::String },
                Column { header: "FS2.COLB".into(), header_no_prefix: "COLB".into(), data_type: DataType::String })
        };

        let mut gs = GridSchema::default();

        gs.add_file_schema(fs_1).unwrap();
        gs.add_file_schema(fs_2).unwrap();
        assert_eq!(4, gs.headers().len());
    }


    #[test]
    fn test_headers_from_files_without_prefixes_can_clash() {
        let fs_1 = FileSchema {
            prefix: None,
            columns: vec!(
                Column { header: "COLA".into(), header_no_prefix: "COLA".into(), data_type: DataType::String },
                Column { header: "COLB".into(), header_no_prefix: "COLB".into(), data_type: DataType::String })
        };

        let fs_2 = FileSchema {
            prefix: None,
            columns: vec!(
                Column { header: "COLA".into(), header_no_prefix: "COLA".into(), data_type: DataType::String },
                Column { header: "COLB".into(), header_no_prefix: "COLB".into(), data_type: DataType::Boolean })
        };

        let mut gs = GridSchema::default();
        gs.add_file_schema(fs_1).unwrap();

        match gs.add_file_schema(fs_2) {
            Ok(_) => panic!("Expected an error adding duplicate headers"),
            Err(err) => match err {
                MatcherError::TwoSchemaWithDuplicateHeader { .. } => {},
                e @ _ => panic!("Expected TwoSchemaWithDuplicateHeader error got: {}", e),
            },
        }
    }

    #[test]
    fn test_cannot_project_duplicate_name() {
        let fs_1 = FileSchema {
            prefix: Some("FS1".into()),
            columns: vec!(
                Column { header: "FS1.COLA".into(), header_no_prefix: "COLA".into(), data_type: DataType::String },
                Column { header: "FS1.COLB".into(), header_no_prefix: "COLB".into(), data_type: DataType::String })
        };


        let mut gs = GridSchema::default();
        gs.add_file_schema(fs_1).unwrap();

        let col = Column { header: "FS1.COLA".into(), header_no_prefix: "COLA".into(), data_type: DataType::String };
        match gs.add_projected_column(col) {
            Ok(_) => panic!("Expected an error adding duplicate headers"),
            Err(err) => match err {
                MatcherError::ProjectedColumnExists { .. } => {},
                e @ _ => panic!("Expected ProjectedColumnExists error got: {}", e),
            },
        }
    }

    #[test]
    fn test_cannot_merge_duplicate_name() {
        let fs_1 = FileSchema {
            prefix: Some("FS1".into()),
            columns: vec!(
                Column { header: "FS1.COLA".into(), header_no_prefix: "COLA".into(), data_type: DataType::String },
                Column { header: "FS1.COLB".into(), header_no_prefix: "COLB".into(), data_type: DataType::String })
        };

        let mut gs = GridSchema::default();
        gs.add_file_schema(fs_1).unwrap();

        let col = Column { header: "FS1.COLA".into(), header_no_prefix: "COLA".into(), data_type: DataType::String };
        match gs.add_merged_column(col) {
            Ok(_) => panic!("Expected an error adding merged column"),
            Err(err) => match err {
                MatcherError::MergedColumnExists { .. } => {},
                e @ _ => panic!("Expected MergedColumnExists error got: {}", e),
            },
        }
    }
}