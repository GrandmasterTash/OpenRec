use std::{collections::HashMap, fs};
use crate::{data_type::DataType, error::MatcherError, record::Record};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    header: String,
    data_type: DataType,
}

///
/// The schema of a CSV data file. The GridSchema will be composed of these and projected and merged columns.
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSchema {
    prefix: String,         // The short part of the filename - see folders::shortname() for details. Each header name
    columns: Vec<Column>,   // is prefixed by this. So 'invoices.amount' for example.
}

///
/// The Schema of the entire Grid of data.
///
/// The grid schema is built from sourced data files and projected columns. It can be used to get or set fields
/// on records in the grid.
///
#[derive(Clone, Debug)]
pub struct GridSchema {
    // Cached column details.
    headers: Vec<String>,
    type_map: HashMap<String, DataType>,
    position_map: HashMap<usize /* file_schema idx */, HashMap<String /* header */, usize /* column idx */>>,

    // Schemas from the files.
    file_schemas: Vec<FileSchema>,

    // Artificial columns.
    projected_columns: Vec<Column>,
    merged_columns: Vec<Column>,
}

impl Column {
    pub fn new(header: String, data_type: DataType) -> Self {
        Self { header, data_type, }
    }

    pub fn header(&self) -> &str {
        &self.header
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl FileSchema {
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

impl GridSchema {
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
            type_map: HashMap::new(),
            position_map: HashMap::new(),
            file_schemas: Vec::new(),
            projected_columns: Vec::new(),
            merged_columns: Vec::new(),
        }
    }

    ///
    /// If the schema is already present, return the existing index, otherwise add the schema and return
    /// it's index.
    ///
    pub fn add_file_schema(&mut self, schema: FileSchema) -> usize {
        match self.file_schemas.iter().position(|s| *s == schema) {
            Some(position) => position,
            None => {
                self.file_schemas.push(schema);
                self.rebuild_cache();
                self.file_schemas.len() - 1
            },
        }
    }

    ///
    /// Added the projected column or error if it already exists.
    ///
    pub fn add_projected_column(&mut self, column: Column) -> Result<usize, MatcherError> {
        if self.projected_columns.contains(&column) {
            return Err(MatcherError::ProjectedColumnExists { header: column.header })
        }

        self.projected_columns.push(column);
        self.rebuild_cache();
        Ok(self.projected_columns.len() - 1)
    }

    ///
    /// Added the merged column or error if it already exists.
    ///
    pub fn add_merged_column(&mut self, column: Column) -> Result<usize, MatcherError> {
        if self.merged_columns.contains(&column) {
            return Err(MatcherError::MergedColumnExists { header: column.header })
        }

        self.merged_columns.push(column);
        self.rebuild_cache();
        Ok(self.merged_columns.len() - 1)
    }

    pub fn file_schemas(&self) -> &[FileSchema] {
        &self.file_schemas
    }

    pub fn headers(&self) -> &[String] {
        &self.headers
    }

    pub fn data_type(&self, header: &str) -> Option<&DataType> {
        self.type_map.get(header)
    }

    pub fn position_in_record(&self, header: &str, record: &Record) -> Option<&usize> {
        match self.position_map.get(&record.schema_idx()) {
            Some(position_map) => position_map.get(header),
            None => None,
        }
    }

    fn rebuild_cache(&mut self) {
        let mut headers = Vec::new();
        let mut type_map = HashMap::new();
        let mut position_map = HashMap::new();

        // Initialise the position map of maps.
        self.file_schemas
            .iter()
            .enumerate()
            .for_each(|(idx, _fsc)| { position_map.insert(idx, HashMap::new()); });

        // Cache all the projected columns. They start as the left-most column in the main grid.
        self.projected_columns
            .iter()
            .enumerate()
            .for_each(|(idx, pc)| {
                headers.push(pc.header.clone());
                type_map.insert(pc.header.clone(), pc.data_type);
                self.file_schemas
                    .iter()
                    .enumerate()
                    .for_each(|(sdx, fsc)| {
                        // Projected columns map to the right-most set of columns in the underlying Record/File schema.
                        position_map
                            .get_mut(&sdx)
                            .unwrap()
                            .insert(pc.header.clone(), fsc.columns().len() + idx);
                    });
            });

        // Cache all the merged columns. They follow immediately after projected columns in the main grid.
        self.merged_columns
            .iter()
            .enumerate()
            .for_each(|(idx, mc)| {
                headers.push(mc.header.clone());
                type_map.insert(mc.header.clone(), mc.data_type);
                self.file_schemas
                    .iter()
                    .enumerate()
                    .for_each(|(sdx, fsc)| {
                        // Merged columns map to the right-most set of columns (after projected) in the underlying Record/File schema.
                        position_map
                            .get_mut(&sdx)
                            .unwrap()
                            .insert(mc.header.clone(), fsc.columns().len() + self.projected_columns.len() + idx);
                    });
            });

        // Cache all the file schema columns. The first file forms the first set of columns, then the second file and so on.
        self.file_schemas
            .iter()
            .enumerate()
            .for_each(|(sdx, fsc)| {
                fsc.columns()
                    .iter()
                    .enumerate()
                    .for_each(|(cdx, col)| {
                        headers.push(col.header.clone());
                        type_map.insert(col.header.clone(), col.data_type);
                         // File schema columns map to the left-most set of columns in the underlying Record/File schema.
                         position_map
                            .get_mut(&sdx)
                            .unwrap()
                            .insert(col.header.clone(), cdx);
                    } );
            } );

        self.headers = headers;
        self.type_map = type_map;
        self.position_map = position_map;
    }
}

impl FileSchema {
    ///
    /// Build a hashmap of column header to parsed data-types. The data types should be on the first
    /// csv row after the headers.
    ///
    pub fn new(prefix: String, rdr: &mut csv::Reader<fs::File>) -> Result<Self, MatcherError> {
        let mut type_record = csv::StringRecord::new();

        if let Err(source) = rdr.read_record(&mut type_record) {
            return Err(MatcherError::NoSchemaRow { source })
        }

        let hdrs = rdr.headers()
            .map_err(|source| MatcherError::CannotReadHeaders { source })?;
        let mut columns = Vec::new();

        for (idx, hdr) in hdrs.iter().enumerate() {
            let data_type = match type_record.get(idx) {
                Some(raw_type) => {
                    let parsed = raw_type.into();
                    if parsed == DataType::UNKNOWN {
                        return Err(MatcherError::UnknownDataTypeInColumn { column: idx })
                    }
                    parsed
                },
                None => return Err(MatcherError::NoSchemaTypeForColumn { column: idx }),
            };

            let header = format!("{}.{}", prefix, hdr);
            columns.push(Column { header, data_type });
        }

        Ok(Self { prefix, columns })
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    // pub fn prefix(&self) -> &str {
    //     &self.prefix
    // }

    // pub fn headers(&self) -> &[String] {
    //     &self.headers
    // }

    // pub fn data_type(&self, header: &str) -> Option<DataType> {
    //     self.type_map.get(header).map(|dt|*dt)
    // }

    pub fn to_short_string(&self) -> String {
        self.columns
            .iter()
            .map(|col| col.data_type.to_str())
            .collect::<Vec<&str>>()
            .join(",")
    }
}