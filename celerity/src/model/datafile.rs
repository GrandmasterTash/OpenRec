use crate::folders;
use std::{fs::DirEntry, path::PathBuf};

///
/// Represents a physical sourced file of data that has been 'loaded' into the grid.
///
/// This could be an incoming data file or a pre-existing unmatched data file from a previous job.
///
/// It is a bit like the folders module on steroids and provides all the different path and folder representions for
/// a single file.
///
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DataFile {
    path: PathBuf,
    derived_path: PathBuf,
    modifying_path: PathBuf,
    pre_modified_path: PathBuf,
    filename: String,
    derived_filename: String,
    archived_filename: Option<String>,
    schema_idx: usize,
}

impl DataFile {
    pub fn new(entry: &DirEntry, schema_idx: usize) -> Self {
        let pb = entry.path();
        let derived_path = folders::derived(&pb);

        Self {
            path: pb.clone(),
            filename: entry.file_name().to_string_lossy().into(),
            derived_filename: derived_path.file_name().expect("no dervied filename").to_string_lossy().into(),
            modifying_path: folders::modifying(&pb),
            pre_modified_path: folders::pre_modified(&pb),
            archived_filename: None,
            derived_path,
            schema_idx,
        }
    }

    ///
    /// This index of the FileSchema in the grid that this file uses.
    ///
    pub fn schema_idx(&self) -> usize {
        self.schema_idx
    }

    ///
    /// 'invoices' if filename is '/tmp/20201118_053000000_invoices.csv' or '/tmp/20201118_053000000_invoices.unmatched.csv'
    ///
    pub fn shortname(&self) -> &str {
        folders::shortname(self.filename())
    }

    ///
    /// '20201118_053000000' if the filename is '20201118_053000000_invoices.csv'.
    ///
    pub fn timestamp(&self) -> &str {
        folders::timestamp(self.filename()).expect("timestamp not correct")
    }

    ///
    /// The full canonical path to the data or unmatched file.
    ///
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    ///
    /// The path to any temporary file storing derived data, eg. '/tmp/20201118_053000000_invoices.derived.csv'
    ///
    pub fn derived_path(&self) -> &PathBuf {
        &self.derived_path
    }

    ///
    /// '20201118_053000000_invoices.csv' if path is '/tmp/20201118_053000000_invoices.csv'
    ///
    pub fn filename(&self) -> &str {
        &self.filename
    }

    ///
    /// '20201118_053000000_invoices.derived.csv'
    ///
    pub fn derived_filename(&self) -> &str {
        &self.derived_filename
    }

    ///
    /// The path to any temporary file storing modified data, eg. '/tmp/20201118_053000000_invoices.csv.modifying'
    ///
    pub fn modifying_path(&self) -> &PathBuf {
        &self.modifying_path
    }

    ///
    /// The path to an unmatched file before a changeset is applied, eg. '/tmp/20201118_053000000_invoices.unmatched.csv.pre_modified'
    ///
    pub fn pre_modified_path(&self) -> &PathBuf {
        &self.pre_modified_path
    }

    ///
    /// The name of the archived file - only set for datafiles when they are first archived.
    ///
    pub fn archived_filename(&self) -> &Option<String> {
        &self.archived_filename
    }

    ///
    /// Record that this file has been archived and where that is (as the filename may change as part of the
    /// archive process).
    ///
    pub fn set_archived_filename(&mut self, archived_filename: String) {
        self.archived_filename = Some(archived_filename);
    }
}