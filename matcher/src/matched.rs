use serde_json::json;
use std::{fs::File, io::{BufWriter, Write}};
use crate::{error::MatcherError, folders::{self, ToCanoncialString}, model::{grid::Grid, record::Record}, Context};

///
/// Manages the matched job file and appends matched groups to it.
///
pub struct MatchedHandler {
    groups: usize,
    path: String,
    writer: BufWriter<File>,
} // TODO: This bad-boy will be created at the start of the job and passed into this module as there may be multiple


impl MatchedHandler {
    ///
    /// Open a matched output file to write Json groups to. We'll add job details to the top of the file.
    ///
    pub fn new(ctx: &Context, grid: &Grid) -> Result<Self, MatcherError> {
        let path = folders::new_matched_file(ctx);
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        write!(&mut writer, "[\n")?;

        let job_header = json!(
        {
            "job_id": ctx.job_id().to_hyphenated().to_string(),
            "charter_name": ctx.charter().name(),
            "charter_version": ctx.charter().version(),
            "files": grid.files().iter().map(|f|f.original_filename()).collect::<Vec<&str>>()
        });

        if let Err(source) = serde_json::to_writer_pretty(&mut writer, &job_header) {
            return Err(MatcherError::FailedToWriteJobHeader { job_header: job_header.to_string(), path: path.to_canoncial_string(), source })
        }

        write!(&mut writer, ",\n{{\n  \"groups\": [\n    ")?;

        Ok(Self { groups: 0, writer, path: path.to_canoncial_string() })
    }

    ///
    /// Append the records in this group to the matched group file.
    ///
    /// Each group entry in the file is a 'file coordinate' to the original data. This is in the form: -
    /// [[n1,y1], [n2,y2], [n2,y3]]
    ///
    /// When n is a file index in the grid and y is the line number in the file for the record. Line numbers include
    /// the header rows (so the first line of data will start at 3).
    ///
    pub fn append_group(&mut self, records: &[&Box<Record>]) -> Result<(), MatcherError> {
        // Push this file writing into an fn.
        if self.groups !=  0 {
            write!(&mut self.writer, ",\n    ")
                .map_err(|source| MatcherError::CannotWriteThing { thing: "matched padding".into(), filename: self.path.clone(), source })?;
        }

        let json = records.iter().map(|r| json!(vec!(r.file_idx(), r.row()))).collect::<Vec<serde_json::Value>>();
        serde_json::to_writer(&mut self.writer, &json)
            .map_err(|source| MatcherError::CannotWriteMatchedRecord{ filename: self.path.clone(), source })?;

        self.groups += 1;

        Ok(())
    }

    ///
    /// Terminate the matched file to make it's contents valid JSON.
    ///
    pub fn complete_files(&mut self) -> Result<(), MatcherError> {
        // Remove the .inprogress suffix
        folders::complete_file(&self.path)?;

        Ok(write!(&mut self.writer, "]\n}}\n]\n")
            .map_err(|source| MatcherError::CannotWriteThing { thing: "matched terminator".into(), filename: self.path.clone(), source })?)

        // TODO: Completing a job should also log the unmatched files and counts) - this will be the 3rd object in the matched JSON array.
    }
}