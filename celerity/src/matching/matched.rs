use itertools::Itertools;
use positioned_io::WriteAt;
use serde_json::{json, Value};
use super::unmatched::UnmatchedHandler;
use std::{fs::{File, OpenOptions}, io::{BufWriter, Write}};
use crate::{error::MatcherError, folders::{self, ToCanoncialString}, model::{grid::Grid, record::Record}, Context, changeset::{ChangeSet, Change}};

///
/// Manages the matched job file and appends matched groups to it.
///
pub struct MatchedHandler {
    groups: usize,
    path: String,
    writer: BufWriter<File>, // For the matched.json file.
    data_writers: Vec<File>, // To update the status byte for matched records.
}

impl MatchedHandler {
    ///
    /// Open a matched output file to write Json groups to. We'll add job details to the top of the file.
    ///
    pub fn new(ctx: &Context, grid: &Grid) -> Result<Self, MatcherError> {

        // Initialise the matched.json file.
        let path = folders::new_matched_file(ctx);
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        writeln!(&mut writer, "[")?;

        let job_header = json!(
        {
            "job_id": ctx.job_id().to_hyphenated().to_string(),
            "charter": {
                "name": ctx.charter().name(),
                "version": ctx.charter().version(),
                "file": ctx.charter_path()
            },
            "files": grid.schema()
                .files()
                .iter()
                .sorted_by(|f1, f2| Ord::cmp(&f1.filename(), &f2.filename()))
                .map(|f|f.filename())
                .collect::<Vec<&str>>()
        });

        if let Err(source) = serde_json::to_writer_pretty(&mut writer, &job_header) {
            return Err(MatcherError::FailedToWriteJobHeader { job_header: job_header.to_string(), path: path.to_canoncial_string(), source })
        }

        write!(&mut writer, ",\n{{\n  \"groups\": [\n    ")?;

        Ok(Self {
            groups: 0,
            writer,
            path: path.to_canoncial_string(),
            data_writers: grid.schema().files()
                .iter()
                .map(|df| OpenOptions::new().write(true).open(df.path()).unwrap())
                .collect()
        })
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
    pub fn append_group(&mut self, records: &[&Record]) -> Result<(), MatcherError> {
        // Mark all records as matched in thier source files.
        self.set_matched_status(records)?;

        // Update the matched.json file.
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
    pub fn complete_files(&mut self, unmatched: &UnmatchedHandler, changesets: Vec<ChangeSet>) -> Result<(), MatcherError> {

        // Terminate the groups object.
        write!(&mut self.writer, "]\n}},\n")
            .map_err(|source| MatcherError::CannotWriteThing { thing: "matched groups terminator".into(), filename: self.path.clone(), source })?;

        let footer = json!(
        {
            "unmatched": summerise_unmatched(unmatched),
            "changesets": summerise_changesets(changesets),
        });

        // Write the unmatched count and changeset metrics.
        serde_json::to_writer_pretty(&mut self.writer, &footer)
            .map_err(|source| MatcherError::CannotWriteFooter { filename: self.path.clone(), source })?;

        // Terminate the root array.
        write!(&mut self.writer, "]\n")
            .map_err(|source| MatcherError::CannotWriteThing { thing: "matched file terminator".into(), filename: self.path.clone(), source })?;

        // Remove the .inprogress suffix
        folders::complete_file(&self.path)?;

        Ok(())
    }

    ///
    /// Writer a '1' to the first column of each matched record.
    ///
    pub fn set_matched_status(&mut self, records: &[&Record]) -> Result<(), MatcherError> {
        let buf = vec!(0x31); // = 1 = Matched

        for record in records {
            let file = &mut self.data_writers[record.file_idx()];
            file.write_all_at(record.data_position().byte() +/* Skip double-quotes */ 1, &buf).unwrap();
        }

        Ok(())
    }
}

///
/// List each remaining unmatched file and how many records it contains.
///
fn summerise_unmatched(unmatched: &UnmatchedHandler) -> Vec<Value> {
    unmatched.unmatched_files()
        .iter()
        .filter(|uf| uf.rows() > 0)
        .sorted_by(|uf1, uf2| Ord::cmp(&uf1.filename(), &uf2.filename()))
        .map(|uf| json!({
            "file": uf.filename(),
            "rows": uf.rows()
        }) )
        .collect()
}

///
/// List each changeset file that was present for the match job and summerise the count of effected records
/// for each file.
///
fn summerise_changesets(changesets: Vec<ChangeSet>) -> Vec<Value> {

    let mut json = vec!();

    for group in &changesets
        .iter()
        .sorted_by(|cs1, cs2| Ord::cmp(cs1.filename(), cs2.filename()))
        .group_by(|cs| cs.filename().to_string() ) {

        let (updated, ignored): (Vec<&ChangeSet>, Vec<&ChangeSet>) = group.1.partition(|cs| {
            match cs.change() {
                Change::UpdateFields { .. }  => true,
                Change::IgnoreRecords { .. } => false,
            }
        });

        json.push(json!(
        {
            "file": &group.0,
            "updated": updated.iter().map(|cs| cs.effected()).sum::<usize>(),
            "ignored": ignored.iter().map(|cs| cs.effected()).sum::<usize>()
        }));
    }

    json
}