use chrono::Utc;
use regex::Regex;
use lazy_static::lazy_static;
use crate::{datafile::DataFile, error::MatcherError};
use std::{fs::{self, DirEntry}, path::{Path, PathBuf}};

/*
    Files are processed alphabetically - hence the human-readable timestamp prefix - to ensure consistent ordering.

    $REC_HOME/waiting
    20200103_030405000_INV.csv.incomplete <<< External component is writing or moving the file here.
    20200102_030405000_INV.csv            <<< This file is waiting to be matched.

    $REC_HOME/matching
    20191210_020405000_INV.csv            <<< These files are being matched. Any files present here at start-up are re-processed.
    20191212_020405000_REC.csv            <<<
    20191214_020405000_PAY.csv            <<<
    20191209_020405000_INV.unmatched.csv  <<< Files in /unmatched are moved here at the start of a job. At the end of a job
                                          <<< .unmatched.csv files in this folder are deleted (their data will have been
                                          <<< written to a /matched json file or a new /unmatched file).

    $REC_HOME/unmatched
    20191210_020405000_INV.unmatched.csv.inprogress <<< This file is part of the in-progress match job and contains records which failed to match.
    20191212_020405000_REC.unmatched.csv.inprogress <<< These files only contain a subset of records from the original file(s).
    20191214_020405000_PAY.unmatched.csv.inprogress <<< After a match job has completed, the .inprogress is removed.
                                                    <<< Any .inprogress here at start-up under-go a rollback procedure.
                                                    <<< These file are moved to matching at the start of a job.

    $REC_HOME/matched
    20200201_093000000_JOB.json           <<< Match groups indexes generated from a match job.
                                          <<< Fields along these lines: GroupId, LogFile, Row, CharterId, CharterVersion
    20200202_093000000_JOB.json.inprogress

    $REC_HOME/archive
    20181209_020405000_INV.csv            <<< These files are the original files recieved without being modified in anyway.
    20181209_020405000_REC.csv            <<< i.e. files from MATCHING are moved to here when done.
    20181209_020405000_PAY.csv            <<< data from them is sifted into MATCHED/UNMATCHED files as well but the original
                                          <<< file is always preserved.

*/

// The root folder under which all data files are processed. In future this may become a mandatory command-line arg.
pub const REC_HOME: &str = "./tmp";
pub const IN_PROGRESS: &str = ".inprogress";
pub const UNMATCHED: &str = ".unmatched.csv";

lazy_static! {
    static ref FILENAME_REGEX: Regex = Regex::new(r"^(\d{8}_\d{9})_(.*)\.csv$").unwrap();
}

///
/// Ensure the folders exist to process files for this reconcilliation.
///
pub fn ensure_exist(debug: bool) -> Result<(), MatcherError> {
    let home = Path::new(REC_HOME);

    log::info!("Using folder REC_HOME [{}]", home.to_canoncial_string());

    let mut folders = vec!(waiting(), matching(), matched(), unmatched(), archive());
    if debug {
        folders.push(debug_path());
    }

    for folder in folders {
        fs::create_dir_all(&folder)
            .map_err(|source| MatcherError::CannotCreateDir { source, path: folder.to_canoncial_string() } )?;
    }

    Ok(())
}

///
/// Move any waiting files to the matching folder.
///
pub fn progress_to_matching() -> Result<(), MatcherError> {
    // TODO: Any unmatched files as well.
    for entry in waiting().read_dir()? {
        if let Ok(entry) = entry {
            if is_data_file(&entry) {
                let dest = matching().join(entry.file_name());

                log::info!("Moving file [{file}] from [{src}] to [{dest}]",
                    file = entry.file_name().to_string_lossy(),
                    src = entry.path().parent().unwrap().to_string_lossy(),
                    dest = dest.parent().unwrap().to_string_lossy());
                fs::rename(entry.path(), dest)?;
            }
        }
    }

    Ok(())
}

///
/// Move any matching files to the archive folder.
///
pub fn progress_to_archive() -> Result<(), MatcherError> {
    for entry in matching().read_dir()? {
        if let Ok(entry) = entry {
            if is_data_file(&entry) {
                let dest = archive().join(entry.file_name());

                log::info!("Moving file [{file}] from [{src}] to [{dest}]",
                    file = entry.file_name().to_string_lossy(),
                    src = entry.path().parent().unwrap().to_string_lossy(),
                    dest = dest.parent().unwrap().to_string_lossy());
                fs::rename(entry.path(), dest)?;
            }
            // TODO: Delete matching/ *.unmatched files.
        }
    }

    Ok(())
}

///
/// Return all the files in the matching folder which match the filename (wildcard) specified.
///
pub fn files_in_matching(filename: &str) -> Result<Vec<DirEntry>, MatcherError> {
    let wildcard = Regex::new(filename).map_err(|source| MatcherError::InvalidSourceFileRegEx { source })?;
    let mut files = vec!();
    for entry in matching().read_dir()? {
        if let Ok(entry) = entry {
            if is_data_file(&entry) && wildcard.is_match(&entry.file_name().to_string_lossy()) {
                files.push(entry);
            }
        }
    }

    // Ensure files are processed by sorted filename - i.e. chronologically.
    files.sort_by(|a,b| a.file_name().cmp(&b.file_name()));

    Ok(files)
}

///
/// Any .inprogress files should be deleted.
///
pub fn rollback_incomplete() -> Result<(), MatcherError> {
    for folder in vec!(matched(), unmatched()) {
        for entry in folder.read_dir()? {
            if let Ok(entry) = entry {
                if entry.file_name().to_string_lossy().ends_with(IN_PROGRESS) {
                    log::warn!("Rolling back file {}", entry.path().to_canoncial_string());
                    fs::remove_file(entry.path())?;
                }
            }
        }
    }

    Ok(())
}

///
/// Rename a file ending in .inprogress to remove the suffix.
///
pub fn complete_file(path: &str) -> Result<(), MatcherError> {
    if !path.ends_with(IN_PROGRESS) {
        return Err(MatcherError::FileNotInProgress { path: path.into() })
    }

    let from = Path::new(path);
    let to = Path::new(path.strip_suffix(IN_PROGRESS).unwrap(/* Check above makes this sage */));

    fs::rename(from, to)
        .map_err(|source| MatcherError::CannotRenameFile { from: from.to_canoncial_string(), to: to.to_canoncial_string(), source })?;

    log::debug!("Renaming {} -> {}", from.to_canoncial_string(), to.to_canoncial_string());
    Ok(())
}

pub fn delete_empty_unmatched(filename: &str) -> Result<(), MatcherError> {
    log::debug!("Deleting empty unmatched file {}", filename);
    let path = unmatched().join(filename);

    Ok(fs::remove_file(&path)
        .map_err(|source| MatcherError::CannotDeleteFile { filename: filename.into(), source })?)
}

pub fn waiting() -> PathBuf {
    Path::new(REC_HOME).join("waiting/")
}

pub fn matching() -> PathBuf {
    Path::new(REC_HOME).join("matching/")
}

pub fn matched() -> PathBuf {
    Path::new(REC_HOME).join("matched/")
}

pub fn unmatched() -> PathBuf {
    Path::new(REC_HOME).join("unmatched/")
}

pub fn archive() -> PathBuf {
    Path::new(REC_HOME).join("archive/")
}

pub fn debug_path() -> PathBuf {
    Path::new(REC_HOME).join("debug/")
}

pub fn new_matched_file() -> PathBuf {
    matched().join(format!("{}_matched.json{}", new_timestamp(), IN_PROGRESS))
}

///
/// e.g. 20201118_053000000_invoices.unmatched.csv
///
pub fn new_unmatched_file(file: &DataFile) -> PathBuf {
    unmatched().join(format!("{}_{}{}{}", file.timestamp(), file.shortname(), UNMATCHED, IN_PROGRESS))
}

///
/// Return a new timestamp in the file prefix format.
///
pub fn new_timestamp() -> String {
    Utc::now().format("%Y%m%d_%H%M%S%3f_").to_string()
}

///
/// Returns true if the file starts with a datetime prefix in the form 'YYYYMMDD_HHmmSSsss_' and a '.csv' suffix.
/// Logs a warning if we couldn't get a file's metadata and returns false.
///
fn is_data_file(entry: &DirEntry) -> bool {
    match entry.metadata() {
        Ok(metadata) => metadata.is_file() && FILENAME_REGEX.is_match(&entry.file_name().to_string_lossy()),
        Err(err) => {
            log::warn!("Skipping file, failed to get metadata for {}: {}", entry.path().to_canoncial_string(), err);
            false
        }
    }
}

///
/// Retrun the timestamp prefix from the filename.
///
pub fn timestamp<'a>(filename: &'a str) -> Result<&'a str, MatcherError> {
    match FILENAME_REGEX.captures(filename) {
        Some(captures) if captures.len() == 3 => Ok(captures.get(1).map(|ts|ts.as_str()).ok_or(MatcherError::InvalidTimestampPrefix{ filename: filename.into() })?),
        Some(_captures) => Err(MatcherError::InvalidTimestampPrefix{ filename: filename.into() }),
        None => Err(MatcherError::InvalidTimestampPrefix{ filename: filename.into() }),
    }
}

///
/// Remove the timestamp prefix and the file-extension suffix from the filename.
///
pub fn shortname<'a>(filename: &'a str) -> &'a str {
    match FILENAME_REGEX.captures(filename) {
        Some(captures) if captures.len() == 3 => captures.get(2).map_or(filename, |m| m.as_str()),
        Some(_captures) => filename,
        None => filename,
    }
}

///
/// Remove the timestamp prefix and the file-extension suffix from the filename.
///
pub fn entry_shortname<'a>(entry: &'a DirEntry) -> String {
    let filename: String = entry.file_name().to_string_lossy().into();
    shortname(&filename).into()
}

///
/// Return the filename part of the path.
///
/// e.g. $REC_HOME/unmatched/20191209_020405000_INV.unmatched.csv -> 20191209_020405000_INV.unmatched.csv
///
pub fn filename(pb: &PathBuf) -> Result<String, MatcherError> {
    match pb.file_name() {
        Some(os_str) => Ok(os_str.to_string_lossy().into()),
        None => Err(MatcherError::PathNotAFile { path: pb.to_canoncial_string() }),
    }
}

///
/// Returns a canonicalised path if possible, otherwise just the debug output.
///
pub trait ToCanoncialString: std::fmt::Debug {
    fn to_canoncial_string(&self) -> String;
}

impl ToCanoncialString for Path {
    fn to_canoncial_string(&self) -> String {
        match self.canonicalize() {
            Ok(path) => path.to_string_lossy().into(),
            Err(_) => self.to_string_lossy().into(),
        }
    }
}