use chrono::{Utc, TimeZone};
use regex::Regex;
use lazy_static::lazy_static;
use std::{fs::{self, DirEntry}, path::{Path, PathBuf}};
use crate::{model::{datafile::DataFile, grid::Grid}, error::MatcherError, Context};

/*
    Files are processed alphabetically - hence the human-readable timestamp prefix - to ensure consistent ordering.
    Note: There are some additional files involved with changesets. These are documented in the changeset module.

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
pub const IN_PROGRESS: &str = ".inprogress";
pub const UNMATCHED: &str = ".unmatched.csv";
pub const DERIVED: &str = "derived.csv";
pub const MODIFYING: &str = "modifying";
pub const PRE_MODIFIED: &str = "pre_modified";
const CHANGESET_PATTERN: &str = r"^(\d{8}_\d{9})_changeset\.json$";

lazy_static! {
    static ref FILENAME_REGEX: Regex = Regex::new(r"^(\d{8}_\d{9})_(.*)\.csv$").unwrap();
    static ref SHORTNAME_REGEX: Regex = Regex::new(r"^(\d{8}_\d{9})_(.*?)(\.unmatched)*\.csv$").unwrap();
    static ref DERIVED_REGEX: Regex = Regex::new(r"^(\d{8}_\d{9})_(.*)\.derived\.csv$").unwrap();
    static ref CHANGESET_REGEX: Regex = Regex::new(CHANGESET_PATTERN).unwrap();
    static ref TIMESTAMP_REGEX: Regex = Regex::new(r"^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})(\d{3})").unwrap();
    pub static ref UNMATCHED_REGEX: Regex = Regex::new(r"^(\d{8}_\d{9})_(.*)\.unmatched\.csv$").unwrap();
}

///
/// Rename a folder or file - captures the paths to log if fails.
///
pub fn rename(from: &str, to: &str) -> Result<(), MatcherError> {
    Ok(fs::rename(from, to)
        .map_err(|source| MatcherError::CannotRenameFile { from: from.into(), to: to.into(), source })?)
}

///
/// Remove the file specified - captures the path to log if fails.
///
pub fn remove_file(filename: &str) -> Result<(), MatcherError> {
    Ok(fs::remove_file(filename)
        .map_err(|source| MatcherError::CannotDeleteFile { source, filename: filename.into() })?)
}

///
/// Ensure the folders exist to process files for this reconcilliation.
///
pub fn ensure_dirs_exist(ctx: &Context) -> Result<(), MatcherError> {
    let home = Path::new(ctx.base_dir());

    log::debug!("Creating folder structure in [{}]", home.to_canoncial_string());

    let mut folders = vec!(waiting(ctx), matching(ctx), matched(ctx), unmatched(ctx), archive(ctx));
    if ctx.charter().debug() {
        folders.push(debug_path(ctx));
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
pub fn progress_to_matching(ctx: &Context) -> Result<(), MatcherError> {
    // Move files from the unmatched folder to the matching folder.
    for entry in unmatched(ctx).read_dir()? {
        if let Ok(entry) = entry {
            if is_unmatched_data_file(&entry) {
                let dest = matching(ctx).join(entry.file_name());

                log::debug!("Moving file [{file}] from [{src}] to [{dest}]",
                    file = entry.file_name().to_string_lossy(),
                    src = entry.path().parent().unwrap().to_string_lossy(),
                    dest = dest.parent().unwrap().to_string_lossy());
                fs::rename(entry.path(), dest)?;
            }
        }
    }

    // Move waiting files to the matching folder.
    for entry in waiting(ctx).read_dir()? {
        if let Ok(entry) = entry {
            if is_data_file(&entry) || is_changeset_file(&entry) {
                let dest = matching(ctx).join(entry.file_name());

                log::debug!("Moving file [{file}] from [{src}] to [{dest}]",
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
pub fn progress_to_archive(ctx: &Context, grid: Grid) -> Result<(), MatcherError> {
    for entry in matching(ctx).read_dir()? {
        if let Ok(entry) = entry {
            if is_unmatched_data_file(&entry) {
                // Delete .unmatched files don't move them to archive. At the end of a match job,
                // their contents will have been written to a new unmatched file in the unmatched folder.
                fs::remove_file(entry.path())?;
                log::debug!("Deleted unmatched file [{}]", entry.path().to_string_lossy())

            } else if is_derived_file(&entry) {
                // Delete .derived files don't move them to archive.
                fs::remove_file(entry.path())?;
                log::debug!("Deleted derived file [{}]", entry.path().to_string_lossy())

            } else if is_data_file(&entry) && was_processed(&entry, &grid) {
                let dest = archive(ctx).join(entry.file_name());

                // Ensure we don't blat existing files. This can happen if a changeset has been
                // applied to a file. There will be a non-modified version of it already present.
                if dest.exists() {
                    log::debug!("Removing file [{file}] from [{src}] - archive version already exists",
                        file = entry.file_name().to_string_lossy(),
                        src = entry.path().parent().unwrap().to_string_lossy());
                    fs::remove_file(entry.path())?;

                } else {
                    log::debug!("Moving file [{file}] from [{src}] to [{dest}]",
                        file = entry.file_name().to_string_lossy(),
                        src = entry.path().parent().unwrap().to_string_lossy(),
                        dest = dest.parent().unwrap().to_string_lossy());
                    fs::rename(entry.path(), dest)?;
                }
            }
        }
    }

    Ok(())
}

///
/// Move the specified file to the matched folder immediately.
///
pub fn progress_to_matched_now(ctx: &Context, entry: &DirEntry) -> Result<(), MatcherError> {
    let dest = matched(ctx).join(entry.file_name());

    log::debug!("Moving file [{file}] from [{src}] to [{dest}]",
        file = entry.file_name().to_string_lossy(),
        src = entry.path().parent().unwrap().to_string_lossy(),
        dest = dest.parent().unwrap().to_string_lossy());

    fs::rename(entry.path(), dest)?;

    Ok(())
}

///
/// Move the specified file to archive.
///
pub fn archive_immediately(ctx: &Context, path: &str) -> Result<(), MatcherError> {

    let p = Path::new(path);

    if !p.is_file() {
        return Err(MatcherError::PathNotAFile { path: path.into() })
    }

    let filename = match p.file_name() {
        Some(filename) => filename,
        None => return Err(MatcherError::PathNotAFile { path: path.into() }),
    };

    let dest = archive(ctx).join(filename);

    log::debug!("Moving file [{file}] from [{src}] to [{dest}]",
        file = filename.to_string_lossy(),
        src = p.parent().unwrap().to_string_lossy(),
        dest = dest.parent().unwrap().to_string_lossy());

    fs::rename(p, dest)?;

    Ok(())
}

///
/// Return all the files in the matching folder which match the filename (wildcard) specified.
///
pub fn files_in_matching(ctx: &Context, file_pattern: &str) -> Result<Vec<DirEntry>, MatcherError> {
    let wildcard = Regex::new(file_pattern).map_err(|source| MatcherError::InvalidSourceFileRegEx { source })?;
    let mut files = vec!();
    for entry in matching(ctx).read_dir()? {
        if let Ok(entry) = entry {
            if (is_data_file(&entry) || is_changeset_file(&entry)) && wildcard.is_match(&entry.file_name().to_string_lossy()) {
                files.push(entry);
            }
        }
    }

    // Ensure files are processed by sorted filename - i.e. chronologically.
    files.sort_by(|a,b| a.file_name().cmp(&b.file_name()));

    Ok(files)
}

///
/// Return all the changeset files in the matching folder.
///
pub fn changesets_in_matching(ctx: &Context) -> Result<Vec<DirEntry>, MatcherError> {
    files_in_matching(ctx, CHANGESET_PATTERN)
}

///
/// Any .inprogress files should be deleted.
///
pub fn rollback_any_incomplete(ctx: &Context) -> Result<(), MatcherError> {
    // TODO: Any index.* files.
    
    for folder in vec!(matched(ctx), unmatched(ctx)) {
        for entry in folder.read_dir()? {
            if let Ok(entry) = entry {
                if entry.file_name().to_string_lossy().ends_with(IN_PROGRESS) {
                    log::warn!("Rolling back file {}", entry.path().to_canoncial_string());
                    fs::remove_file(entry.path())?;
                }
            }
        }
    }

    for folder in vec!(matching(ctx)) {
        for entry in folder.read_dir()? {
            if let Ok(entry) = entry {
                if entry.file_name().to_string_lossy().ends_with(MODIFYING)
                    || entry.file_name().to_string_lossy().ends_with(DERIVED) {
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
pub fn complete_file(path: &str) -> Result<PathBuf, MatcherError> {
    if !path.ends_with(IN_PROGRESS) {
        return Err(MatcherError::FileNotInProgress { path: path.into() })
    }

    let from = Path::new(path);
    let to = Path::new(path.strip_suffix(IN_PROGRESS).unwrap(/* Check above makes this safe */));

    fs::rename(from, to)
        .map_err(|source| MatcherError::CannotRenameFile { from: from.to_canoncial_string(), to: to.to_canoncial_string(), source })?;

    log::debug!("Renaming {} -> {}", from.to_canoncial_string(), to.to_canoncial_string());
    Ok(to.to_path_buf())
}

pub fn delete_empty_unmatched(ctx: &Context, filename: &str) -> Result<(), MatcherError> {
    log::debug!("Deleting empty unmatched file {}", filename);
    let path = unmatched(ctx).join(filename);

    Ok(fs::remove_file(&path)
        .map_err(|source| MatcherError::CannotDeleteFile { filename: filename.into(), source })?)
}

pub fn waiting(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("waiting/")
}

pub fn matching(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("matching/")
}

pub fn matched(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("matched/")
}

pub fn unmatched(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("unmatched/")
}

pub fn archive(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("archive/")
}

pub fn debug_path(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("debug/")
}

pub fn new_matched_file(ctx: &Context) -> PathBuf {
    matched(ctx).join(format!("{}_matched.json{}", new_timestamp(), IN_PROGRESS))
}

///
/// e.g. 20201118_053000000_invoices.unmatched.csv.inprogress
///
pub fn new_unmatched_file(ctx: &Context, file: &DataFile) -> PathBuf {
    unmatched(ctx).join(format!("{}_{}{}{}", file.timestamp(), file.shortname(), UNMATCHED, IN_PROGRESS))
}

///
/// Return a new timestamp in the file prefix format.
///
pub fn new_timestamp() -> String {

    // This behaviour can be overriden by the tests.
    if let Ok(ts) = std::env::var("OPENREC_FIXED_TS") {
        return ts
    }

    Utc::now().format("%Y%m%d_%H%M%S%3f").to_string()
}

///
/// Returns true if the file starts with a datetime prefix in the form 'YYYYMMDD_HHmmSSsss_' and ends with
/// a '.csv' suffix.
///
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
/// Returns true if the file starts with a datetime prefix in the form 'YYYYMMDD_HHmmSSsss_' and ends with
/// a '.unmatched.csv' suffix.
///
/// Logs a warning if we couldn't get a file's metadata and returns false.
///
fn is_unmatched_data_file(entry: &DirEntry) -> bool {
    match entry.metadata() {
        Ok(metadata) => metadata.is_file() && UNMATCHED_REGEX.is_match(&entry.file_name().to_string_lossy()),
        Err(err) => {
            log::warn!("Skipping unmatched file, failed to get metadata for {}: {}", entry.path().to_canoncial_string(), err);
            false
        }
    }
}

///
/// Returns true if the file starts with a datetime prefix in the form 'YYYYMMDD_HHmmSSsss_' and ends with
/// a '.derived.csv' suffix.
///
/// Logs a warning if we couldn't get a file's metadata and returns false.
///
fn is_derived_file(entry: &DirEntry) -> bool {
    match entry.metadata() {
        Ok(metadata) => metadata.is_file() && DERIVED_REGEX.is_match(&entry.file_name().to_string_lossy()),
        Err(err) => {
            log::warn!("Skipping derived file, failed to get metadata for {}: {}", entry.path().to_canoncial_string(), err);
            false
        }
    }
}

///
/// Returns true if the file matches the changeset filename pattern.
///
/// Logs a warning if we couldn't get a file's metadata and returns false.
///
fn is_changeset_file(entry: &DirEntry) -> bool {
    match entry.metadata() {
        Ok(metadata) => metadata.is_file() && CHANGESET_REGEX.is_match(&entry.file_name().to_string_lossy()),
        Err(err) => {
            log::warn!("Skipping file, failed to get metadata for {}: {}", entry.path().to_canoncial_string(), err);
            false
        }
    }
}

///
/// True if the file is a data-file in the grid (and has therefore been 'sourced').
///
fn was_processed(entry: &DirEntry, grid: &Grid) -> bool {
    grid.schema()
        .files()
        .iter()
        .map(|df| df.path())
        .any(|path| path == entry.path().to_canoncial_string())
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
/// Parse the YYYYMMDD_HHSSMMIII file prefix into a unix epoch timestamp.
///
pub fn unix_timestamp(file_timestamp: &str) -> Option<i64> {
    match TIMESTAMP_REGEX.captures(file_timestamp) {
        Some(captures) if captures.len() == 8 => {
            Some(Utc
                .ymd(
                    captures.get(1).expect("No years").as_str().parse::<i32>().expect("year not numeric"),
                    captures.get(2).expect("No months").as_str().parse::<u32>().expect("month not numeric"),
                    captures.get(3).expect("No days").as_str().parse::<u32>().expect("day not numeric"))
                .and_hms_milli(
                    captures.get(4).expect("No hours").as_str().parse::<u32>().expect("hour not numeric"),
                    captures.get(5).expect("No minutes").as_str().parse::<u32>().expect("minute not numeric"),
                    captures.get(6).expect("No seconds").as_str().parse::<u32>().expect("second not numeric"),
                    captures.get(7).expect("No millis").as_str().parse::<u32>().expect("milli not numeric"))
                .timestamp_millis())
        },
        Some(_captures) => None,
        None => None,
    }
}

///
/// Remove the timestamp prefix and the file-extension suffix from the filename.
///
/// e.g. 20191209_020405000_INV.unmatched.csv -> INV
///
pub fn shortname<'a>(filename: &'a str) -> &'a str {
    match SHORTNAME_REGEX.captures(filename) {
        Some(captures) if captures.len() == 4 => captures.get(2).map_or(filename, |m| m.as_str()),
        Some(_captures) => filename,
        None => filename,
    }
}

///
/// Returns the original filename for a file, regardless of whether it's unmatched or not.
///
/// e.g. 20201118_053000000_invoices.unmatched.csv -> 20201118_053000000_invoices.csv
///
pub fn original_filename(filename: &str) -> Result<String, MatcherError> {
    let shortname = shortname(&filename).to_string();
    let timestamp = timestamp(&filename)?.to_string();
    Ok(format!("{}_{}.csv", timestamp, shortname))
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
/// Take the path to a data file and return a path to a derived data file from it.
///
/// e.g. $REC_HOME/unmatched/20191209_020405000_INV.csv
///    -> $REC_HOME/unmatched/20191209_020405000_INV.csv.derived.csv
///
pub fn derived(entry: &DirEntry) -> Result<PathBuf, MatcherError> {

    if is_data_file(entry) || is_unmatched_data_file(entry) {
        return Ok(entry.path().with_extension(DERIVED))
    }

    Err(MatcherError::FileCantBeDerived { path: entry.path().to_canoncial_string() })
}

///
/// Take the path to a data file and return a path to a modifying data file from it.
///
/// e.g. $REC_HOME/unmatched/20191209_020405000_INV.unmatched.csv.
///    -> $REC_HOME/unmatched/20191209_020405000_INV.unmatched.csv.modifying
///
/// e.g. $REC_HOME/unmatched/20191209_020405000_INV.csv
///    -> $REC_HOME/unmatched/20191209_020405000_INV.csv.modifyng
///
pub fn modifying(entry: &DirEntry) -> Result<PathBuf, MatcherError> {

    if is_data_file(entry) || is_unmatched_data_file(entry) {
        return Ok(entry.path().with_extension(MODIFYING))
    }

    Err(MatcherError::FileCantBeDerived { path: entry.path().to_canoncial_string() })
}

///
/// Take the path to a data file and return a path to a modified version of it.
///
/// e.g. $REC_HOME/unmatched/20191209_020405000_INV.unmatched.csv.
///    -> $REC_HOME/unmatched/20191209_020405000_INV.unmatched.csv.pre_modified
///
pub fn pre_modified(entry: &DirEntry) -> PathBuf {
    entry.path().with_extension(PRE_MODIFIED)
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