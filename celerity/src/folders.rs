use regex::Regex;
use chrono::{Utc, TimeZone};
use lazy_static::lazy_static;
use anyhow::Context as ErrContext;
use std::{fs::{self, DirEntry}, path::{Path, PathBuf}};
use crate::{model::{datafile::DataFile, grid::Grid}, error::{MatcherError, here}, Context};

///
/// This module provides file and folder util methods.
///

pub const IN_PROGRESS: &str = ".inprogress";
pub const UNMATCHED: &str = ".unmatched.csv";
pub const DERIVED: &str = "derived.csv";
pub const MODIFYING: &str = "modifying";
pub const PRE_MODIFIED: &str = "pre_modified";
const CHANGESET_PATTERN: &str = r"^(\d{8}_\d{9})_changeset\.json$";

lazy_static! {
    static ref FILENAME_REGEX: Regex = Regex::new(r"^(\d{8}_\d{9})_(.*)\.csv$").expect("bad regex for FILENAME_REGEX");
    static ref SHORTNAME_REGEX: Regex = Regex::new(r"^(\d{8}_\d{9})_(.*?)(\.unmatched)*\.csv$").expect("bad regex for SHORTNAME_REGEX");
    static ref DERIVED_REGEX: Regex = Regex::new(r"^(\d{8}_\d{9})_(.*)\.derived\.csv$").expect("bad regex for DERIVED_REGEX");
    static ref CHANGESET_REGEX: Regex = Regex::new(CHANGESET_PATTERN).expect("bad regex for CHANGESET_REGEX");
    static ref TIMESTAMP_REGEX: Regex = Regex::new(r"^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})(\d{3})").expect("bad regex for TIMESTAMP_REGEX");
    pub static ref UNMATCHED_REGEX: Regex = Regex::new(r"^(\d{8}_\d{9})_(.*)\.unmatched\.csv$").expect("bad regex for UNMATCHED_REGEX");
}

///
/// Rename a folder or file - captures the paths to log if fails.
///
pub fn rename<F, T>(from: F, to: T) -> Result<(), MatcherError>
where
    F: AsRef<Path>,
    T: AsRef<Path>
{
    let f_str = from.as_ref().to_canoncial_string();
    let t_str = to.as_ref().to_canoncial_string();

    log::debug!("Moving/renaming {} -> {}", f_str, t_str);

    Ok(fs::rename(from, to)
        .with_context(|| format!("Cannot rename {} to {}{}", f_str, t_str, here!()))?)
}

///
/// Remove the file specified - captures the path to log if fails.
///
pub fn remove_file<P>(filename: P) -> Result<(), MatcherError>
where
    P: AsRef<Path>
{
    let f_str = filename.as_ref().to_canoncial_string();

    log::debug!("Removing file {}", f_str);

    Ok(fs::remove_file(filename)
        .with_context(|| format!("Cannot remove file {}{}", f_str, here!()))?)
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
            .with_context(|| format!("Unable to create directory {}{}", folder.to_canoncial_string(), here!()))?;
    }

    Ok(())
}

///
/// Move any waiting files to the matching folder.
///
pub fn progress_to_matching(ctx: &Context) -> Result<(), MatcherError> {
    // Move files from the unmatched folder to the matching folder.
    for entry in (unmatched(ctx).read_dir()?).flatten() { // Result is an iterator, so flatten if only interested in Ok values.
        if is_unmatched_data_file(&entry.path()) {
            let dest = matching(ctx).join(entry.file_name());
            rename(entry.path(), dest)?
        }
    }

    // Move waiting files to the matching folder.
    for entry in (waiting(ctx).read_dir()?).flatten() {
        let pb = entry.path();
        if is_data_file(&pb) || is_changeset_file(&pb) {
            let dest = matching(ctx).join(entry.file_name());
            rename(entry.path(), dest)?
        }
    }

    Ok(())
}

///
/// Move any matching files to the archive folder, remove derived data and old unmatched data.
///
pub fn progress_to_archive(ctx: &Context, mut grid: Grid) -> Result<(), MatcherError> {
    for entry in (matching(ctx).read_dir()?).flatten() {
        let pb = entry.path();

        if is_unmatched_data_file(&pb) || is_derived_file(&pb) {
            // Delete .unmatched files don't move them to archive. At the end of a match job,
            // their still-unmatched contents will have been written to a new unmatched file in
            // the unmatched folder.
            //   also
            // Delete .derived files don't move them to archive.
            remove_file(entry.path())?;

        } else if is_data_file(&pb) {
            // If the data file is in the grid, get a mut and archive it.
            if let Some(data_file) = grid.schema_mut().files_mut()
                .find(|df| df.path().to_canoncial_string() == entry.path().to_canoncial_string()) {

                archive_data_file(ctx, data_file)?;
            }
        }
    }

    Ok(())
}


///
/// Move the specified file to the archive folder immediately.
///
pub fn progress_to_archive_now(ctx: &Context, entry: &DirEntry) -> Result<(), MatcherError> {
    let dest = archive(ctx).join(entry.file_name());
    rename(entry.path(), dest)
}


///
/// Archive the data file ensuring it's archive filename is unique and recorded.
///
pub fn archive_data_file(ctx: &Context, file: &mut DataFile) -> Result<(), MatcherError> {

    if file.archived_filename().is_none() {
        let mut counter = 0;
        let mut dest = archive(ctx).join(file.filename());

        while dest.exists() {
            counter += 1;
            dest = archive(ctx).join(format!("{}_{:02}", file.filename(), counter));
        }

        rename(file.path(), &dest)?;
        file.set_archived_filename(dest.file_name().expect("no archive filename").to_string_lossy().into());
    }

    Ok(())
}

///
/// Return all the files in the matching folder which match the filename (wildcard) specified.
///
pub fn files_in_matching(ctx: &Context, file_pattern: &str) -> Result<Vec<DirEntry>, MatcherError> {
    let wildcard = Regex::new(file_pattern).map_err(|source| MatcherError::InvalidSourceFileRegEx { source })?;
    let mut files = vec!();
    for entry in (matching(ctx).read_dir()?).flatten() {
        let pb = entry.path();
        if (is_data_file(&pb) || is_changeset_file(&pb)) && wildcard.is_match(&entry.file_name().to_string_lossy()) {
            files.push(entry);
        }
    }

    // Ensure files are processed by sorted filename - i.e. chronologically.
    files.sort_by_key(|a| a.file_name());

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

    for folder in vec!(matched(ctx), unmatched(ctx)) {
        for entry in (folder.read_dir()?).flatten() {
            if entry.file_name().to_string_lossy().ends_with(IN_PROGRESS) {
                log::warn!("Rolling back file {}", entry.path().to_canoncial_string());
                fs::remove_file(entry.path())?;
            }
        }
    }

    for folder in vec!(matching(ctx)) {
        for entry in (folder.read_dir()?).flatten() {
            let filename = entry.file_name().to_string_lossy().to_string();

            if filename.ends_with(MODIFYING)
                || filename.ends_with(DERIVED)
                || filename.starts_with("index.") {
                log::warn!("Rolling back file {}", entry.path().to_canoncial_string());
                fs::remove_file(entry.path())?;
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

    rename(from, to)?;
    Ok(to.to_path_buf())
}


pub fn delete_empty_unmatched(ctx: &Context, filename: &str) -> Result<(), MatcherError> {
    let path = unmatched(ctx).join(filename);
    remove_file(&path)
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
    Path::new(ctx.base_dir()).join("archive/celerity")
}

pub fn lookups(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("lookups/")
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
/// The path to the unsorted index file.
///
pub fn unsorted_index(ctx: &Context) -> PathBuf {
    matching(ctx).join("index.unsorted.csv")
}

///
/// Returns true if the file starts with a datetime prefix in the form 'YYYYMMDD_HHmmSSsss_' and ends with
/// a '.csv' suffix.
///
fn is_data_file(path: &Path) -> bool {
    path.is_file() && FILENAME_REGEX.is_match(&path.file_name().unwrap_or_default().to_string_lossy())
}

///
/// Returns true if the file starts with a datetime prefix in the form 'YYYYMMDD_HHmmSSsss_' and ends with
/// a '.unmatched.csv' suffix.
///
fn is_unmatched_data_file(path: &Path) -> bool {
    path.is_file() && UNMATCHED_REGEX.is_match(&path.file_name().unwrap_or_default().to_string_lossy())
}

///
/// Returns true if the file starts with a datetime prefix in the form 'YYYYMMDD_HHmmSSsss_' and ends with
/// a '.derived.csv' suffix.
///
fn is_derived_file(path: &Path) -> bool {
    path.is_file() && DERIVED_REGEX.is_match(&path.file_name().unwrap_or_default().to_string_lossy())
}

///
/// Returns true if the file matches the changeset filename pattern.
///
fn is_changeset_file(path: &Path) -> bool {
    path.is_file() && CHANGESET_REGEX.is_match(&path.file_name().unwrap_or_default().to_string_lossy())
}

///
/// Retrun the timestamp prefix from the filename.
///
pub fn timestamp(filename: &'_ str) -> Result<&'_ str, MatcherError> {
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
pub fn shortname(filename: &'_ str) -> &'_ str {
    match SHORTNAME_REGEX.captures(filename) {
        Some(captures) if captures.len() == 4 => captures.get(2).map_or(filename, |m| m.as_str()),
        Some(_captures) => filename,
        None => filename,
    }
}

///
/// Return the filename part of the path.
///
/// e.g. $REC_HOME/unmatched/20191209_020405000_INV.unmatched.csv -> 20191209_020405000_INV.unmatched.csv
///
pub fn filename(pb: &Path) -> String {
    match pb.file_name() {
        Some(os_str) => os_str.to_string_lossy().into(),
        None => panic!("{} is not a file/has no filename", pb.to_canoncial_string()),
    }
}

///
/// Take the path to a data file and return a path to a derived data file from it.
///
/// e.g. $REC_HOME/unmatched/20191209_020405000_INV.csv
///    -> $REC_HOME/unmatched/20191209_020405000_INV.csv.derived.csv
///
pub fn derived(path: &PathBuf) -> PathBuf {

    if is_data_file(path) || is_unmatched_data_file(path) {
        return path.with_extension(DERIVED)
    }

    panic!("Cannot create a derived file for {}", path.to_canoncial_string())
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
pub fn modifying(path: &Path) -> PathBuf {

    if is_data_file(path) || is_unmatched_data_file(path) {
        return path.with_extension(MODIFYING)
    }

    panic!("Cannot create a modifying file for {}", path.to_canoncial_string())
}

///
/// Take the path to a data file and return a path to a modified version of it.
///
/// e.g. $REC_HOME/unmatched/20191209_020405000_INV.unmatched.csv.
///    -> $REC_HOME/unmatched/20191209_020405000_INV.unmatched.csv.pre_modified
///
pub fn pre_modified(path: &Path) -> PathBuf {
    path.with_extension(PRE_MODIFIED)
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