use chrono::Utc;
use regex::Regex;
use lazy_static::lazy_static;
use anyhow::Context as ErrContext;
use crate::{error::{JetwashError, here}, Context};
use std::{fs::{self, DirEntry}, path::{Path, PathBuf}};

lazy_static! {
    static ref CHANGESET_REGEX: Regex = Regex::new(r"^(\d{8}_\d{9})_changeset\.json$").expect("bad regex for CHANGESET_REGEX");
}

///
/// Rename a folder or file - captures the paths to log if fails.
///
pub fn rename<F, T>(from: F, to: T) -> Result<(), JetwashError>
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
/// Copy a file - captures the paths to log if fails.
///
pub fn copy<F, T>(from: F, to: T) -> Result<(), JetwashError>
where
    F: AsRef<Path>,
    T: AsRef<Path>
{
    let f_str = from.as_ref().to_canoncial_string();
    let t_str = to.as_ref().to_canoncial_string();

    log::debug!("Copying {} -> {}", f_str, t_str);

    fs::copy(from, to)
        .with_context(|| format!("Cannot copy {} to {}{}", f_str, t_str, here!()))?;

    Ok(())
}

///
/// Ensure the folders exist to process files for this reconcilliation.
///
pub fn ensure_dirs_exist(ctx: &Context) -> Result<(), JetwashError> {
    let home = Path::new(ctx.base_dir());

    log::debug!("Creating folder structure in [{}]", home.to_canoncial_string());

    let folders = vec!(
        inbox(ctx),
        waiting(ctx),
        archive(ctx),
        lookups(ctx));

    for folder in folders {
        fs::create_dir_all(&folder)
            .map_err(|source| JetwashError::CannotCreateDir { source, path: folder.to_canoncial_string() } )?;
    }

    Ok(())
}

///
/// Changeset files should already be timestamp prefixed, Jetwash just moves them into the waiting
/// folder for celerity - no processing is done on them although they are archived.
///
pub fn progress_changesets(ctx: &Context) -> Result<(), JetwashError> {
    for entry in (inbox(ctx).read_dir()?).flatten() { // Result is an iterator, so flatten if only interested in Ok values.
        if is_changeset_file(&entry.path()) {
            // Copy to the archive folder.
            copy(entry.path(), archive(ctx).join(entry.file_name()))?;

            // Move to the celerity waiting folder.
            let dest = waiting(ctx).join(entry.file_name());
            rename(entry.path(), dest)?
        }
    }
    Ok(())
}

///
/// Return all the files in the inbox folder which match the filename (wildcard) specified.
///
pub fn files_in_inbox(ctx: &Context, file_pattern: &str) -> Result<Vec<DirEntry>, JetwashError> {
    let wildcard = Regex::new(file_pattern).map_err(|source| JetwashError::InvalidSourceFileRegEx { source })?;
    let mut files = vec!();
    for entry in (inbox(ctx).read_dir()?).flatten() {
        if wildcard.is_match(&entry.file_name().to_string_lossy()) && !is_failed(&entry) {
            files.push(entry);
        }
    }

    // Return files sorted by filename - for consistent behaviour.
    files.sort_by_key(|a| a.file_name());

    Ok(files)
}

///
/// Return all the failed files in the inbox folder.
///
pub fn failed_files_in_inbox(ctx: &Context) -> Result<Vec<DirEntry>, JetwashError> {
    let mut files = vec!();
    for entry in (inbox(ctx).read_dir()?).flatten() {
        if is_failed(&entry) {
            files.push(entry);
        }
    }

    // Return files sorted by filename - for consistent behaviour.
    files.sort_by_key(|a| a.file_name());

    Ok(files)
}

///
/// Return all the .inprogress files in the waiting folder.
///
pub fn incomplete_in_waiting(ctx: &Context) -> Result<Vec<DirEntry>, JetwashError> {
    let mut files = vec!();
    for entry in (waiting(ctx).read_dir()?).flatten() {
        if entry.file_name().to_string_lossy().ends_with(".inprogress") {
            files.push(entry);
        }
    }

    // Return files sorted by filename - for consistent behaviour.
    files.sort_by_key(|a| a.file_name());

    Ok(files)
}

///
/// Move the original source file to the original folder. It is given a timestamp as a prefix. e.g.
///
/// invoices.csv -> 20211229_113200000_invoices.csv
///
/// If archiving is disabled in the charter, the file is deleted.
///
pub fn move_to_archive(ctx: &Context, path: &Path) -> Result<(), JetwashError> {
    if ctx.charter().archive_files() {
        let mut destination = archive(ctx);
        destination.push(format!("{}_{}", ctx.ts(), path.file_name().expect("filename missing from original file").to_string_lossy()));

        log::debug!("Moving {:?} to {:?}", path, destination);

        fs::rename(path, destination.clone())
            .map_err(|source| JetwashError::CannotMoveFile { path: path.to_canoncial_string(), destination: destination.to_canoncial_string(), source })

    } else {
        log::debug!("Removing {:?}", path);
        fs::remove_file(&path)
            .map_err(|source| JetwashError::CannotRemoveFile { path: path.to_canoncial_string(), source })
    }
}

///
/// Rename xxx.csv.inprogress to xxx.csv
///
pub fn complete_new_file(path: &Path) -> Result<PathBuf, JetwashError> {

    let destination = path.with_extension("");

    log::debug!("Moving {:?} to {:?}", path, destination);

    fs::rename(path, destination.clone())
        .map_err(|source| JetwashError::CannotMoveFile { path: path.to_canoncial_string(), destination: destination.to_canoncial_string(), source })?;

    Ok(destination)
}


fn is_failed(entry: &DirEntry) -> bool {
    match entry.metadata() {
        Ok(metadata) => metadata.is_file() && entry.file_name().to_string_lossy().ends_with(".failed"),
        Err(err) => {
            log::warn!("Skipping file, failed to get metadata for {}: {}", entry.path().to_canoncial_string(), err);
            false
        }
    }
}

pub fn inbox(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("inbox/")
}

pub fn waiting(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("waiting/")
}

pub fn archive(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("archive/jetwash")
}

pub fn lookups(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("lookups/")
}

///
/// Returns true if the file starts with a datetime prefix in the form 'YYYYMMDD_HHmmSSsss_' and ends with
/// a '.changeset.json' suffix.
///
fn is_changeset_file(path: &Path) -> bool {
    path.is_file() && CHANGESET_REGEX.is_match(&path.file_name().unwrap_or_default().to_string_lossy())
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
/// Generate the path to a new waiting file, given a file in the inbox folder. e.g.
///
/// ./tmp/inbox/invoices.csv -> ./tmp/waiting/20211229_063800123_invoices.csv.inprogress
///
pub fn new_waiting_file(ctx: &Context, file: &Path) -> PathBuf {
    let mut pb = waiting(ctx);
    pb.push(format!("{ts}_{filename}.inprogress",
        ts = new_timestamp(),
        filename = file.file_name().expect("no filename available").to_string_lossy()));
    pb
}

///
/// Put a .failed extension on the file.
///
pub fn fail_file(file: &DirEntry) -> Result<(), JetwashError> {
    let new_path = file.path().with_extension("csv.failed");
    log::info!("Renaming {} to {}", file.path().to_canoncial_string(), new_path.to_canoncial_string());
    Ok(fs::rename(file.path(), new_path)?)
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
