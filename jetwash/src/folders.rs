use chrono::Utc;
use regex::Regex;
use crate::{error::JetwashError, Context};
use std::{fs::{self, DirEntry}, path::{Path, PathBuf}};

///
/// Ensure the folders exist to process files for this reconcilliation.
///
pub fn ensure_dirs_exist(ctx: &Context) -> Result<(), JetwashError> {
    let home = Path::new(ctx.base_dir());

    log::debug!("Creating folder structure in [{}]", home.to_canoncial_string());

    let folders = vec!(
        inbox(ctx),
        waiting(ctx),
        original(ctx),
        lookups(ctx));

    for folder in folders {
        fs::create_dir_all(&folder)
            .map_err(|source| JetwashError::CannotCreateDir { source, path: folder.to_canoncial_string() } )?;
    }

    Ok(())
}

///
/// Return all the files in the inbox folder which match the filename (wildcard) specified.
///
pub fn files_in_inbox(ctx: &Context, file_pattern: &str) -> Result<Vec<DirEntry>, JetwashError> {
    let wildcard = Regex::new(file_pattern).map_err(|source| JetwashError::InvalidSourceFileRegEx { source })?;
    let mut files = vec!();
    for entry in inbox(ctx).read_dir()? {
        if let Ok(entry) = entry {
            // TODO: only import .ready files - to avoid partial file imports.
            if wildcard.is_match(&entry.file_name().to_string_lossy()) && !is_failed(&entry) {
                files.push(entry);
            }
        }
    }

    // Return files sorted by filename - for consistent behaviour.
    files.sort_by(|a,b| a.file_name().cmp(&b.file_name()));

    Ok(files)
}

///
/// Return all the failed files in the inbox folder.
///
pub fn failed_files_in_inbox(ctx: &Context) -> Result<Vec<DirEntry>, JetwashError> {
    let mut files = vec!();
    for entry in inbox(ctx).read_dir()? {
        if let Ok(entry) = entry {
            if is_failed(&entry) {
                files.push(entry);
            }
        }
    }

    // Return files sorted by filename - for consistent behaviour.
    files.sort_by(|a,b| a.file_name().cmp(&b.file_name()));

    Ok(files)
}

///
/// Return all the .inprogress files in the waiting folder.
///
pub fn incomplete_in_waiting(ctx: &Context) -> Result<Vec<DirEntry>, JetwashError> {
    let mut files = vec!();
    for entry in waiting(ctx).read_dir()? {
        if let Ok(entry) = entry {
            if entry.file_name().to_string_lossy().ends_with(".inprogress") {
                files.push(entry);
            }
        }
    }

    // Return files sorted by filename - for consistent behaviour.
    files.sort_by(|a,b| a.file_name().cmp(&b.file_name()));

    Ok(files)
}

///
/// Move the original source file to the original folder. It is given a timestamp as an extension. e.g.
///
/// invoices.csv -> invoices.csv.20211229_113200000
///
pub fn move_to_original(ctx: &Context, path: &PathBuf) -> Result<(), JetwashError> {
    let mut destination = original(ctx);
    destination.push(format!("{}.{}", path.file_name().expect("filename missing from original file").to_string_lossy(), ctx.ts()));

    log::debug!("Moving {:?} to {:?}", path, destination);

    fs::rename(path, destination.clone())
        .map_err(|source| JetwashError::CannotMoveFile { path: path.to_canoncial_string(), destination: destination.to_canoncial_string(), source })
}

///
/// Rename xxx.csv.inprogress to xxx.csv
///
pub fn complete_new_file(path: &PathBuf) -> Result<PathBuf, JetwashError> {

    let destination = path.with_extension("");
    // destination.push(format!("{}.{}", path.file_name().expect("filename missing from original file").to_string_lossy(), ctx.ts()));

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

pub fn original(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("archive/jetwash")
}

pub fn lookups(ctx: &Context) -> PathBuf {
    Path::new(ctx.base_dir()).join("lookups/")
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
pub fn new_waiting_file(ctx: &Context, file: &PathBuf) -> PathBuf {
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
