mod analyser;
mod error;
mod folders;

use uuid::Uuid;
use anyhow::Result;
use ubyte::ToByteUnit;
use error::JetwashError;
use itertools::Itertools;
use crate::folders::ToCanoncialString;
use std::{time::Instant, path::{PathBuf, Path}, str::FromStr, collections::HashMap, fs::{File, self}};
use core::{charter::{Charter, JetwashSourceFile, Jetwash}, formatted_duration_rate, blue, data_type::DataType};


// TODO: Process instructions to map columns.
// TODO: Changeset generation.

///
/// Created for each match job. Used to pass the main top-level job 'things' around.
///
pub struct Context {
    started: Instant,      // When the job started.
    job_id: Uuid,          // Each job is given a unique id.
    charter: Charter,      // The charter of instructions to run.
    charter_path: PathBuf, // The path to the charter being run.
    base_dir: String,      // The root of the working folder for data (see the folders module).
    timestamp: String,     // A unique timestamp to prefix any generated files with for this job.
    // lua: rlua::Lua,        // Lua engine state.
    // phase: Cell<Phase>,    // The current point in the linear state transition of the job.
}

impl Context {
    pub fn new(charter: Charter, charter_path: PathBuf, base_dir: String) -> Self {
        let job_id = match std::env::var("OPENREC_FIXED_JOB_ID") {
            Ok(job_id) => uuid::Uuid::from_str(&job_id).expect("Test JOB_ID has invalid format"),
            Err(_) => uuid::Uuid::new_v4(),
        };

        Self {
            started: Instant::now(),
            job_id,
            charter,
            charter_path,
            base_dir,
            timestamp: folders::new_timestamp(),
            // lua: rlua::Lua::new(),
            // phase: Cell::new(Phase::FolderInitialisation),
        }
    }

    pub fn started(&self) -> Instant {
        self.started
    }

    pub fn job_id(&self) -> &Uuid {
        &self.job_id
    }

    pub fn charter(&self) -> &Charter {
        &self.charter
    }

    pub fn charter_path(&self) -> &PathBuf {
        &self.charter_path
    }

    pub fn base_dir(&self) -> &str {
        &self.base_dir
    }

    pub fn ts(&self) -> &str {
        &self.timestamp
    }

    // pub fn lua(&self) -> &rlua::Lua {
    //     &self.lua
    // }

    // pub fn phase(&self) -> Phase {
    //     self.phase.get()
    // }

    // pub fn set_phase(&self, phase: Phase) {
    //     self.phase.set(phase);
    // }
}

#[derive(Debug)]
struct AnalysisResult {
    source_file: JetwashSourceFile,
    schema: Vec<DataType>
}

impl AnalysisResult {
    pub fn source_file(&self) -> &JetwashSourceFile {
        &self.source_file
    }

    pub fn schema(&self) -> &Vec<DataType> {
        &self.schema
    }
}

type AnalysisResults = HashMap<PathBuf /* inbox-file */, AnalysisResult>;

///
/// Scan and analyse inbox files, then run them through the Jetwash to produce waiting files for celerity.
///
pub fn run_charter(charter_path: &str, base_dir: &str) -> Result<(), JetwashError> {

    // Load the charter and create a load job context.
    let ctx = init_job(charter_path, base_dir)?;

    // Create inbox, archive and waiting folders (if required).
    folders::ensure_dirs_exist(&ctx)?;

    // If there are any previous .failed files in the inbox log an error and abort this job.
    abort_if_previous_failures(&ctx)?;

    // Any .inprogress files in waiting should log a warn and be removed.
    remove_incomplete_files(&ctx)?;

    // Validate and analyse the files.
    if let Some(jetwash) = ctx.charter().jetwash() {
        // Check the file is UTF8, a valid CSV, and analyse each column's data-type.
        let results = analyse_and_validate(&ctx, jetwash)?;

        // Create sanitised copies of the original files for celerity. Mapping any columns with mapping config.
        for file in results.keys() {
            let result = results.get(file).expect(&format!("Result for {:?} was not found", file));
            let new_file = folders::new_waiting_file(&ctx, file);
            let mut reader = csv_reader(file, result.source_file())?;

            // Create new file to start transforming the data into.
            let mut writer = csv::WriterBuilder::new()
                .has_headers(true)
                .quote_style(csv::QuoteStyle::Always)
                .from_path(new_file.clone())
                .map_err(|source| JetwashError::CannotOpenCsv{ path: new_file.to_canoncial_string(), source } )?;

            // Use either hardcoded headers (from the charter), or headers from source file.
            let header_record = match result.source_file().headers() {
                Some(headers) => csv::ByteRecord::from(headers.iter().map(|hdr| hdr.as_str()).collect::<Vec<&str>>()),
                None => reader.byte_headers()?.clone(),
            };

            writer.write_byte_record(&header_record)?;

            // Write the schema row to the new file.
            writer.write_record(result.schema().iter().map(|dt| dt.as_str()).collect::<Vec<&str>>())
                .map_err(|source| JetwashError::CannotWriteSchema{ filename: new_file.to_canoncial_string(), source })?;

            // Read each row in, write to new file.
            for result in reader.byte_records() {
                let record = result // Ensure we can read the record - but ignore it at this point.
                    .map_err(|source| JetwashError::CannotParseCsvRow { source, path: new_file.to_canoncial_string() })?;

                writer.write_byte_record(&record)?;
            }

            writer.flush()?;

            // Move the original file now.
            folders::move_to_original(&ctx, file)?;

            // Rename xxx.csv.inprogress to xxx.csv
            let new_file = folders::complete_new_file(&new_file)?;

            // Log file sizes.
            let f = File::open(new_file.clone())?;
            log::info!("Created file {} ({})", new_file.to_canoncial_string(), f.metadata().unwrap().len().bytes());
        }
    }

    Ok(())
}

fn remove_incomplete_files(ctx: &Context) -> Result<(), JetwashError> {
    let incomplete = folders::incomplete_in_waiting(ctx)?;
    for entry in incomplete {
        log::warn!("Deleting incomplete file {}", entry.file_name().to_string_lossy());
        fs::remove_file(entry.path())?;
    }

    Ok(())
}

fn abort_if_previous_failures(ctx: &Context) -> Result<(), JetwashError> {
    let failed = folders::failed_files_in_inbox(ctx)?;
    match failed.is_empty() {
        true  => Ok(()),
        false => {
            log::error!("Previous job failed. Cannot start a new job until .failed files have been manually fixed or removed. Failed files: -\n{}",
                failed.iter().map(|file| file.path().to_canoncial_string()).join("\n"));
            Err(JetwashError::PreviousFailures)
        },
    }
}

///
/// Parse and load the charter configuration, return a job Context.
///
fn init_job(charter: &str, base_dir: &str) -> Result<Context, JetwashError> {
    let ctx = Context::new(
        Charter::load(charter)?,
        Path::new(charter).canonicalize()?.to_path_buf(),
        base_dir.into());

    log::info!("Starting jetwash job:");
    log::info!("    Job ID: {}", ctx.job_id());
    log::info!("   Charter: {} (v{})", ctx.charter().name(), ctx.charter().version());
    log::info!("  Base dir: {}", ctx.base_dir());

    Ok(ctx)
}


///
/// Read each cell in and deduce what each column's data_type should be.
///
fn analyse_and_validate(ctx: &Context, jetwash: &Jetwash) -> Result<AnalysisResults, JetwashError> {

    let mut any_errors = false;
    let mut results = AnalysisResults::new();

    for source_file in jetwash.source_files().iter() {
        // Open a csv reader and iterate each row in the file to validate it's readable.
        for file in folders::files_in_inbox(ctx, source_file.pattern())? {
            let started = Instant::now();
            log::debug!("Scanning file {} ({})", file.path().to_string_lossy(), file.metadata().unwrap().len().bytes());

            // For now, just count all the records in a file and log them.
            let mut row_count = 0;
            let mut col_count = 0;
            let mut err_count = 0;

            // These are the analysed column's data-types.
            let mut data_types = vec!();

            // The row number to report in any errors is offset by the header row.
            let row_offset = match source_file.headers().is_some() {
                true  => 1,
                false => 0,
            };

            let mut rdr = csv_reader(&file.path(), source_file)?;

            for result in rdr.byte_records() {
                row_count += 1;

                match result {
                    Ok(csv_record) => {
                        // If this is the first row, initialise all current data-types.
                        if col_count == 0 {
                            data_types = vec![DataType::Unknown; csv_record.len()];
                        }

                        // Analyse the row's actual data and narrow-down what the type is.
                        if let Err(err) = analyser::analyse_types(&mut data_types, &csv_record) {
                            log::error!("{:?}:{} {}", file.path(), row_count + row_offset, err);
                            err_count += 1;
                        }

                        col_count = csv_record.len();
                    },
                    Err(err) => {
                        log::error!("{:?}:{} {}", file.path(), row_count + row_offset, err);
                        err_count += 1;
                    },
                }
            }

            if err_count > 0 {
                // If there are any errors rename the file.
                folders::fail_file(&file)?;
                any_errors = true;

            } else {
                // Store the analysis results for this file.
                results.insert(file.path(), AnalysisResult { source_file: source_file.clone(), schema: data_types });
            }

            let (duration, _rate) = formatted_duration_rate(row_count, started.elapsed());

            log::info!("{} records with {} columns scanned from file {} in {}",
                row_count,
                col_count,
                file.file_name().to_string_lossy(),
                blue(&duration));
        }
    }

    // TODO: If there's been any errors, abort the job (IF configured)
    if any_errors {
        return Err(JetwashError::AnalysisErrors)
    }

    Ok(results)
}

///
/// Create a CSV reader configured from the source file options ready to read the file/path specified.
///
fn csv_reader(path: &PathBuf, source_file: &JetwashSourceFile) -> Result<csv::Reader<File>, JetwashError> {
    csv::ReaderBuilder::new()
        // TODO: Allow Esccape, Quote and Delimeter chars to be configured.
        .has_headers(!source_file.headers().is_some())
        .from_path(path)
        .map_err(|source| JetwashError::CannotOpenCsv { source, path: path.to_canoncial_string() })
}