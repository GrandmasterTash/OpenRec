mod analyser;
mod error;
mod folders;

use regex::Regex;
use uuid::Uuid;
use anyhow::Result;
use ubyte::ToByteUnit;
use rlua::FromLuaMulti;
use error::JetwashError;
use itertools::Itertools;
use chrono::{Utc, TimeZone};
use lazy_static::lazy_static;
use bytes::{Bytes, BytesMut, BufMut};
use crate::folders::ToCanoncialString;
use std::{time::Instant, path::{PathBuf, Path}, str::FromStr, collections::HashMap, fs::{File, self}};
use core::{charter::{Charter, JetwashSourceFile, Jetwash, ColumnMapping}, formatted_duration_rate, blue, data_type::DataType};

// TODO: Append a uuid column to each record.
// TODO: Changeset generation. Suggest this is a new component.

lazy_static! {
    static ref DATES: Vec<Regex> = vec!(
        Regex::new(r"^(\d{1,4})-(\d{1,4})-(\d{1,4})$").unwrap(),   // d-m-y
        Regex::new(r"^(\d{1,4})/(\d{1,4})/(\d{1,4})$").unwrap(), // d/m/y
        Regex::new(r"^(\d{1,4})\\(\d{1,4})\\(\d{1,4})$").unwrap(), // d\m\y
        Regex::new(r"^(\d{1,4})\W(\d{1,4})\W(\d{1,4})$").unwrap(), // d m y
    );
}

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
    lua: rlua::Lua,        // Lua engine state.
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
            lua: rlua::Lua::new(),
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

    pub fn lua(&self) -> &rlua::Lua {
        &self.lua
    }
}

#[derive(Debug)]
struct AnalysisResult {
    source_file: JetwashSourceFile,
    analysed_schema: Vec<DataType>
}

///
/// Represents the result of analysing a csv file.
///
impl AnalysisResult {
    pub fn source_file(&self) -> &JetwashSourceFile {
        &self.source_file
    }

    pub fn analysed_schema(&self) -> &Vec<DataType> {
        &self.analysed_schema
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
            // TODO: This for loop block needs to go in a fn....

            let result = results.get(file).expect(&format!("Result for {:?} was not found", file));
            let new_file = folders::new_waiting_file(&ctx, file);
            let mut reader = csv_reader(file, result.source_file())?;

            // TODO: Push this line into fn as per above.
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
            let schema = final_schema(result.analysed_schema(), result.source_file(), &header_record);
            writer.write_record(schema.iter().map(|dt| dt.as_str()).collect::<Vec<&str>>())
                .map_err(|source| JetwashError::CannotWriteSchema{ filename: new_file.to_canoncial_string(), source })?;

            ctx.lua().context(|lua_ctx| {
                // Read each row in, write to new file.
                for record_result in reader.byte_records() {
                    let record = record_result // Ensure we can read the record - but ignore it at this point.
                        .map_err(|source| JetwashError::CannotParseCsvRow { source, path: new_file.to_canoncial_string() })?;

                    let record = transform_record(&lua_ctx, result.source_file(), &header_record, record)?; // TODO: Track lua eval context for errors....

                    writer.write_byte_record(&record).map_err(|source| JetwashError::CannotWriteCsvRow {source, path: new_file.to_canoncial_string() })?;
                }
                Ok(())
            })
            .map_err(|source| JetwashError::TransformRecordError { source })?;

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

///
/// Perform any column Lua script transformations on the data.
///
fn transform_record(
    lua_ctx: &rlua::Context,
    source_file: &JetwashSourceFile,
    header_record: &csv::ByteRecord,
    record: csv::ByteRecord) -> Result<csv::ByteRecord, JetwashError> {

    match source_file.column_mappings() {
        Some(mappings) => {
            if mappings.is_empty() {
                return Ok(record)
            }

            let mut new_record = csv::ByteRecord::new();

            for (idx, header) in header_record.iter().enumerate() {
                let header = String::from_utf8_lossy(header);

                // Get the record field data into a Bytes snapshot to avoid lifetime issues.
                let mut original = BytesMut::new();
                original.put(record.get(idx).expect("No data"));
                let original = original.freeze();

                // If there's a mapping perform it now - otherwise just copy the source record value.
                let new_value = match mappings.iter().find(|m| m.column() == header) {
                    Some(mapping) => {
                        let new_value = map_field(lua_ctx, mapping, original.clone())?;

                        log::trace!("Mapping row {row}, column {column} from [{from}] to [{to}]",
                            column = header,
                            row = record.position().expect("no row position").line(),
                            from = String::from_utf8_lossy(&original),
                            to = String::from_utf8_lossy(&new_value.to_vec()));

                        new_value
                    },
                    None => original.clone(),
                };

                new_record.push_field(&new_value);
            }

            Ok(new_record)
        },
        None => Ok(record),
    }
}

///
/// Perform a column mapping on the value specified.
///
/// Mappings could be raw Lua script or one of a preset help mappings, trim, dmy, etc.
///
fn map_field(lua_ctx: &rlua::Context, mapping: &ColumnMapping, original: Bytes) -> Result<Bytes, JetwashError> {
    // Provide the original value to the Lua script as a string variable called 'value'.
    let value = String::from_utf8_lossy(&original).to_string();

    let mapped: String = match mapping {
        ColumnMapping::Map { from, .. } => {
            // Perform some Lua to evaluate the mapped value to push into the new record.
            lua_ctx.globals().set("value", value.clone())?;
            eval(lua_ctx, from)?
        },

        ColumnMapping::Dmy( _column ) => {
            // If there's a value, try to parse as d/m/y, then d-m-y, then d\m\y, then d m y.
            match date_captures(&value) {
                Some(captures) => {
                    let dt = Utc.ymd(captures.2 as i32, captures.1, captures.0).and_hms_milli(0, 0, 0, 0);
                    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                },
                None => value,
            }
        },

        ColumnMapping::Mdy( _column ) => {
            // If there's a value, try to parse as m/d/y, then m-d-y, then m\d\y, then m d y.
            match date_captures(&value) {
                Some(captures) => {
                    let dt = Utc.ymd(captures.2 as i32, captures.0, captures.1).and_hms_milli(0, 0, 0, 0);
                    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                },
                None => value,
            }
        },

        ColumnMapping::Ymd( _column ) => {
            // If there's a value, try to parse as y/m/d, then y-m-d, then y/m/d, then y m d.
            match date_captures(&value) {
                Some(captures) => {
                    let dt = Utc.ymd(captures.0 as i32, captures.1, captures.0).and_hms_milli(0, 0, 0, 0);
                    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                },
                None => value,
            }
        },

        ColumnMapping::Trim( _column ) => value.trim().to_string(),
    };

    Ok(mapped.into())
}

///
/// Iterate the date pattern combinations and if we get a match, return the three component/captures
///
fn date_captures(value: &str) -> Option<(u32, u32, u32)> {
    for pattern in &*DATES {
        match pattern.captures(&value) {
            Some(captures) if captures.len() == 4 => {
                if let Ok(n1) = captures.get(1).expect("capture 1 missing").as_str().parse::<u32>() {
                    if let Ok(n2) = captures.get(2).expect("capture 2 missing").as_str().parse::<u32>() {
                        if let Ok(n3) = captures.get(3).expect("capture 3 missing").as_str().parse::<u32>() {
                            return Some((n1, n2, n3))
                        }
                    }
                }
            },
            Some(_capture) => {},
            None => {},
        }
    }
    None
}

///
/// Remove any .inprogress files left-over from a previous job.
///
fn remove_incomplete_files(ctx: &Context) -> Result<(), JetwashError> {
    let incomplete = folders::incomplete_in_waiting(ctx)?;
    for entry in incomplete {
        log::warn!("Deleting incomplete file {}", entry.file_name().to_string_lossy());
        fs::remove_file(entry.path())?;
    }

    Ok(())
}

///
/// Do not allow a new job to commence if there are .failed files from a previous job.
///
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
                results.insert(file.path(), AnalysisResult { source_file: source_file.clone(), analysed_schema: data_types });
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


///
/// The analysed schema datatype adjusted for mapped column datatypes in the charter.
///
fn final_schema(analysed_schema: &Vec<DataType>, source_file: &JetwashSourceFile, header_record: &csv::ByteRecord) -> Vec<DataType> {
    header_record.iter()
        .enumerate()
        // Use the analysed data-type for this column - unless there's a column mapping in the charter.
        // in which case, use the configured 'as_a' data-type.
        .map(|(idx, hdr)| {
            let header = String::from_utf8_lossy(hdr).to_string();

            let mapped_type = match source_file.column_mappings() {
                Some(mappings) => mappings.iter().find(|mapping| mapping.column() == header).map(|cm| {
                    match cm {
                        ColumnMapping::Map { as_a, .. } => *as_a,
                        ColumnMapping::Dmy { .. } => DataType::Datetime,
                        ColumnMapping::Mdy { .. } => DataType::Datetime,
                        ColumnMapping::Ymd { .. } => DataType::Datetime,
                        ColumnMapping::Trim { .. } => *analysed_schema.get(idx).expect("no analyed type"),
                    }
                }),
                None => None,
            };

            match mapped_type {
                Some(data_type) => data_type,
                None => *analysed_schema.get(idx).expect("no analyed type"),
            }
        })
        .collect::<Vec<DataType>>()
}

///
/// Run the lua script provided. Reporting the failing script if it errors.
///
fn eval<'lua, R: FromLuaMulti<'lua>>(lua_ctx: &rlua::Context<'lua>, lua: &str)
    -> Result<R, rlua::Error> {

    match lua_ctx.load(lua).eval::<R>() {
        Ok(result) => Ok(result),
        Err(err) => {
            log::error!("Error in Lua script:\n{}\n\n{}", lua, err.to_string());
            Err(err)
        },
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_date() {
        let re = regex::Regex::new(r"^(\d{1,4})/(\d{1,4})/(\d{1,4})$").unwrap();
        assert!(re.is_match("16/03/2009"));

    }
}