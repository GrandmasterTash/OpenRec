mod error;
mod folders;
mod analyser;

use uuid::Uuid;
use regex::Regex;
use anyhow::Result;
use ubyte::ToByteUnit;
use rlua::FromLuaMulti;
use error::JetwashError;
use itertools::Itertools;
use rust_decimal::Decimal;
use lazy_static::lazy_static;
use bytes::{Bytes, BytesMut, BufMut};
use crate::folders::ToCanoncialString;
use chrono::{Utc, TimeZone, SecondsFormat};
use std::{time::Instant, path::{PathBuf, Path}, str::FromStr, collections::HashMap, fs::{File, self}};
use core::{charter::{Charter, JetwashSourceFile, Jetwash, ColumnMapping}, formatted_duration_rate, blue, data_type::DataType, lua::{init_context, LuaDecimal}};

// TODO: Lookups. https://github.com/geoffleyland/lua-csv
// TODO: Clippy!
// TODO: Log time for job.
// TODO: Non-example based tests.

lazy_static! {
    static ref DATES: Vec<Regex> = vec!(
        Regex::new(r"^(\d{1,4})-(\d{1,4})-(\d{1,4})$").expect("bad regex d-m-y"),      // d-m-y
        Regex::new(r"^(\d{1,4})/(\d{1,4})/(\d{1,4})$").expect("bad regex d/m/y"),      // d/m/y
        Regex::new(r"^(\d{1,4})\\(\d{1,4})\\(\d{1,4})$").expect(r#"bad regex d\m\y"#), // d\m\y
        Regex::new(r"^(\d{1,4})\W(\d{1,4})\W(\d{1,4})$").expect("bad regex d m y"),    // d m y
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
    base_dir: PathBuf,     // The root of the working folder for data (see the folders module).
    timestamp: String,     // A unique timestamp to prefix any generated files with for this job.
    lua: rlua::Lua,        // Lua engine state.
}

impl Context {
    pub fn new(charter: Charter, charter_path: PathBuf, base_dir: PathBuf) -> Self {
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

    pub fn base_dir(&self) -> &PathBuf {
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
pub fn run_charter<P: AsRef<Path>>(charter_path: P, base_dir: P) -> Result<(), JetwashError> {

    // Load the charter and create a load job context.
    let ctx = init_job(
        charter_path.as_ref().to_path_buf().canonicalize()?,
        base_dir.as_ref().to_path_buf().canonicalize()?)?;

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
            wash_file(&ctx, file, &results)?;
        }
    }

    Ok(())
}

///
/// Run any column transformations for this file and generate a 'standard form' csv for Celerity.
///
fn wash_file(ctx: &Context, file: &PathBuf, results: &AnalysisResults) -> Result<(), JetwashError> {

    let result = results.get(file).expect(&format!("Result for {:?} was not found", file));
    let new_file = folders::new_waiting_file(&ctx, file);
    let mut reader = csv_reader(file, result.source_file())?;

    // Create new file to start transforming the data into.
    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .quote_style(csv::QuoteStyle::Always)
        .from_path(new_file.clone())
        .map_err(|source| JetwashError::CannotOpenCsv{ path: new_file.to_canoncial_string(), source } )?;

    let header_record = header_record(result.source_file(), &mut reader)?;
    writer.write_byte_record(&header_record)?;

    // Write the schema row to the new file.
    let schema = final_schema(result.analysed_schema(), result.source_file(), &header_record);
    writer.write_record(schema.iter().map(|dt| dt.as_str()).collect::<Vec<&str>>())
        .map_err(|source| JetwashError::CannotWriteSchema{ filename: new_file.to_canoncial_string(), source })?;

    // Read each row in, write to new file.
    ctx.lua().context(|lua_ctx| {
        init_context(&lua_ctx, ctx.charter().global_lua())?;

        for record_result in reader.byte_records() {
            let record = record_result // Ensure we can read the record - but ignore it at this point.
                .map_err(|source| JetwashError::CannotParseCsvRow { source, path: new_file.to_canoncial_string() })?;

            let record = transform_record(&lua_ctx, result.source_file(), &header_record, &record)?; // TODO: Track lua eval context for errors....

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
    let f = File::open(new_file.clone()).expect(&format!("Unable to open {}", new_file.to_canoncial_string()));
    log::info!("Created file {} ({})", new_file.to_canoncial_string(), f.metadata().expect("no metadata").len().bytes());

    Ok(())
}

///
/// Use the defined column headers or those in the source csv file.
///
/// Ensure the internal status and id columns are added first.
/// Ensure new columns are appended to the end.
///
fn header_record(source_file: &JetwashSourceFile, reader: &mut csv::Reader<File>) -> Result<csv::ByteRecord, JetwashError> {
    let mut header_record = csv::ByteRecord::new();
    header_record.push_field(b"OpenRecStatus");
    header_record.push_field(b"OpenRecId");

    match source_file.headers() {
        Some(headers) => headers.iter().for_each(|hdr| header_record.push_field(hdr.as_bytes())),
        None => reader.byte_headers()?.iter().for_each(|f| header_record.push_field(f)),
    }

    // Add any column headers for Jetwash-created columns.
    if let Some(new_cols) = source_file.new_columns() {
        new_cols.iter().for_each(|nc| header_record.push_field(nc.column().as_bytes()));
    }

    Ok(header_record)
}

///
/// Perform any column Lua script transformations on the data.
///
fn transform_record(
    lua_ctx: &rlua::Context,
    source_file: &JetwashSourceFile,
    header_record: &csv::ByteRecord,
    record: &csv::ByteRecord) -> Result<csv::ByteRecord, JetwashError> {

    let line = record.position().expect("no row position").line();

    let mut new_record = csv::ByteRecord::new();
    new_record.push_field(b"0"); // OpenRecStatus - 0 = unmatched
    new_record.push_field(uuid::Uuid::new_v4().to_hyphenated().to_string().as_bytes()); // OpenRecId.

    // Copy each existing field into the new record - applying a mapping if there is one.
    for (header, value) in header_record.iter().skip(2 /* hardcoded headers */).zip(record.iter()) {
        let header = String::from_utf8_lossy(header).to_string();

        match source_file.column_mappings() {
            Some(mappings) => {
                match mappings.iter().find(|m| m.column() == header) {
                    Some(mapping) => {
                        let new_value = map_field(lua_ctx, mapping, bytes_from_slice(&value))?;

                        log::trace!("Mapping row {row}, column {column} from [{from}] to [{to}]",
                            column = header,
                            row = line,
                            from = String::from_utf8_lossy(&value),
                            to = String::from_utf8_lossy(&new_value.to_vec()));

                        new_record.push_field(&new_value);
                    },
                    None => new_record.push_field(&value),
                }
            },
            None => new_record.push_field(&value),
        }
    }

    // Transform new columns.
    if let Some(new_columns) = source_file.new_columns() {
        lua_ctx.globals().set("record", lua_record(lua_ctx, &new_record, &header_record)?)?;

        for column in new_columns {
            let new_value: Bytes = eval_typed_lua(&lua_ctx, column.from(), column.as_a())?.into();

            log::trace!("Mapping row {row}, column {column} from [new] to [{to}]",
                column = column.column(),
                row = line,
                to = String::from_utf8_lossy(&new_value.to_vec()));

            new_record.push_field(&new_value);
        }
    }

    Ok(new_record)
}

fn bytes_from_slice(data: &[u8]) -> Bytes {
    let mut bm = BytesMut::new();
    bm.put(data);
    bm.freeze()
}

///
/// Use Lua to generate a new column on the incoming file.
///
fn eval_typed_lua(lua_ctx: &rlua::Context, lua: &str, as_a: DataType) -> Result<String, JetwashError> {
    let mapped = match as_a {
        DataType::Unknown => panic!("Can't eval if data-type is Unknown"),
        DataType::Boolean => bool_to_string(eval(lua_ctx, lua)?),
        DataType::Datetime => datetime_to_string(eval(lua_ctx, lua)?),
        DataType::Decimal => decimal_to_string(eval::<LuaDecimal>(lua_ctx, lua)?.0),
        DataType::Integer => int_to_string(eval(lua_ctx, lua)?),
        DataType::String => eval(lua_ctx, lua)?,
        DataType::Uuid => eval(lua_ctx, lua)?,
    };
    Ok(mapped)
}

fn bool_to_string(value: bool) -> String {
    format!("{}", value)
}

fn datetime_to_string(value: u64) -> String {
    let dt = Utc.timestamp(value as i64 / 1000, (value % 1000) as u32 * 1000000);
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn decimal_to_string(value: Decimal) -> String {
    format!("{}", value)
}

fn int_to_string(value: i64) -> String {
    format!("{}", value)
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
        ColumnMapping::Map { from, as_a, .. } => {
            // Perform some Lua to evaluate the mapped value to push into the new record.
            lua_ctx.globals().set("value", value.clone())?;
            eval_typed_lua(lua_ctx, from, *as_a)?
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
fn init_job(charter: PathBuf, base_dir: PathBuf) -> Result<Context, JetwashError> {
    let ctx = Context::new(
        Charter::load(&charter)?,
        charter,
        base_dir);

    log::info!("Starting jetwash job:");
    log::info!("    Job ID: {}", ctx.job_id());
    log::info!("   Charter: {} (v{})", ctx.charter().name(), ctx.charter().version());
    log::info!("  Base dir: {}", ctx.base_dir().to_canoncial_string());

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
            log::debug!("Scanning file {} ({})", file.path().to_string_lossy(), file.metadata().expect("no metadata").len().bytes());

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
    let escape = match source_file.escape() {
        Some(e) => Some(e.as_bytes()[0]),
        None => None,
    };

    let quote = match source_file.quote() {
        Some(q) => q.as_bytes()[0],
        None => b'"',
    };

    let delimeter = match source_file.delimeter() {
        Some(d) => d.as_bytes()[0],
        None => b',',
    };

    csv::ReaderBuilder::new()
        .has_headers(!source_file.headers().is_some())
        .escape(escape)
        .quote(quote)
        .delimiter(delimeter)
        .from_path(path)
            .map_err(|source| JetwashError::CannotOpenCsv { source, path: path.to_canoncial_string() })
}


///
/// The analysed schema datatype adjusted for mapped column datatypes in the charter.
///
fn final_schema(analysed_schema: &Vec<DataType>, source_file: &JetwashSourceFile, header_record: &csv::ByteRecord)
    -> Vec<DataType> {

    header_record.iter()
        .enumerate()
        .map(|(idx, hdr)| {
            let header = String::from_utf8_lossy(hdr).to_string();

            if header == "OpenRecStatus" {
                DataType::Integer

            } else if header == "OpenRecId" {
                DataType::Uuid

            } else {
                let idx = idx - 2; // Two hardcoded columns to offset by.

                // If theere's a column mapping for this header, use the as_a type.
                let mapped_type = match source_file.column_mappings() {
                    Some(mappings) => mappings.iter().find(|mapping| mapping.column() == header).map(|cm| {
                        match cm {
                            ColumnMapping::Map { as_a, .. } => *as_a,
                            ColumnMapping::Dmy { .. } => DataType::Datetime,
                            ColumnMapping::Mdy { .. } => DataType::Datetime,
                            ColumnMapping::Ymd { .. } => DataType::Datetime,
                            ColumnMapping::Trim { .. } => *analysed_schema.get(idx).expect(&format!("no analyed type for {}", header)),
                        }
                    }),
                    None => None,
                };

                // If there's a new column for this header, use the as_a type.
                let mapped_type = match mapped_type {
                    Some(mt) => Some(mt),
                    None => match source_file.new_columns() {
                        Some(new_cols) => {
                            match new_cols.iter().find(|nc| nc.column() == header) {
                                Some(new_col) => Some(new_col.as_a()),
                                None => None,
                            }
                        },
                        None => None,
                    },
                };

                // Otherwise, use the analysed type for the header.
                match mapped_type {
                    Some(data_type) => data_type,
                    None => *analysed_schema.get(idx).expect(&format!("no analyed type for {}", header)),
                }
            }
        })
        .collect::<Vec<DataType>>()
}

///
/// Populate a Lua table of strings.
///
fn lua_record<'a>(lua_ctx: &rlua::Context<'a>, record: &csv::ByteRecord, header_record: &csv::ByteRecord)
    -> Result<rlua::Table<'a>, JetwashError> {

    // TODO: Perf. Consider scanning for only referenced columns.

    let lua_record = lua_ctx.create_table()?;

    for (header, value) in header_record.iter().zip(record.iter()) {
        let l_header: String = String::from_utf8_lossy(header).into();
        let l_value: String = String::from_utf8_lossy(value).into();
        lua_record.set(l_header, l_value)?;
    }

    Ok(lua_record)
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


// TODO: Break this bad-boy up a bit.