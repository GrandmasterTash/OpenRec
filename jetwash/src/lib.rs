mod error;
mod folders;
mod mapping;
mod analyser;

use uuid::Uuid;
use ubyte::ToByteUnit;
use error::JetwashError;
use itertools::Itertools;
use analyser::AnalysisResults;
use bytes::{Bytes, BytesMut, BufMut};
use crate::folders::ToCanoncialString;
use anyhow::{Result, Context as ErrContext};
use std::{time::Instant, path::{PathBuf, Path}, str::FromStr, fs::{File, self}, sync::atomic::{AtomicUsize, Ordering}};
use core::{charter::{Charter, JetwashSourceFile, ColumnMapping}, data_type::DataType, lua::init_context, blue, formatted_duration_rate};

// TODO: If charter doesn't exist - log the path that's failing.
// TODO: Logging - log files moved into waiting - reduce analyser spam
// TODO: Ensure the output file ends in .csv (even if original didn't).
// TODO: Clippy!

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
    uuid_provider: UuidProvider, // Generate record uuids.
}

impl Context {
    pub fn new(charter: Charter, charter_path: PathBuf, base_dir: PathBuf, uuid_seed: Option<usize>) -> Self {
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
            uuid_provider: UuidProvider::new(uuid_seed),
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

    pub fn uuid_provider(&self) -> &UuidProvider {
        &self.uuid_provider
    }
}

///
/// Scan and analyse inbox files, then run them through the Jetwash to produce waiting files for celerity.
///
/// The uuid_seed is only used for tests to give records a predictable uuid.
///
pub fn run_charter<P: AsRef<Path>>(charter_path: P, base_dir: P, uuid_seed: Option<usize>) -> Result<(), JetwashError> {

    // Load the charter and create a load job context.
    let ctx = init_job(
        charter_path.as_ref().to_path_buf().canonicalize().with_context(|| format!("charter path {:?}", charter_path.as_ref()))?,
        base_dir.as_ref().to_path_buf().canonicalize().with_context(|| format!("base dir {:?}", base_dir.as_ref()))?,
        uuid_seed)?;

    // Create inbox, archive and waiting folders (if required).
    folders::ensure_dirs_exist(&ctx)?;

    // If there are any previous .failed files in the inbox log an error and abort this job.
    abort_if_previous_failures(&ctx)?;

    // Any .inprogress files in waiting should log a warn and be removed.
    remove_incomplete_files(&ctx)?;

    // Changeset files should be moved from the inbox to the waiting folder.
    folders::progress_changesets(&ctx)?;

    // Validate and analyse the files.
    if let Some(jetwash) = ctx.charter().jetwash() {
        // Check the file is UTF8, a valid CSV, and analyse each column's data-type.
        let results = analyser::analyse_and_validate(&ctx, jetwash)?;

        // Create sanitised copies of the original files for celerity. Mapping any columns with mapping config.
        for file in results.keys().sorted() {
            wash_file(&ctx, file, &results)?;
        }
    }

    log::info!("Completed jetwash job {} in {}", ctx.job_id(), blue(&formatted_duration_rate(1, ctx.started().elapsed()).0));

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
        init_context(&lua_ctx, ctx.charter().global_lua(), &folders::lookups(ctx))?;

        for record_result in reader.byte_records() {
            let record = record_result // Ensure we can read the record - but ignore it at this point.
                .map_err(|source| JetwashError::CannotParseCsvRow { source, path: new_file.to_canoncial_string() })?;

            let record = transform_record(ctx, &lua_ctx, result.source_file(), &header_record, &record)?; // TODO: Track lua eval context for errors....

            writer.write_byte_record(&record).map_err(|source| JetwashError::CannotWriteCsvRow {source, path: new_file.to_canoncial_string() })?;
        }
        Ok(())
    })
    .map_err(|source| JetwashError::TransformRecordError { source })?;

    writer.flush()?;

    // Move the original file now.
    folders::move_to_archive(&ctx, file)?;

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
    ctx: &Context,
    lua_ctx: &rlua::Context,
    source_file: &JetwashSourceFile,
    header_record: &csv::ByteRecord,
    record: &csv::ByteRecord) -> Result<csv::ByteRecord, JetwashError> {

    let line = record.position().expect("no row position").line();

    let mut new_record = csv::ByteRecord::new();
    new_record.push_field(b"0"); // OpenRecStatus - 0 = unmatched
    new_record.push_field(ctx.uuid_provider().next_record_id().to_hyphenated().to_string().as_bytes()); // OpenRecId.

    // Copy each existing field into the new record - applying a mapping if there is one.
    for (header, value) in header_record.iter().skip(2 /* hardcoded headers */).zip(record.iter()) {
        let header = String::from_utf8_lossy(header).to_string();

        match source_file.column_mappings() {
            Some(mappings) => {
                match mappings.iter().find(|m| m.column() == header) {
                    Some(mapping) => {
                        let new_value = mapping::map_field(lua_ctx, mapping, bytes_from_slice(&value))?;

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
        lua_ctx.globals().set("record", mapping::lua_record(lua_ctx, &new_record, &header_record)?)?;

        for column in new_columns {
            let new_value: Bytes = mapping::eval_typed_lua(&lua_ctx, column.from(), column.as_a())?.into();

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
fn init_job(charter: PathBuf, base_dir: PathBuf, uuid_seed: Option<usize>) -> Result<Context, JetwashError> {
    let ctx = Context::new(
        Charter::load(&charter)?,
        charter,
        base_dir,
        uuid_seed);

    log::info!("Starting jetwash job:");
    log::info!("    Job ID: {}", ctx.job_id());
    log::info!("   Charter: {} (v{})", ctx.charter().name(), ctx.charter().version());
    log::info!("  Base dir: {}", ctx.base_dir().to_canoncial_string());

    Ok(ctx)
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

                // If there's a column mapping for this header, use the as_a type.
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

                match mapped_type {
                    Some(data_type) => data_type,
                    // Otherwise, use the analysed type for the header - or Unknown if the file is empty?
                    // None => *analysed_schema.get(idx).unwrap_or_else(|| panic!("no analyed type for {}", header)),
                    None => *analysed_schema.get(idx).unwrap_or(&DataType::Unknown),
                }
            }
        })
        .collect::<Vec<DataType>>()
}

///
/// The record UUID provider returns a secure random v4 uuid in normal mode.
///
/// If a test setting is set, it will generated predictable ids to allow tests to make assertions.
///
pub struct UuidProvider { counter: Option<AtomicUsize> }

impl UuidProvider {
    fn new(uuid_seed: Option<usize>) -> Self {
        let counter = match uuid_seed {
            Some(arg) => {
                Some(AtomicUsize::new(arg))
            },
            None => None,
        };

        Self { counter }
    }

    ///
    /// Get a secure random v4 uuid - if we're running tests, we'll use a counter to return predicable id's.
    ///
    fn next_record_id(&self) -> uuid::Uuid {

        match &self.counter {
            Some(counter) => {
                let next = counter.fetch_add(1, Ordering::SeqCst);
                uuid::Builder::from_u128(next as u128).build()
            },
            None => uuid::Uuid::new_v4(),
        }
    }
}