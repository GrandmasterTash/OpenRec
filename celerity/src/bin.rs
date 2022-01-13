use chrono::Utc;
use anyhow::Result;
use clap::{App, Arg};
use log::LevelFilter;
use std::{path::Path, fs};
use log4rs::{append::{console::ConsoleAppender, file::FileAppender}, Config, config::{Appender, Root}, encode::pattern::PatternEncoder, Handle};

// Ref: https://docs.rs/log4rs/latest/log4rs/encode/pattern/index.html
const CONSOLE_PATTERN: &str = "[{d(%Y-%m-%d %H:%M:%S%.3f)} {h({l:<5})}] {m}{n}";
const FILE_PATTERN: &str = "[{d(%Y-%m-%d %H:%M:%S%.3f)} {l:<5}] {m}{n}";

pub fn main() -> Result<()> {

    let options = App::new("celerity")
        .version("1.0")
        .about("Celerity is a reconciliation engine used to group and match data from CSV files. Leaving only unmatched data behind. Data must be in the correct format and placed in the waiting folder. Results are written to the matched and unmatched folders. Incoming files are recorded in the archive/celerity folder. Refer to the README.md for more details.")
        .arg(Arg::with_name("charter_path")
            .help("The full path to the charter yaml file containing the instructions for matching")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("control_dir")
            .help("The base directory where data files will be processed. This should be distinct from any other control's directory")
            .required(true)
            .takes_value(true))
        .get_matches();

    dotenv::dotenv().ok();

    let charter_path = Path::new(options.value_of("charter_path").expect("no charter specified"));
    let base_path = Path::new(options.value_of("control_dir").expect("no control dir specififed"));
    let _handle = init_logging(&base_path);

    celerity::run_charter(charter_path, base_path)?;

    Ok(())
}

fn init_logging(base_path: &Path) -> Handle {

    // Set the log filter level.
    let log_filter = match std::env::var("RUST_LOG") {
        Ok(env_log) if env_log.to_lowercase().starts_with("trace") => LevelFilter::Trace,
        Ok(env_log) if env_log.to_lowercase().starts_with("debug") => LevelFilter::Debug,
        Ok(env_log) if env_log.to_lowercase().starts_with("warn")  => LevelFilter::Warn,
        Ok(env_log) if env_log.to_lowercase().starts_with("error") => LevelFilter::Error,
        Ok(env_log) if env_log.to_lowercase().starts_with("off")   => LevelFilter::Off,
        Ok(_)  => LevelFilter::Info,
        Err(_) => LevelFilter::Info,
    };

    // Create the logs folder.
    let log_path = base_path.join("logs/");
    fs::create_dir_all(&log_path).expect(&format!("cannot create log folder {}", log_path.to_string_lossy()));

    // Initialise logging.
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(CONSOLE_PATTERN)))
        .build();

    let log_file = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(FILE_PATTERN)))
        .build(&log_path.join(format!("{}_celerity.log", Utc::now().format("%Y%m%d").to_string())))
        .expect(&format!("cannot create log file appended to {}", log_path.to_string_lossy()));

    let config = Config::builder()
        .appender(Appender::builder().build("file", Box::new(log_file)))
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder()
            .appender("file")
            .appender("stdout")
            .build(log_filter))
        .unwrap();

    log4rs::init_config(config).unwrap()
}