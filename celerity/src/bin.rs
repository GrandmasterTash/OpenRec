use anyhow::Result;
use clap::{App, Arg};

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
    let _ = env_logger::try_init();

    // log::info!("{}", BANNER);

    celerity::run_charter(
        options.value_of("charter_path").expect("no charter specified"),
        options.value_of("control_dir").expect("no base dir specififed").into())?;

    Ok(())
}

// const BANNER: &str = r#"
//   ____     _           _ _
//  / ___|___| | ___ _ __(_) |_ _   _
// | |   / _ \ |/ _ \ '__| | __| | | |
// | |__|  __/ |  __/ |  | | |_| |_| |
//  \____\___|_|\___|_|  |_|\__|\__, |
//  OpenRec: Matching Engine    |___/
// "#;