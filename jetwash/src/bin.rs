use anyhow::Result;
use clap::{App, Arg};

pub fn main() -> Result<()> {

    let options = App::new("jetwash")
        .version("1.0")
        .about("Jetwash is a CSV file pre-processor to scrub and transform data before it is passed to Celerity for matching.")
        .arg(Arg::with_name("charter_path")
            .help("The full path to the charter yaml file containing the instructions for jetwash")
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

    jetwash::run_charter(
        options.value_of("charter_path").expect("no charter specified"),
        options.value_of("control_dir").expect("no base dir specififed"),
        None)?;

    Ok(())
}

// const BANNER: &str = r#"
//   ____    ___ ______  __    __   ____  _____ __ __
//  |    |  /  _]      ||  |__|  | /    |/ ___/|  |  |
//  |__  | /  [_|      ||  |  |  ||  o  (   \_ |  |  |
//  __|  ||    _]_|  |_||  |  |  ||     |\__  ||  _  |
// /  |  ||   [_  |  |  |  `  '  ||  _  |/  \ ||  |  |
// \  `  ||     | |  |   \      / |  |  |\    ||  |  |
//  \____j|_____| |__|    \_/\_/  |__|__| \___||__|__|
//  OpenRec: Data Importer & Cleanser
// "#;