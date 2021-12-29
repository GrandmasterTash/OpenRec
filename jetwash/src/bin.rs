use anyhow::Result;
use clap::{App, Arg};

pub fn main() -> Result<()> {

    let options = App::new("jetwash")
        .version("1.0")
        .about("TODO: Some info here") // TODO: Cmdline help
        .arg(Arg::with_name("CHARTER")
            .help("The charter yaml file")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("BASE_DIR")
            .help("The base directory where data files will be processed")
            .required(true)
            .takes_value(true))
        .get_matches();

    dotenv::dotenv().ok();
    let _ = env_logger::try_init();

    log::info!("{}", BANNER);

    jetwash::run_charter(
        options.value_of("CHARTER").unwrap(),
        options.value_of("BASE_DIR").unwrap().into())?;

    Ok(())
}

const BANNER: &str = r#"
  ____    ___ ______  __    __   ____  _____ __ __
 |    |  /  _]      ||  |__|  | /    |/ ___/|  |  |
 |__  | /  [_|      ||  |  |  ||  o  (   \_ |  |  |
 __|  ||    _]_|  |_||  |  |  ||     |\__  ||  _  |
/  |  ||   [_  |  |  |  `  '  ||  _  |/  \ ||  |  |
\  `  ||     | |  |   \      / |  |  |\    ||  |  |
 \____j|_____| |__|    \_/\_/  |__|__| \___||__|__|
 OpenRec: Data Cleanser
"#;