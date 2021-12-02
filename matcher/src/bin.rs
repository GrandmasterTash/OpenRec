use anyhow::Result;
use clap::{App, Arg};

pub fn main() -> Result<()> {

    let options = App::new("matcher")
        .version("1.0")
        .about("TODO: Some info here")
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

    matcher::run_charter(
        options.value_of("CHARTER").unwrap(),
        options.value_of("BASE_DIR").unwrap().into())?;

    Ok(())
}