use anyhow::Result;
use clap::{App, Arg};

pub fn main() -> Result<()> {

    let options = App::new("seward")
        .version("1.0")
        .about("Stweard is a match job orchistrator for OpenRec and manages one or more control charters. Refer to the README.md for more details.")
        // .arg(Arg::with_name("charter_path")
        //     .help("The full path to the charter yaml file containing the instructions for matching")
        //     .required(true)
        //     .takes_value(true))
        // .arg(Arg::with_name("control_dir")
        //     .help("The base directory where data files will be processed. This should be distinct from any other control's directory")
        //     .required(true)
        //     .takes_value(true))
        .get_matches();

    dotenv::dotenv().ok();
    let _ = env_logger::try_init();

    // log::info!("{}", BANNER);

    steward::main_loop("./tmp/register.yml")?;
    // steward::run_charter(
    //     options.value_of("charter_path").expect("no charter specified"),
    //     options.value_of("control_dir").expect("no base dir specififed").into())?;

    Ok(())
}
