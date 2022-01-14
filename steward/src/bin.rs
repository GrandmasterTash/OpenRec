use clap::{App, Arg};
use anyhow::Result;

pub fn main() -> Result<()> {

    let options = App::new("steward")
        .version("1.0")
        .about("Steward is a match job orchistrator for OpenRec and manages one or more controls. Refer to the README.md for more details.")
        .arg(Arg::with_name("register_path")
            .help("The full path to the register yaml file containing all the controls to manage,")
            .required(true)
            .default_value("/etc/openrec/register.yml")
            .takes_value(true))
        .arg(Arg::with_name("pushgateway_address")
            .help("The address to a prometheus pushgateway instance used to publish metrics to, eg. 'localhost:9091'")
            .required(false)
            .takes_value(true))
        .get_matches();

    dotenv::dotenv().ok();
    let _ = env_logger::try_init();

    steward::main_loop(
        options.value_of("register_path").expect("no registry specified"),
        options.value_of("pushgateway_address")
    )?;

    Ok(())
}
