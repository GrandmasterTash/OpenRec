use anyhow::Result;

pub fn main() -> Result<()> {
    // TODO: Clap this up!

    // TODO: Clap interface and a lib interface.
    // let charter = Charter::load("../examples/03-Net-With-Tolerance.yaml")?;
    let charter = "../examples/04-3-Way-Match.yaml";
    // let charter = Charter::load("../examples/06-Advanced-Scripts.yaml")?;
    // let charter = Charter::load("../examples/09-3-Way-Performance.yaml")?;

    matcher::run_charter(charter, "./tmp".into())?;
    Ok(())
}