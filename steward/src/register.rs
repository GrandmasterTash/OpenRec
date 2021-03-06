use serde::Deserialize;
use core::charter::Charter;
use anyhow::{Context, Result};
use std::{path::{PathBuf, Path}, io::BufReader};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Register {
    controls: Vec<Control>
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Control {
    #[serde(default)]
    name: String,
    charter: PathBuf,
    root: PathBuf,

    #[serde(default)]
    disabled: bool,

    #[serde(skip)]
    parsed: bool,

    #[serde(skip)]
    parse_err: Option<String>,
}

impl Register {
    pub fn load(path: &Path) -> Result<Self, anyhow::Error> {
        let rdr = BufReader::new(std::fs::File::open(&path)
            .with_context(|| format!("attempting to open register {}", path.to_string_lossy()))?);

        let mut register: Self = serde_yaml::from_reader(rdr)
            .with_context(|| format!("parsing register {}", path.to_string_lossy()))?;

        // Attempt to parse each charter to get the control's name.
        for control in &mut register.controls {
            match Charter::load(control.charter()) {
                Ok(charter) => {
                    control.set_name(charter.name().to_string());
                    control.parsed = true;
                    control.parse_err = None;
                },
                Err(err) => {
                    control.set_name(control.charter().file_name().unwrap_or_default().to_string_lossy().to_string());
                    control.parsed = false;
                    control.parse_err = Some(err.to_string());
                },
            }
        }

        Ok(register)
    }

    pub fn controls(&self) -> &[Control] {
        &self.controls
    }
}

impl Control {
    pub fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn charter(&self) -> &Path {
        &self.charter
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn disabled(&self) -> bool {
        self.disabled
    }

    pub fn parsed(&self) -> bool {
        self.parsed
    }

    pub fn parse_err(&self) -> String {
        match &self.parse_err {
            Some(err) => err.to_string(),
            None => String::default(),
        }
    }
}