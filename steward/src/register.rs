use serde::Deserialize;
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
    id: String,
    charter: PathBuf,
    root: PathBuf,

    #[serde(default)]
    disabled: bool
}

impl Control {
    // TEMP: Temporary - just to test the UI layout.
    fn random(idx: usize) -> Self {
        Self {
            id: format!("Control {}", idx),
            charter: PathBuf::from(format!("./tmp/control_{}.yml", idx)),
            root: PathBuf::from(format!("./tmp/control{}/root/", idx)),
            disabled: crate::random(100, 20),
        }
    }
}

impl Register {
    pub fn load(path: &Path) -> Result<Self, anyhow::Error> {
        Ok(Self {
            controls: (1..151).map(|idx| Control::random(idx)).collect()
        })

        // let rdr = BufReader::new(std::fs::File::open(&path)
        //     .with_context(|| format!("attempting to open register {}", path.to_string_lossy()))?);

        // Ok(serde_yaml::from_reader(rdr)
        //     .with_context(|| format!("parsing register {}", path.to_string_lossy()))?)
    }

    pub fn controls(&self) -> &[Control] {
        &self.controls
    }
}

impl Control {
    pub fn id(&self) -> &str {
        &self.id
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
}