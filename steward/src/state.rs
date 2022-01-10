use std::{time::{Duration, Instant}, thread, path::{Path, PathBuf}, sync::Arc};

use crate::register::{Register, self};

#[derive(Clone, Copy)]
pub enum ControlState {
    Started,
    Stopped,
    Suspended,
}

pub trait Task {
    fn started(&self) -> Instant;
    fn done(&self) -> bool;
    fn description(&self) -> String;
}

pub struct MatchJob {
}

impl Task for MatchJob {
    fn started(&self) -> Instant {
        todo!()
    }

    fn done(&self) -> bool {
        todo!()
    }

    fn description(&self) -> String {
        String::from("matching")
    }
}

///
/// Checks a folder for new files creates a monitor for any files it finds.
///
pub struct InboxScan {
    monitored: Vec<FileMonitor>, // Files being monitored.
}

// impl Task for InboxScan {
//     fn started(&self) -> Instant {
//         todo!()
//     }

//     fn done(&self) -> bool {
//         todo!()
//     }
// }

///
/// Watches an inbox file to see if it is still being written to.
///
pub struct FileMonitor {
    started: Instant,
    path: PathBuf,
    last_size: usize,
    last_probed: Instant,
}

// impl Task for FileMonitor {
//     fn started(&self) -> Instant {
//         todo!()
//     }

//     fn done(&self) -> bool {
//         todo!()
//     }
// }

pub struct Control {
    state: ControlState,
    inner: register::Control,
    tasks: Vec<Box<dyn Task>>,
    processing: Option<Box<dyn Task>>,
}

impl Control {
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    pub fn state(&self) -> ControlState {
        self.state
    }

    pub fn processing(&self) -> &Option<Box<dyn Task>> {
        &self.processing
    }
}

pub struct State {
    controls: Vec<Control>
}

impl State {
    pub fn new(register: &Register) -> Self {
        let controls = register.controls()
            .iter()
            .map(|c| {
                Control {
                    inner: c.clone(),
                    state: if c.disabled() {
                            ControlState::Stopped
                        } else {
                            // TEMP: for testing.
                            if crate::random(100, 10) {
                                ControlState::Suspended
                            } else {
                                ControlState::Started
                            }
                        },
                    tasks: vec!(),
                    processing: None,
                }
            })
            .collect();

        Self { controls }
    }

    pub fn controls(&self) -> &[Control] {
        &self.controls
    }
}