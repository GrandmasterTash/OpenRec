use std::{time::{Duration, Instant}, thread::{self, JoinHandle}, path::{Path, PathBuf}, sync::Arc, slice::IterMut, fs};

use crossbeam::channel;

use crate::{register::{Register, self}, do_match_job};

#[derive(Clone, Copy)]
pub enum ControlState {
    Started,
    Stopped,
    Suspended,
}

// pub trait Task {
//     fn started(&self) -> Instant;
//     fn done(&self) -> bool;
//     fn description(&self) -> String;
// }

// pub struct MatchJob {
// }

// impl Task for MatchJob {
//     fn started(&self) -> Instant {
//         todo!()
//     }

//     fn done(&self) -> bool {
//         todo!()
//     }

//     fn description(&self) -> String {
//         String::from("matching")
//     }
// }

pub struct Control {
    state: ControlState,
    inner: register::Control,
    job: Option<JoinHandle<()>>, // The handle to a thread running a match job.
    callback: Option<channel::Receiver<bool>>, // The job thread will call back to the main thread when it's done.
    queued: bool,                // Start another job after the current has finished.
    inbox_monitors: Vec<String>  // Filenames of files we know are in the inbox.
}

pub struct State {
    controls: Vec<Control>
}

impl Control {
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    pub fn state(&self) -> ControlState {
        self.state
    }

    pub fn suspend(&mut self) {
        self.state = ControlState::Suspended;
    }

    pub fn is_running(&self) -> bool {
        match self.state {
            ControlState::Started   => true,
            ControlState::Stopped   => false,
            ControlState::Suspended => false,
        }
    }

    pub fn charter(&self) -> &Path {
        self.inner.charter()
    }

    pub fn root(&self) -> &Path {
        self.inner.root()
    }

    pub fn job(&self) -> &Option<JoinHandle<()>> {
        &self.job
    }

    pub fn callback(&self) -> &Option<channel::Receiver<bool>> {
        &self.callback
    }

    ///
    /// Check the control's inbox and return any NEW files since our last check.
    ///
    pub fn scan_inbox(&mut self) -> Vec<String> {

        // Create the inbox if required.
        let inbox = self.inner.root().join("inbox");
        if !inbox.exists() {
            match fs::create_dir_all(&inbox) {
                Ok(_) => {},
                Err(err) => {
                    self.state = ControlState::Suspended;
                    log::error!("Unable to create in for control {id} at {inbox:?} : {err}",
                        id = self.id(),
                        inbox = inbox,
                        err = err);
                    return vec!()
                },
            }
        }

        // Get all the files in the inbox.
        let contents = match fs_extra::dir::get_dir_content(&inbox) {
            Ok(con) => con,
            Err(err) => {
                self.state = ControlState::Suspended;
                log::error!("Unable to read inbox for control {id} : {err}",
                    id = self.id(),
                    err = err);
                return vec!()
            },
        };

        let contents = contents.files
            .iter()
            .filter(|f| !f.ends_with(".inprogress"))
            .cloned()
            .collect::<Vec<String>>();

        let new_contents = contents
            .iter()
            .filter(|f| !self.inbox_monitors.contains(f))
            .cloned()
            .collect::<Vec<String>>();

        self.inbox_monitors = contents;
        new_contents
    }

    pub fn queue_job(&mut self) {
        match self.job() {
            Some(_) => self.queued = true, // Queue the job.
            None => {
                let (s, r) = channel::unbounded();
                let control_id = self.id().to_string();
                let charter = self.charter().to_path_buf();
                let root = self.root().to_path_buf();
                self.callback = Some(r);
                self.job = Some(std::thread::spawn(|| do_match_job(control_id, charter, root, s)))
            },
        }
    }

    pub fn job_done(&mut self) {
        self.callback = None;
        self.job = None;
    }
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
                            // // TEMP: for testing.
                            // if crate::random(100, 10) {
                            //     ControlState::Suspended
                            // } else {
                                ControlState::Started
                            // }
                        },
                    job: None,
                    callback: None,
                    queued: false,
                    inbox_monitors: vec!(),
                }
            })
            .collect();

        Self { controls }
    }

    pub fn controls(&self) -> &[Control] {
        &self.controls
    }

    pub fn controls_mut(&mut self) -> IterMut<'_, Control> {
        self.controls.iter_mut()
    }
}