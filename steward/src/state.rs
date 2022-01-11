use chrono::Local;
use crossbeam::channel;
use crate::{register::{Register, self}, do_match_job};
use std::{thread::JoinHandle, path::Path, slice::IterMut, fs, time::{Instant, Duration}, collections::VecDeque};

#[derive(Clone, Copy)]
pub enum ControlState {
    StartedIdle,
    StartedMatching,
    Stopped,
    Suspended,
}

pub struct Control {
    state: ControlState,
    state_changed: Instant,                // When did the state get state to it's current value.
    inner: register::Control,              // The parsed config from the register file.
    job: Option<JoinHandle<()>>,           // The handle to a thread running a match job.
    callback: Option<channel::Receiver<JobResult>>, // The job thread will call back to the main thread when it's done.
    queued: bool,                          // Start another job after the current has finished.
    inbox_files: Vec<String>,              // Filenames of files we know are in the inbox.
    messages: VecDeque<(Instant, String)>, // A queue of messages to display for the control.
}

pub struct State {
    controls: Vec<Control>
}

pub struct JobResult {
    success: bool,
    message: Option<String>,
}

impl Control {
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn state(&self) -> ControlState {
        self.state
    }

    pub fn suspend(&mut self) {
        self.state_changed = Instant::now();
        self.state = ControlState::Suspended;
    }

    pub fn is_running(&self) -> bool {
        match self.state {
            ControlState::StartedIdle     => true,
            ControlState::StartedMatching => true,
            ControlState::Stopped         => false,
            ControlState::Suspended       => false,
        }
    }

    // pub fn duration(&self) -> chrono::Duration {
    pub fn duration(&self) -> Duration {
        // chrono::Duration::from_std(self.state_changed.elapsed()).expect("bad duration")
        self.state_changed.elapsed()
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

    pub fn callback(&self) -> &Option<channel::Receiver<JobResult>> {
        &self.callback
    }

    pub fn queue_message(&mut self, msg: String) {
        self.messages.push_back((Instant::now(), msg));
    }

    ///
    /// If the current head of the message queue is older then 5s then pop and return the next
    /// message in the queue.
    ///
    pub fn next_message(&mut self) -> Option<String> {
        if !self.messages.is_empty() {
            if (self.messages.len() > 1)
                && (self.messages[0].0.elapsed() > Duration::from_secs(5)) {

                self.messages.pop_front();
            }

            if let Some((when, msg)) = self.messages.iter().next() {
                let when = Local::now() - chrono::Duration::from_std(when.elapsed()).expect("bad duration");
                return Some(format!("[{}] {}", when.format("%a %T"), msg)) // e.g. SUN 12:45:12
            }
        }
        None
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
                    self.queue_message(format!("Unable to create inbox {:?}", inbox));
                    log::error!("Unable to create inbox for control {name} at {inbox:?} : {err}",
                        name = self.name(),
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
                self.queue_message(format!("Unable to read inbox {:?}", inbox));
                log::error!("Unable to read inbox for control {name} : {err}",
                    name = self.name(),
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
            .filter(|f| !self.inbox_files.contains(f))
            .cloned()
            .collect::<Vec<String>>();

        self.inbox_files = contents;
        new_contents
    }

    ///
    /// Create a thread to spawn a matching job - or flip a flag if there's already a job in progress.
    ///
    pub fn queue_job(&mut self) {
        match self.job() {
            Some(_) => self.queued = true, // Queue the job.
            None => {
                let (s, r) = channel::unbounded();
                let control_name = self.name().to_string();
                let charter = self.charter().to_path_buf();
                let root = self.root().to_path_buf();
                self.state = ControlState::StartedMatching;
                self.callback = Some(r);
                self.job = Some(std::thread::spawn(|| do_match_job(control_name, charter, root, s)))
            },
        }
    }

    ///
    /// Mark the control as idle - after a job has completed.
    ///
    pub fn job_done(&mut self) {
        self.state_changed = Instant::now();
        self.state = ControlState::StartedIdle;
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
                    state: if c.disabled() || !c.parsed() {
                            ControlState::Stopped
                        } else {
                            ControlState::StartedIdle
                        },
                    state_changed: Instant::now(),
                    job: None,
                    callback: None,
                    queued: false,
                    inbox_files: vec!(),
                    messages: if c.parsed() {
                        VecDeque::new()
                    } else {
                        VecDeque::from([(Instant::now(), "Charter failed to parse".into())])
                    },
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


impl JobResult {
    pub fn new_success() -> Self {
        Self {
            success: true,
            message: None
        }
    }

    pub fn new_failure(msg: String) -> Self {
        Self {
            success: false,
            message: Some(msg),
        }
    }

    pub fn success(&self) -> bool {
        self.success
    }

    pub fn failure(&self) -> bool {
        !self.success
    }

    pub fn message(&self) -> &Option<String> {
        &self.message
    }
}