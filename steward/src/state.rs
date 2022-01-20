use regex::Regex;
use chrono::Local;
use crossbeam::channel;
use lazy_static::lazy_static;
use fs_extra::dir::get_dir_content;
use prometheus::{Registry, Histogram, Opts, HistogramOpts, IntGauge, labels};
use crate::{register::{Register, self}, do_match_job, find_latest_match_file};
use std::{thread::JoinHandle, path::{Path, PathBuf}, slice::IterMut, fs, time::{Instant, Duration}, io::BufReader};

lazy_static! {
    pub static ref MATCH_JOB_FILENAME_REGEX: Regex = Regex::new(r".*(\d{8}_\d{9})_matched\.json$").expect("bad regex for FILENAME_REGEX");
}

#[derive(Clone, Copy, PartialEq)]
pub enum ControlState {
    StartedIdle,
    StartedQueued,
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
    latest_report: Option<PathBuf>,        // The latest match report file.
    message: String,                       // A message to display next to the control.
    metrics: ControlMetrics,
}

pub struct State {
    register: PathBuf,
    controls: Vec<Control>,
}

#[derive(PartialEq)]
pub enum JobResult {
    Started,
    Completed { success: bool, message: Option<String> },
}

pub struct ControlMetrics {
    registry: Registry,
    unmatched_txs: Box<IntGauge>,
    matched_txs: Box<IntGauge>,
    matched_groups: Box<IntGauge>,
    disk_usage_bytes: Box<IntGauge>,
    inbox_usage_bytes: Box<IntGauge>,
    outbox_usage_bytes: Box<IntGauge>,
    jetwash_duration: Box<Histogram>,
    celerity_duration: Box<Histogram>,
}

impl Control {
    fn new(c: &register::Control) -> Self {
        let latest_match_file = find_latest_match_file(c.root());

        // Suspend un-parseable controls, unless they are already disabled.
        let state = if c.parsed() && !c.disabled() {
            ControlState::StartedIdle
        } else if c.disabled() {
            ControlState::Stopped
        } else {
            ControlState::Suspended
        };

        Self {
            inner: c.clone(),
            state,
            state_changed: Instant::now(),
            job: None,
            callback: None,
            queued: false,
            latest_report: latest_match_file.clone(),
            inbox_files: vec!(),
            message: if c.parsed() {
                String::default()
            } else {
                c.parse_err()
            },
            metrics: ControlMetrics::new(c.name(), &latest_match_file),
        }
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn state(&self) -> ControlState {
        self.state
    }

    pub fn start(&mut self) {
        self.state_changed = Instant::now();
        self.state = ControlState::StartedMatching;
    }

    pub fn stop(&mut self) {
        self.state_changed = Instant::now();
        self.state = ControlState::Stopped;
    }

    pub fn suspend(&mut self, msg: &str) {
        self.set_message(msg.into());
        self.state_changed = Instant::now();
        self.state = ControlState::Suspended;
    }

    pub fn is_running(&self) -> bool {
        match self.state {
            ControlState::StartedIdle     => true,
            ControlState::StartedQueued   => true,
            ControlState::StartedMatching => true,
            ControlState::Stopped         => false,
            ControlState::Suspended       => false,
        }
    }

    pub fn duration(&self) -> Duration {
        self.state_changed.elapsed()
    }

    pub fn update_metrics(&mut self) -> &Registry {
        // The job_done handler will keep the matched/unmatched counts up to date.

        // Use the disk scrape fns below to do this.
        self.metrics.inbox_usage_bytes.set(self.inbox_len() as i64);
        self.metrics.outbox_usage_bytes.set(self.outbox_len() as i64);
        self.metrics.disk_usage_bytes.set(self.root_len() as i64);

        &self.metrics.registry
    }

    pub fn unmatched(&self) -> usize {
        self.metrics.unmatched_txs.get() as usize
    }

    pub fn charter(&self) -> &Path {
        self.inner.charter()
    }

    pub fn root(&self) -> &Path {
        self.inner.root()
    }

    pub fn latest_report(&self) -> &Option<PathBuf> {
        &self.latest_report
    }

    pub fn set_latest_report(&mut self, latest_report: PathBuf) {
        self.latest_report = Some(latest_report.clone());

        // Set the metrics from this match report now.
        if let Some(footer) = get_json_report_footer(&latest_report) {
             self.metrics.unmatched_txs.set(footer["unmatched_records"].as_u64().map(|u| u as usize).unwrap_or_default() as i64);
             self.metrics.matched_txs.set(footer["matched_records"].as_u64().map(|u| u as usize).unwrap_or_default() as i64);
             self.metrics.matched_groups.set(footer["matched_groups"].as_u64().map(|u| u as usize).unwrap_or_default() as i64);
        }
    }

    pub fn job(&self) -> &Option<JoinHandle<()>> {
        &self.job
    }

    pub fn callback(&self) -> &Option<channel::Receiver<JobResult>> {
        &self.callback
    }

    ///
    /// If additional inbox data was found whilst already running a job return true.
    ///
    /// Note: This is not the same state as 'Running - queued'.
    ///
    pub fn is_more(&self) -> bool {
        self.queued
    }

    pub fn set_message(&mut self, msg: String) {
        self.message = format!("[{}] {}", Local::now().format("%a %T"), msg) // e.g. SUN 12:45:12
    }

    pub fn message(&mut self) -> &str {
        &self.message
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
                    self.set_message(format!("Unable to create inbox {:?}", inbox));
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
                self.set_message(format!("Unable to read inbox {:?}", inbox));
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
        self.set_message("Running match job".into());
        match self.job() {
            Some(_) => self.queued = true, // Queue the job. Note this is not the same as being in a
                                           // queued state - it means we have a follow-up job to run
                                           // after our current job.
            None => {
                let (s, r) = channel::unbounded();
                let control_name = self.name().to_string();
                let charter = self.charter().to_path_buf();
                let root = self.root().to_path_buf();
                let jetwash_histogram = self.metrics.jetwash_duration.clone();
                let celerity_histogram = self.metrics.celerity_duration.clone();
                self.state = ControlState::StartedQueued;
                self.callback = Some(r);
                self.queued = false;
                self.job = Some(std::thread::spawn(|| do_match_job(control_name, charter, root, s, jetwash_histogram, celerity_histogram)))
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

    pub fn inbox_len(&self) -> usize {
        let inbox = self.inner.root().join("inbox");
        if inbox.exists() {
            if let Ok(contents) = get_dir_content(inbox) {
                return contents.dir_size as usize
            }
        }
        0
    }

    pub fn outbox_len(&self) -> usize {
        let outbox = self.inner.root().join("outbox");
        if outbox.exists() {
            if let Ok(contents) = get_dir_content(outbox) {
                return contents.dir_size as usize
            }
        }
        0
    }

    pub fn root_len(&self) -> usize {
        let dir = self.inner.root();
        if dir.exists() {
            if let Ok(contents) = get_dir_content(dir) {
                return contents.dir_size as usize
            }
        }
        0
    }
}

impl State {
    pub fn new(register: &Register, path: &Path) -> Self {
        let controls = register.controls()
            .iter()
            .map(Control::new)
            .collect();

        Self {
            controls,
            register: path.to_path_buf(),
        }
    }

    pub fn register(&self) -> &Path {
        &self.register
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
        Self::Completed {
            success: true,
            message: None
        }
    }

    pub fn new_failure(msg: String) -> Self {
        Self::Completed {
            success: false,
            message: Some(msg),
        }
    }
}

impl ControlMetrics {
    pub fn new(control_name: &str, latest_match_file: &Option<PathBuf>) -> Self {
        let me = Self {
            registry: Registry::new_custom(Some("control".into()), Some(labels! { "control_name".into() => control_name.into(), })).expect("bad registry"),
            unmatched_txs: Box::new(IntGauge::with_opts(Opts::new("unmatched_txs", "the number of unmatched records this control currently has")).expect("bad opts")),
            matched_txs: Box::new(IntGauge::with_opts(Opts::new("matched_txs", "the number of matched records this control currently has")).expect("bad opts")),
            disk_usage_bytes: Box::new(IntGauge::with_opts(Opts::new("disk_usage_bytes", "the total disk usage for this control in bytes")).expect("bad opts")),
            inbox_usage_bytes: Box::new(IntGauge::with_opts(Opts::new("inbox_size_bytes", "the inbox folder disk usage for this control in bytes")).expect("bad opts")),
            outbox_usage_bytes: Box::new(IntGauge::with_opts(Opts::new("outbox_size_bytes", "the outbox folder disk usage for this control in bytes")).expect("bad opts")),
            matched_groups: Box::new(IntGauge::with_opts(Opts::new("matched_groups", "the number of matched groups this control currently has")).expect("bad opts")),
            jetwash_duration: Box::new(Histogram::with_opts(HistogramOpts::new("jetwash_duration", "the duration of the jetwash phase of a match job")).expect("bad opts")),
            celerity_duration: Box::new(Histogram::with_opts(HistogramOpts::new("celerity_duration", "the duration of the celerity phase of a match job")).expect("bad opts")),
        };

        me.registry.register(me.unmatched_txs.clone()).expect("bad metric");
        me.registry.register(me.matched_txs.clone()).expect("bad metric");
        me.registry.register(me.disk_usage_bytes.clone()).expect("bad metric");
        me.registry.register(me.inbox_usage_bytes.clone()).expect("bad metric");
        me.registry.register(me.outbox_usage_bytes.clone()).expect("bad metric");
        me.registry.register(me.matched_groups.clone()).expect("bad metric");
        me.registry.register(me.jetwash_duration.clone()).expect("bad metric");
        me.registry.register(me.celerity_duration.clone()).expect("bad metric");

        // Get the latest match report statistics if available.
        if let Some(match_report) = latest_match_file {
            if let Some(footer) = get_json_report_footer(match_report) {
                me.unmatched_txs.set(footer["unmatched_records"].as_u64().map(|u| u as usize).unwrap_or_default() as i64);
                me.matched_txs.set(footer["matched_records"].as_u64().map(|u| u as usize).unwrap_or_default() as i64);
                me.matched_groups.set(footer["matched_groups"].as_u64().map(|u| u as usize).unwrap_or_default() as i64);
            }
        }

        me
    }
}

///
/// Return the footer section of the match report.
///
fn get_json_report_footer(matched_json: &Path) -> Option<serde_json::Value> {
    if let Ok(file) = fs::File::open(matched_json) {
        let reader = BufReader::new(file);
        if let Ok(json) = serde_json::from_reader::<_, serde_json::Value>(reader) {
            if let Some(details) = json.get(2) {
                return Some(details.to_owned())
            }
        }
    }

    None
}