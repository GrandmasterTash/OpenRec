mod state;
mod display;
mod metrics;
mod register;

use chrono::Utc;
use crossbeam::channel;
use parking_lot::Mutex;
use register::Register;
use itertools::Itertools;
use prometheus::Histogram;
use anyhow::{Result, bail};
use lazy_static::lazy_static;
use std_semaphore::Semaphore;
use fs_extra::dir::get_dir_content;
use std::io::{Write, stdout, Read, BufReader};
use termion::{terminal_size, raw::IntoRawMode};
use state::{State, JobResult, ControlState, Control, MATCH_JOB_FILENAME_REGEX};
use std::{time::Duration, thread, path::{Path, PathBuf}, process::Command, fs};

// TODO: Jetwash and celerity should create a .lock - prohibit starting a job if exists - incase of steward hang.
// TODO: Default steward to noop - then use --ui --headless to control start mode.
// TODO: Headless mode with Ctrl..c graceful shtdown
// TODO: Recover unpublished outbox files on start-up (i.e. make it safe to kill sentinal).

lazy_static! {
    static ref FORCE_QUIT: Mutex<bool> = Mutex::new(false);
    static ref SEMAPHORE: Semaphore = Semaphore::new(num_cpus::get() as isize);
}

#[derive(Clone, Copy, PartialEq)]
pub enum AppState  {
    Running,
    Reloading,
    Terminating,
}

pub fn main_loop<P: AsRef<Path>>(register_path: P, pushgateway: Option<&str>) -> Result<()> {

    // Check jetwash and celerity are where we expect them.
    check_child_binaries()?;

    // Parse and load the register of controls into a state model.
    let mut state = load_state(register_path.as_ref())?;
    let mut app_state = AppState::Running;

    // Initialise the terminal and input buffers.
    let stdout = stdout();
    let mut stdout = stdout.lock().into_raw_mode().unwrap();
    let mut stdin = termion::async_stdin().bytes();

    // Display the top panel.
    display::init(&mut stdout);

    // Measure the screen area.
    let mut terminal_size = terminal_size().unwrap();

    // Main application loop.
    loop {
        app_state = handle_keyboard(app_state, stdin.next());

        // Stop any controls which can be stopped - if required.
        if app_state != AppState::Running {
            for control in state
                .controls_mut()
                .filter(|c| c.state() == ControlState::StartedIdle)
                .collect::<Vec<&mut Control>>() {
                control.stop();
            }
        }

        // Render the controls which will fit in the terminal
        terminal_size = display::display(&mut stdout, &mut state, &app_state, terminal_size);

        for control in state.controls_mut() {
            if !control.is_running() {
                continue
            }

            // Is a running job complete?
            handle_job_done(control);

            // Are there new files to process?
            check_inbox(control);
        }

        // Check if we can quit or reload.
        match app_state {
            AppState::Running => {},
            AppState::Reloading => {
                if state.controls().iter().all(|c| !c.is_running()) {
                    state = load_state(register_path.as_ref())?;
                    app_state = AppState::Running;
                }
            },
            AppState::Terminating => {
                if *FORCE_QUIT.lock() || state.controls().iter().all(|c| !c.is_running()) {
                    write!(stdout, "{}", termion::cursor::Show).unwrap();
                    stdout.suspend_raw_mode().expect("keep it raw");
                    println!("\nSteward terminated.");
                    return Ok(())
                }
            },
        }

        metrics::push(pushgateway, &mut state);

        // Shush for a bit.
        thread::sleep(Duration::from_millis(500));
    }
}

///
/// Load a state model using the charter file specified.
///
fn load_state(register_path: &Path) -> Result<State, anyhow::Error> {

    // Parse and load the register.
    let register = Register::load(register_path)?;

    // Build a state engine to track control states and task queues.
    Ok(State::new(&register, register_path))
}

///
/// Process any keyboard input if approriate.
///
fn handle_keyboard(app_state: AppState, key: Option<Result<u8, std::io::Error>>) -> AppState {
    // Ignore input if we're reloading or terminating.
    if app_state == AppState::Running {
        if let Some(Ok(b'q')) = key {
            return AppState::Terminating;
        }

        if let Some(Ok(b'r')) = key {
            return AppState::Reloading;
        }
    }

    if app_state == AppState::Terminating {
        if let Some(Ok(b'f')) = key {
            *FORCE_QUIT.lock() = true;
        }
    }

    app_state
}

///
/// Queue a match job if there are NEW files in the inbox.
///
fn check_inbox(control: &mut Control) {
    if !control.scan_inbox().is_empty() { // Push this into fn.
        control.queue_job();
    }
}

///
/// Ensure the jetwash binary and celerity binary are where we expect them to be.
///
fn check_child_binaries() -> Result<()> {

    if !Path::new(&jetwash()).exists() {
        bail!("The Jetwash binary '{}' is not found - you can use JETWASH_HOME to force it's location to be know", jetwash())
    }

    if !Path::new(&celerity()).exists() {
        bail!("The Celerity binary '{}' is not found - you can use CELERITY_HOME to force it's location to be know", celerity())
    }

    Ok(())
}

fn jetwash() -> String {
    format!("{}jetwash", std::env::var("JETWASH_HOME").unwrap_or_else(|_| "./".into()))
}

fn celerity() -> String {
    format!("{}celerity", std::env::var("CELERITY_HOME").unwrap_or_else(|_| "./".into()))
}

///
/// Initiate a match job (jetwash then celerity).
///
/// This is called on a seperate thread and notifies the main thread of the result via a channel.
///
fn do_match_job(
    control_id: String,
    charter: PathBuf,
    root: PathBuf,
    sender: channel::Sender<JobResult>,
    jetwash_histogram: Box<Histogram>,
    celerity_histogram: Box<Histogram>) {

    // Block until capacity is available to run the job.
    let _guard = SEMAPHORE.access();
    let _ignored = sender.send(JobResult::Started);

    // JETWASH
    let _jw_timer = jetwash_histogram.start_timer();
    match Command::new(jetwash()).arg(&charter).arg(&root).output() {
        Ok(output) => {
            if !output.status.success() {
                let _ignore = sender.send(JobResult::new_failure(format!("{} jetwash status: {}", control_id, output.status)));
                return
            }
        },
        Err(err) => {
            let _ignore = sender.send(JobResult::new_failure(format!("{} failed to run jetwash: {}", control_id, err)));
            return
        },
    }

    // CELERITY
    let _c_timer = celerity_histogram.start_timer();
    match Command::new(celerity()).arg(&charter).arg(&root).output() {
        Ok(output) => {
            if !output.status.success() {
                let _ignore = sender.send(JobResult::new_failure(format!("{} celerity status: {}", control_id, output.status)));
                return
            }
        },
        Err(err) => {
            let _ignore = sender.send(JobResult::new_failure(format!("{} failed to run celerity: {}", control_id, err)));
            return
        },
    }

    let _ignore = sender.send(JobResult::new_success());
}

///
/// Check if the control has completed a match job and needs state updating.
///
fn handle_job_done(control: &mut Control) {

    // Is a running job complete?
    if let Some(callback) = control.callback() {
        if let Ok(result) = callback.try_recv() {
            match result {
                JobResult::Started => control.start(),
                JobResult::Completed { success, message } => {
                    control.job_done();

                    if success {
                        // Has the latest report changed?
                        let latest = find_latest_match_file(control.root());
                        if  latest.is_some() && (latest != *control.latest_report()) {
                            // Package results into outbox.
                            let latest = latest.expect("latest");
                            let filename = latest.file_name().expect("filename").to_string_lossy().to_string();

                            // Get the ts from it's name.
                            let ts = timestamp(&latest);
                            let out_dir = control.root().join("outbox").join(ts);

                            // Create an outbox folder.
                            if let Err(err) = fs::create_dir_all(&out_dir) {
                                control.suspend(&format!("Can't create outbox: {}", err));
                                return
                            }

                            // Copy the match report into the outbox/ts/ folder.
                            if let Err(err) = fs::copy(&latest, out_dir.join(filename)) {
                                control.suspend(&format!("Can't copy match report: {}", err));
                                return
                            }

                            // Copy all the unmatched files from the report into the outbox/ts folder
                            match unmatched_filenames(&latest) {
                                Ok(filenames) => {
                                    for filename in filenames {
                                        let path = control.root().join("unmatched").join(&filename);
                                        if let Err(err) = fs::copy(&path, out_dir.join(&filename)) {
                                            control.suspend(&format!("Can't copy unmatched file {} to outbox : {}", filename, err));
                                            return
                                        }
                                    }
                                },
                                Err(err) => {
                                    control.suspend(&format!("Can't find the unmatched files: {}", err));
                                    return
                                },
                            }

                            // Update the latest match report in the control.
                            control.set_latest_report(latest);
                        }

                        control.set_message("Match job complete".into());

                        if control.is_more() {
                            control.queue_job();
                        }

                    } else {
                        control.suspend(message.as_ref().expect("should have message"));
                    }
                },
            }
        }
    }
}

///
/// Looks for the latest match job report file in the folder structure provided.
///
pub fn find_latest_match_file(root: &Path) -> Option<PathBuf> {

    let latest = match get_dir_content(root.join("matched")) {
        Ok(dir) => dir.files.iter().filter(|f| MATCH_JOB_FILENAME_REGEX.is_match(f)).sorted().max().cloned(),
        Err(_) => return None,
    };

    if let Some(latest) = latest {
        let p = PathBuf::from(latest);
        if p.exists() {
            return Some(p)
        }
    }

    None
}

///
/// Parse the unmatched files from the match report and return the filenames
///
pub fn unmatched_filenames(match_file: &Path) -> Result<Vec<String>, anyhow::Error> {
    let file = fs::File::open(match_file)?;
    let reader = BufReader::new(file);
    let json: serde_json::Value = serde_json::from_reader(reader)?;
    match json.get(2) {
        Some(json) => Ok(json["unmatched"]
            .as_array()
            .unwrap_or(&vec!())
            .iter()
            .map(|un| un["file"].as_str().unwrap().to_string() )
            .collect::<Vec<String>>()),
        None => Ok(vec!()),
    }
}

///
/// Retrun the timestamp prefix from the filename.
///
/// If for any reason this is not possible - return a new timestamp.
///
pub fn timestamp(path: &Path) -> String {

    if let Some(filename) = path.file_name() {
        let filename = filename.to_string_lossy().to_string();

        if let Some(captures) = MATCH_JOB_FILENAME_REGEX.captures(&filename) {
            if captures.len() == 3 {
                return captures.get(1).map(|ts|ts.as_str()).unwrap_or("").to_string()
            }
        }
    }

    // Fall-back to a default timestamp.
    Utc::now().format("%Y%m%d_%H%M%S%3f").to_string()
}