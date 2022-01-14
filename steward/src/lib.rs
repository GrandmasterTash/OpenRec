mod state;
mod display;
mod metrics;
mod register;

use chrono::Utc;
use anyhow::Result;
use crossbeam::channel;
use register::Register;
use itertools::Itertools;
use fs_extra::dir::get_dir_content;
use std::io::{Write, stdout, Read, BufReader};
use termion::{terminal_size, raw::IntoRawMode};
use state::{State, JobResult, ControlState, Control, MATCH_JOB_FILENAME_REGEX};
use std::{time::Duration, thread, path::{Path, PathBuf}, process::Command, fs};

// TODO: Prometheus export. https://github.com/tikv/rust-prometheus/blob/master/examples/example_push.rs
// TODO: Semaphore to limit concurrent match jobs. num_cpus by default - or use memory tickets?
// TODO: Document the above .inprogress inclusion. Ensure Jetwash NEVER processes .inprogress inbox files - regardless of regex.
// BUG: Adding data during a job doesn't trigger a follow-up job.
// TODO: Add a nice error message when registry not found. as it's probably the first thing anyone will see!
// TODO: Unparseable charter should = suspend not stopped state.
// TODO: Document bins must be in the same folder unless the HOME env vars are set.
// TODO: F to force app to terminate - ex, if a match job is 'hung'
// TODO: Start-up - validate we can find jetwash and celerity binaries.

#[derive(PartialEq)]
pub enum AppState  {
    Running,
    Reloading,
    Terminating,
}

pub fn main_loop<P: AsRef<Path>>(register_path: P, pushgateway: Option<&str>) -> Result<()> {

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
        let key = stdin.next();

        // Ignore input if we're reloading or terminating.
        if app_state == AppState::Running {
            if let Some(Ok(b'q')) = key {
                app_state = AppState::Terminating;
            }

            if let Some(Ok(b'r')) = key {
                app_state = AppState::Reloading;
            }
        }

        // Stop any controls which can be stopped - if rquired.
        if app_state != AppState::Running {
            for control in state
                .controls_mut()
                .filter(|c| c.state() == ControlState::StartedIdle)
                .collect::<Vec<&mut Control>>() {
                control.stop();
            }
        }

        // Render the controls which will fit in the terminal
        terminal_size = display::display(&mut stdout, &mut state, &app_state, terminal_size); // TODO: Pass app-state and display at top - shutting down.... or reloading....

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
                if state.controls().iter().all(|c| !c.is_running()) {
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

    // TODO: Validate roots are unique to each control.

    // Build a state engine to track control states and task queues.
    Ok(State::new(&register, register_path.file_name().expect("register has no filename").to_string_lossy().to_string()))
}

///
/// Queue a match job if there are NEW files in the inbox.
///
fn check_inbox(control: &mut Control) {
    if !control.scan_inbox().is_empty() { // Push this into fn.
        control.queue_message("Running match job".into());
        control.queue_job(); // TODO: Above into param for queue_job
    }
}

///
/// Initiate a match job (jetwash then celerity).
///
/// This is called on a seperate thread and notifies the main thread of the result via a channel.
///
fn do_match_job(control_id: String, charter: PathBuf, root: PathBuf, sender: channel::Sender<JobResult>) {

    // JETWASH
    let output = Command::new(format!("{}jetwash", std::env::var("JETWASH_HOME").unwrap_or("./".into())))
        .arg(&charter)
        .arg(&root)
        .output()
        .expect("failed to execute jetwash"); // TODO: Don't unwrap - suspend control and display msg.

    if !output.status.success() {
        // Notify the main thread this control has failed.
        let _ignore = sender.send(JobResult::new_failure(format!("{} - Jetwash status: {}", control_id, output.status)));
        return
    }

    // CELERITY
    let output = Command::new(format!("{}celerity", std::env::var("CELERITY_HOME").unwrap_or("./".into())))
        .arg(charter)
        .arg(&root)
        .output()
        .expect("failed to execute celerity"); // TODO: Don't unwrap - suspend control and display msg.

    // Notify the main thread this control has finished.
    if output.status.success() {
        let _ignore = sender.send(JobResult::new_success());

    } else {
        let _ignore = sender.send(JobResult::new_failure(format!("{} - Celerity status: {}", control_id, output.status)));
    }
}

///
/// Check if the control has completed a match job and needs state updating.
///
fn handle_job_done(control: &mut Control) {

    // Is a running job complete?
    if let Some(callback) = control.callback() {
        if let Ok(result) = callback.try_recv() {
            control.job_done();

            if result.failure() {
                control.suspend(); // TODO: put message in suspend fn param.
                control.queue_message(result.message().as_ref().expect("should have message").clone());

            } else {
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
                    fs::create_dir_all(&out_dir).unwrap(); // TODO: Don't unwrap, log message and suspend control.

                    // Copy the match report into the outbox/ts/ folder.
                    fs::copy(&latest, out_dir.join(filename)).unwrap(); // TODO: Don't unwrap, log message and suspend control.

                    // Copy all the unmatched files from the report into the outbox/ts folder
                    for filename in unmatched_filenames(&latest) {
                        let path = control.root().join("unmatched").join(&filename);
                        fs::copy(&path, out_dir.join(filename)).unwrap(); // TODO: Don't unwrap, log message and suspend control.
                    }

                    // Update the latest match report in the control.
                    control.set_latest_report(latest);
                }

                control.queue_message("Match job complete".into());
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
pub fn unmatched_filenames(match_file: &PathBuf) -> Vec<String> {
    let file = fs::File::open(match_file).unwrap(); // TODO: Don't unwrap, log message and suspend control. so return err
    let reader = BufReader::new(file);
    let json: serde_json::Value = serde_json::from_reader(reader).unwrap(); // TODO: Don't unwrap, log message and suspend control.
    match json.get(2) {
        Some(json) => json["unmatched"]
            .as_array()
            .unwrap() // TODO: return err, dont unwrap
            .iter()
            .map(|un| un["file"].as_str().unwrap().to_string() )
            .collect::<Vec<String>>(),
        None => vec!(),
    }
}

///
/// Retrun the timestamp prefix from the filename.
///
/// If for any reason this is not possible - return a new timestamp.
///
pub fn timestamp(path: &PathBuf) -> String {

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