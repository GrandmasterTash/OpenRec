mod state;
mod display;
mod register;

use chrono::Utc;
use anyhow::Result;
use crossbeam::channel;
use register::Register;
use state::{State, JobResult};
use std::io::{Write, stdout, Read};
use termion::{terminal_size, raw::IntoRawMode};
use std::{time::Duration, thread, path::{Path, PathBuf}, process::Command, fs};

// TODO: Display like this :-
/*
  CONTROL        STATE                 DURATION   UNMATCHED   INBOX   OUTBOX   DISK USAGE   MESSAGE(s)
  Control A      Suspended - Errors       15:00
  Control A      Suspended - Errors       15:00
> Control A    < Suspended - Errors       15:00
*/

// TODO: Graceful termination and refresh.

// TODO: Top panel
/*
Running  : 0/10   Status: GOOD/BAD (suspended = bad, use background colour)
Disabled : 4      Total Disk Usage: 9.5GB
Suspended: 2      Uptime: 912:45:12
*/

// TODO: Flip logic to only include .ready (and drop the suffix). NO! Use a filesize monitor. simpler for the user.
// TODO: Prometheus export.
// TODO: Document the .inprogress exclusion. Ensure Jetwash NEVER processes .inprogress inbox files - regardless of regex.
// TODO: Semaphore to limit concurrent match jobs. num_cpus by default - or use memory tickets?
// TODO: Change banner to OpenRec, Steward: Match Job orchi.
// BUG: Adding data during a job doesn't trigger a follow-up job.
// TODO: Display [Q]uit and [R]efresh shortcuts.

pub fn main_loop<P: AsRef<Path>>(register_path: P) -> Result<()> {

    // Parse and load the register.
    let mut state = load_state(register_path.as_ref())?;

    let stdout = stdout();
    let mut stdout = stdout.lock().into_raw_mode().unwrap();
    let mut stdin = termion::async_stdin().bytes();

    display::init(&mut stdout);

    // Measure the screen area.
    let mut terminal_size = terminal_size().unwrap();

    loop {
        let key = stdin.next();

        if let Some(Ok(b'q')) = key {
            write!(stdout, "{}", termion::cursor::Show).unwrap();
            return Ok(()) // TODO: Graceful like.
        }

        if let Some(Ok(b'r')) = key {
            // TODO: Flag refresh signal - no new jobs. disable all non-active controls- When all jobs done, re-load state.
            state = load_state(register_path.as_ref())?;
        }

        // Render the controls which will fit in the terminal
        terminal_size = display::display(&mut stdout, &mut state, terminal_size);

        for control in state.controls_mut() {
            if !control.is_running() {
                continue
            }

            // Is a running job complete?
            if let Some(callback) = control.callback() { // TODO: Push this into fn.
                if let Ok(result) = callback.try_recv() {
                    control.job_done();

                    if result.failure() {
                        control.suspend(); // TODO: put message in suspend fn param.
                        control.queue_message(result.message().as_ref().expect("should have message").clone());
                    } else {
                        control.queue_message("Match job complete".into());
                    }
                }
            }

            // Are there new files to process?
            if !control.scan_inbox().is_empty() { // Push this into fn.
                control.queue_message("Running match job".into());
                control.queue_job(); // TODO: Above into param for queue_job
            }
        }

        // Shush for a bit.
        thread::sleep(Duration::from_millis(500));
    }
}

fn load_state(register_path: &Path) -> Result<State, anyhow::Error> {
    // Parse and load the register.
    let register = Register::load(register_path)?;

    // TODO: Validate id's are unique.
    // TODO: Validate the charters exist for all controls.
    // TODO: Validate roots are unique to each control.

    // Build a state engine to track control states and task queues.
    Ok(State::new(&register))
}

///
/// Initiate a match job (jetwash then celerity).
///
fn do_match_job(control_id: String, charter: PathBuf, root: PathBuf, sender: channel::Sender<JobResult>) {

    let ts = Utc::now().format("%Y%m%d").to_string();
    let mut logs = root.clone();
    logs.push("logs/");
    std::fs::create_dir_all(&logs).expect("cant create logs");

    // JETWASH
    let output = Command::new(format!("{}jetwash", std::env::var("JETWASH_HOME").unwrap_or_default()))
        .arg(&charter)
        .arg(&root)
        .output()
        .expect("failed to execute jetwash");

    let mut log_file = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(logs.join(format!("jetwash_{}.log", ts)))
        .expect("Cannot create jetwash log");

    if let Err(e) = log_file.write(&output.stderr) {
        eprintln!("Couldn't write to file: {}", e);
    }

    if !output.status.success() {
        // Notify the main thread this control has failed.
        let _ignore = sender.send(JobResult::new_failure(format!("{} - Jetwash status: {}", control_id, output.status)));
        return
    }

    // CELERITY
    let output = Command::new(format!("{}celerity", std::env::var("CELERITY_HOME").unwrap_or_default()))
        .arg(charter)
        .arg(&root)
        .output()
        .expect("failed to execute celerity");

    let mut log_file = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(logs.join(format!("celerity_{}.log", ts)))
        .expect("Cannot create celerity log");

    if let Err(e) = log_file.write(&output.stderr) {
        eprintln!("Couldn't write to file: {}", e);
    }

    // Notify the main thread this control has finished.
    if output.status.success() {
        let _ignore = sender.send(JobResult::new_success());
    } else {
        let _ignore = sender.send(JobResult::new_failure(format!("{} - Celerity status: {}", control_id, output.status)));
    }
}
