mod state;
mod display;
mod register;

use chrono::Utc;
use crossbeam::channel;
use state::{State, Control, JobResult};
use anyhow::Result;
use register::Register;
use termion::{cursor::Goto, terminal_size};
use std::{time::Duration, thread, path::{Path, PathBuf}, process::Command};

// TODO: Single column - use keyboard to navigate up/down list.
// TODO: Sort running, suspended, then others.
// TODO: Display like this :-
/*
  CONTROL        STATE                 DURATION   INBOX   OUTBOX   DISK USAGE   MESSAGE(s)
  Control A      Suspended - Errors       15:00
  Control A      Suspended - Errors       15:00
> Control A    < Suspended - Errors       15:00
*/

// TODO: Top panel
/*
Running  : 0/10   Status: GOOD/BAD (suspended = bad, use background colour)
Disabled : 4      Total Disk Usage: 9.5GB
Suspended: 2      Uptime: 912:45:12
*/
// TODO: How to un-suspend a control!!!? Keyboard.
// TODO: Above to ignore .inprogress (this allows slow file writes - invoking process must rename at end).
// TODO: Ctrl+c stop queuing new jobs. Terminate after last job is complete.
// TODO: Prometheus export.
// TODO: Document the .inprogress exclusion. Ensure Jetwash NEVER processes .inprogress inbox files - regardless of regex.
// TODO: Semaphore to limit concurrent match jobs.
// TODO: Change banner to OpenRec, Steward: Match Job orchi.

pub fn main_loop<P: AsRef<Path>>(register_path: P) -> Result<()> {

    // Parse and load the register.
    let register = Register::load(register_path.as_ref())?;

    valdiate_register(&register)?;

    // Build a state engine to track control states and task queues.
    // let state = Arc::new(Mutex::new(State::new(&register)));
    // TODO: If we run single threaded, may not need arc or mutex.
    let mut state = State::new(&register);

    display::init();

    // Measure the screen area.
    let terminal_size = terminal_size().unwrap();

    loop {
        // Render the controls which will fit in the terminal
        display::display(&mut state, terminal_size);

        // TODO: Check for register config file changes (and new/removed controls).

        // TODO: Monitor enabled inboxes - track their states. new file does not mean data is ready...
        for control in state.controls_mut() {
            if !control.is_running() {
                continue
            }

            // Is a running job complete?
            if let Some(callback) = control.callback() {
                if let Ok(result) = callback.try_recv() {
                    control.job_done();

                    if result.failure() {
                        control.suspend();
                        control.queue_message(result.message().as_ref().expect("should have message").clone());
                    } else {
                        control.queue_message("Match job complete".into());
                    }
                }
            }

            // Are there new files to process?
            if !control.scan_inbox().is_empty() {
                control.queue_message("Queueing match job".into());
                control.queue_job();
                // display::show_msg(format!("Control {} ready for job", control.name()));
            }
        }

        // Shush for a bit.
        thread::sleep(Duration::from_millis(500));
    }
}


fn valdiate_register(register: &Register) -> Result<()> {
    // TODO: Validate id's are unique.
    // TODO: Validate the charters exist for all controls.
    // TODO: Validate roots are unique to each control.
    Ok(())
}

///
/// Initiate a match job (jetwash then celerity).
///
fn do_match_job(control_id: String, charter: PathBuf, root: PathBuf, sender: channel::Sender<JobResult>) {

    let ts = new_timestamp();
    let mut logs = root.clone();
    logs.push("logs/");
    std::fs::create_dir_all(&logs).expect("cant create logs");

    // TODO: Allow bins to be configured - else use path....
    // JETWASH
    let output = Command::new("/home/stef/dev/rust/OpenRec/target/release/jetwash")
        .arg(&charter)
        .arg(&root)
        .output()
        .expect("failed to execute jetwash");

    // TODO: Append to day's logs.
    std::fs::write(logs.join(format!("jetwash_{}.log", ts)), output.stderr).expect("Unable to write stderr");

    if !output.status.success() {
        // Notify the main thread this control has failed.
        let _ignore = sender.send(JobResult::new_failure(format!("{} - Jetwash status: {}", control_id, output.status)));
        return
    }

    // CELERITY
    let output = Command::new("/home/stef/dev/rust/OpenRec/target/release/celerity")
        .arg(charter)
        .arg(&root)
        .output()
        .expect("failed to execute celerity"); // TODO: Don't expect, display error.

    std::fs::write(logs.join(format!("celerity_{}.log", ts)), output.stderr).expect("Unable to write stderr");

    // Notify the main thread this control has finished.
    if output.status.success() {
        let _ignore = sender.send(JobResult::new_success());
    } else {
        let _ignore = sender.send(JobResult::new_failure(format!("{} - Celerity status: {}", control_id, output.status)));
    }
}


pub fn random(range: usize, less_than: usize) -> bool {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen_range(1..=range) < less_than
}

///
/// Return a new timestamp in the file prefix format.
///
pub fn new_timestamp() -> String {
    Utc::now().format("%Y%m%d_%H%M%S%3f").to_string()
}