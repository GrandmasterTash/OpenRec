mod state;
mod display;
mod register;

use chrono::Utc;
use crossbeam::channel;
use state::{State, Control};
use anyhow::Result;
use register::Register;
use termion::{cursor::Goto, terminal_size};
use std::{time::Duration, thread, path::{Path, PathBuf}, process::Command};

// TODO: How to un-suspend a control!!!?
// TODO: control register.
// TODO: task queue per control.
// TODO: Launch other modules shell - capture logging to file.
// TODO: Monitor for config changes and reload (queued task to reload).
// TODO: Monitor inboxes for files (delay and check size to ensure not being written to).
// TODO: Above to ignore .inprogress (this allows slow file writes - invoking process must rename at end).
// TODO: Ctrl+c stop queuing new jobs. Terminate after last job is complete.
// TODO: TestJob with 5s delay - use certain file to queue for dev purposes.
// TODO: Console display of job queues, and control states.
// TODO: Prometheus export.
// TODO: Document the .inprogress exclusion. Ensure Jetwash NEVER processes .inprogress inbox files - regardless of regex.
// TODO: Semaphore to limit concurrent match jobs.

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
        display::display(&state, terminal_size);

        // TODO: Check for register config file changes (and new/removed controls).

        // TODO: Monitor enabled inboxes - track their states. new file does not mean data is ready...
        for control in state.controls_mut() {
            if !control.is_running() {
                continue
            }

            // Is a running job complete?
            if let Some(callback) = control.callback() {
                if let Ok(job_ok) = callback.try_recv() {
                    control.job_done();

                    if !job_ok {
                        control.suspend();
                    }
                }
            }

            // Are there new files to process?
            if !control.scan_inbox().is_empty() {
                control.queue_job();
                display::show_msg(format!("Control {} ready for job", control.id()));
            }
        }


        // TODO: Queue new tasks if new files are in inbox
        // TODO: Track task durations.

        // Shush for a bit.
        thread::sleep(Duration::from_millis(500));
    }

    // Ok(())
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
fn do_match_job(control_id: String, charter: PathBuf, root: PathBuf, sender: channel::Sender<bool>) {

    let ts = new_timestamp();
    let mut logs = root.clone();
    logs.push("logs/");
    std::fs::create_dir_all(&logs).expect("cant create logs");

    // TODO: Allow bins to be configured - else use path....
    let output = Command::new("/home/stef/dev/rust/OpenRec/target/release/jetwash")
        .arg(&charter)
        .arg(&root)
        .output()
        .expect("failed to execute jetwash");

    // TODO: Append to days logs.
    display::show_msg(format!("{} - Jetwash status: {}", control_id, output.status));
    // std::fs::write(logs.join(format!("jetwash_{}_stdout.log", ts)), output.stdout).expect("Unable to write stdout"); // TODO: Dont .expect on this...
    std::fs::write(logs.join(format!("jetwash_{}.log", ts)), output.stderr).expect("Unable to write stderr");

    if output.status.success() {
        let output = Command::new("/home/stef/dev/rust/OpenRec/target/release/celerity")
            .arg(charter)
            .arg(&root)
            .output()
            .expect("failed to execute celerity");

        // TODO: Don't write empty streams to logs files.
        display::show_msg(format!("{} - Celerity status: {}", control_id, output.status));
        // std::fs::write(logs.join(format!("celerity_{}_stdout.log", ts)), output.stdout).expect("Unable to write stdout"); // TODO: Dont .expect on this...
        std::fs::write(logs.join(format!("celerity_{}.log", ts)), output.stderr).expect("Unable to write stderr");


        // Notify the main thread this control has finished.
        let _ignore = sender.send(output.status.success());

    } else {
        // Notify the main thread this control has FAILED.
        let _ignore = sender.send(false);
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