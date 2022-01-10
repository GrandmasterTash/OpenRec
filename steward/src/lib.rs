mod state;
mod display;
mod register;

use state::State;
use anyhow::Result;
use register::Register;
use termion::{cursor::Goto, terminal_size};
use std::{time::Duration, thread, path::Path};


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


pub fn main_loop<P: AsRef<Path>>(register_path: P) -> Result<()> {

    // Parse and load the register.
    let register = Register::load(register_path.as_ref())?;

    valdiate_register(&register)?;

    // Build a state engine to track control states and task queues.
    // let state = Arc::new(Mutex::new(State::new(&register)));
    // TODO: If we run single threaded, may not need arc or mutex.
    let state = State::new(&register);

    println!("{}{}{}", termion::clear::All, Goto(1, 1), BANNER);

    // Measure the screen area.
    let terminal_size = terminal_size().unwrap();

    loop {
        // Render the controls which will fit in the terminal
        display::display(&state, terminal_size);

        // TODO: Check for register config file changes (and new/removed controls).
        // TODO: Monitor enabled inboxes - track their states. new file does not mean data is ready...
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
    // TODO: Validate inboxes and outboxes are unique to each control.
    // TODO: Ensure inbox/outbox folders are created if missing for all controls.
    Ok(())
}



const BANNER: &str = r#" _____ _                             _
/  ___| |                           | |
\ `--.| |_ _____      ____ _ _ __ __| |
 `--. \ __/ _ \ \ /\ / / _` | '__/ _` |
/\__/ / ||  __/\ V  V / (_| | | | (_| |
\____/ \__\___| \_/\_/ \__,_|_|  \__,_|
 OpenRec: Match Job Orchistrator
"#;



pub fn random(range: usize, less_than: usize) -> bool {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen_range(1..=range) < less_than
}