use std::{fmt::Display, time::Instant};
use crate::state::{ControlState, State, Control};
use termion::{cursor::Goto, color::{self, Fg}, terminal_size, clear};

const CONTROL_WIDTH: u16 = 60;

pub fn init() {
    println!("{}{}{}", termion::clear::All, Goto(1, 1), BANNER);
}

// TODO: Work on the flicker.

pub fn display(state: &State, mut terminal_size: (u16, u16)) {

    // If the terminal has been resized then clear it.
    terminal_size = clear_if_resized(terminal_size);

    // How many columns can we display?
    let cols = terminal_size.0 / CONTROL_WIDTH;

    // How many controls per column?
    let rows = terminal_size.1 - 10;

    // Maximum that will fit
    let max = (cols * rows) as usize;

    // Display headers for each column.
    for col in 0..cols {
        println!("{pos}{heading}{id:20} {status:18} {inbox:>6} {outbox:>6}{reset}",
            pos = Goto(col * CONTROL_WIDTH, 9),
            heading = Fg(color::Rgb(240, 230, 140)),
            id = "CONTROL",
            status = "STATUS",
            inbox = "INBOX",
            outbox = "OUTBOX",
            reset = Fg(color::Reset));
    }

    // Display controls across the available columns.
    for (idx, control) in state.controls().iter().take(max).enumerate() {
        // Fill columns down, then across.
        let col = ((idx as u16) / rows) * CONTROL_WIDTH;
        let row = 10 + ((idx as u16 + rows /* - 1 */) % rows);

        println!("{pos}{control}",
            pos = Goto(col, row),
            control = control);
    }
}

///
/// Allows status messages to be queued and displayed for at-least a brief period of time.
///
pub struct MessageQueue {
    current: Option<(Instant, String)>,
    shown_at: Option<Instant>,
    msgs: Vec<(Instant, String)>
}


// TODO: Hook this into the MessageQueue above.
pub fn show_msg(msg: String) {
    println!("{pink}{pos}{msg}{reset}",
        pos = Goto(40, 1),
        msg = msg,
        pink = termion::color::Fg(termion::color::Rgb(255,182,193)),
        reset = termion::color::Fg(termion::color::Reset));
}


///
/// If the terminal is resized, we'll clear it so the next render doesn't leave any
/// left-over output in the wrong place. This means we don't have to clear the terminal
/// on each render which can cause flicker.
///
fn clear_if_resized(prev_terminal_size: (u16, u16)) -> (u16, u16) {
    let terminal_size = terminal_size().unwrap();

    if terminal_size != prev_terminal_size {
        println!("{}{}{}", termion::clear::All, Goto(1, 1), BANNER);
    }

    terminal_size
}

impl Display for Control {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (colour, status) = match self.state() {
            ControlState::Started => {
                match self.job() {
                    Some(_task) => (color::Rgb(255, 255, 255), "Running - matching"),
                    None => (color::Rgb(200, 200, 200), "Running - idle"),
                }
            },
            ControlState::Stopped => (color::Rgb(100, 100, 100), "Stopped - disabled"),
            ControlState::Suspended => (color::Rgb(255, 69, 0), "Suspended - Errors"),
        };

        let inbox = "123MB";
        let outbox = "123MB";

        write!(f, "{colour}{id:20} {status:18} {inbox:>6} {outbox:>6}{reset}",
            id = self.id(),
            status = status,
            inbox = inbox,
            outbox = outbox,
            colour = Fg(colour),
            reset = Fg(color::Reset))
    }
}


const BANNER: &str = r#" _____ _                             _
/  ___| |                           | |
\ `--.| |_ _____      ____ _ _ __ __| |
 `--. \ __/ _ \ \ /\ / / _` | '__/ _` |
/\__/ / ||  __/\ V  V / (_| | | | (_| |
\____/ \__\___| \_/\_/ \__,_|_|  \__,_|
 OpenRec: Match Job Orchistrator
"#;
