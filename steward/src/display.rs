use std::time::Duration;

use crate::state::{ControlState, State, Control};
use termion::{cursor::Goto, color::{self, Fg}, terminal_size};

const CONTROL_WIDTH: u16 = 60;

pub fn init() {
    println!("{}{}{}", termion::clear::All, Goto(1, 1), BANNER);
}

// TODO: Work on the flicker.

pub fn display(state: &mut State, mut terminal_size: (u16, u16)) {

    // If the terminal has been resized then clear it.
    terminal_size = clear_if_resized(terminal_size);

    // How many columns can we display?
    // let cols = terminal_size.0 / CONTROL_WIDTH;
    let cols = 1; // Single column for now.

    // How many controls per column?
    let rows = terminal_size.1 - 10;

    // Maximum that will fit
    let max = (cols * rows) as usize;

    // Get the widest control name enforce a screen-related limit.
    let widest = state.controls().iter().map(|c|c.name().len()).max().unwrap_or(20);

    // Display headers for each column.
    for col in 0..cols {
        println!("{pos}{heading}{name:0widest$}   {status:18}   {duration:>9}   {inbox:>6}   {outbox:>6}   {usage:>10}   {messages}{reset}",
            pos = Goto(col * CONTROL_WIDTH, 9),
            heading = Fg(color::Rgb(240, 230, 140)),
            widest = widest,
            name = "CONTROL",
            status = "STATUS",
            duration = "DURATION",
            inbox = "INBOX",
            outbox = "OUTBOX",
            usage = "DISK USAGE",
            messages = "MESSAGES",
            reset = Fg(color::Reset));
    }

    // Display controls across the available columns.
    for (idx, control) in state.controls_mut().take(max).enumerate() {
        // Fill columns down, then across.
        // let col = ((idx as u16) / rows) * CONTROL_WIDTH;
        // let row = 10 + ((idx as u16 + rows /* - 1 */) % rows);
        let col = 1;
        let row = idx as u16 + 10;

        println!("{pos}{control}",
            pos = Goto(col, row),
            control = format(control, widest, terminal_size.0));
    }
}


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

fn format(control: &mut Control, widest: usize, terminal_width: u16) -> String {
    let (colour, status) = match control.state() {
        ControlState::StartedIdle => (color::Rgb(200, 200, 200), "Running - idle"),
        ControlState::StartedMatching => (color::Rgb(255, 255, 255), "Running - matching"),
        ControlState::Stopped => (color::Rgb(100, 100, 100), "Stopped - disabled"),
        ControlState::Suspended => (color::Rgb(255, 69, 0), "Suspended - Errors"),
    };

    let inbox = "123MB";
    let outbox = "123MB";

    // Truncate duration to seconds and format for humons.
    let duration = humantime::format_duration(Duration::from_secs(control.duration().as_secs())).to_string();
    let message = control.next_message().unwrap_or_default();

    // Blank out any residual message.
    let fill = " ".repeat(terminal_width as usize - (widest + message.len() + (6 * 3) + 18 + 9 + 6 + 6 + 10));

    format!("{name:0widest$}   {colour}{status:18}{reset}   {duration:>9}   {inbox:>6}   {outbox:>6}   {usage:>10}   {messages}{fill}",
        widest = widest,
        name = control.name(),
        status = status,
        duration = duration,
        inbox = inbox,
        outbox = outbox,
        usage = "-",
        messages = message,
        fill = fill,
        colour = Fg(colour),
        reset = Fg(color::Reset))
}

const BANNER: &str = r#" _____ _                             _
/  ___| |                           | |
\ `--.| |_ _____      ____ _ _ __ __| |
 `--. \ __/ _ \ \ /\ / / _` | '__/ _` |
/\__/ / ||  __/\ V  V / (_| | | | (_| |
\____/ \__\___| \_/\_/ \__,_|_|  \__,_|
 OpenRec: Match Job Orchistrator
"#;
