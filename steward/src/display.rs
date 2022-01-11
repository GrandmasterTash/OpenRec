use std::io::Write;
use std::{time::Duration, io::StdoutLock};
use crate::state::{ControlState, State, Control};
use termion::{cursor::Goto, color::{self, Fg}, terminal_size, raw::RawTerminal};

const BANNER_HEIGHT: u16 = 10;

// Column headers for the display.
const CONTROL: &str = "CONTROL";
const STATUS: &str = "STATUS";
const DURATION: &str = "DURATION";
const INBOX: &str = "INBOX";
const OUTBOX: &str = "OUTBOX";
const DISK_USAGE: &str = "DISK USAGE";
const MESSAGES: &str = "MESSAGES";

const COLUMNS: [&str; 7] = [
    CONTROL,
    STATUS,
    DURATION,
    INBOX,
    OUTBOX,
    DISK_USAGE,
    MESSAGES
];

pub fn init(stdout: &mut RawTerminal<StdoutLock>) {
    write!(stdout, "{}{}", termion::clear::All, termion::cursor::Hide).unwrap();
    writeln!(stdout, r#"{}  _____                 ______"#, Goto(1, 1)).unwrap();
    writeln!(stdout, r#"{} |  _  |                | ___ \"#, Goto(1, 2)).unwrap();
    writeln!(stdout, r#"{} | | | |_ __   ___ _ __ | |_/ /___  ___"#, Goto(1, 3)).unwrap();
    writeln!(stdout, r#"{} | | | | '_ \ / _ \ '_ \|    // _ \/ __|"#, Goto(1, 4)).unwrap();
    writeln!(stdout, r#"{} \ \_/ / |_) |  __/ | | | |\ \  __/ (__"#, Goto(1, 5)).unwrap();
    writeln!(stdout, r#"{}  \___/| .__/ \___|_| |_\_| \_\___|\___|"#, Goto(1, 6)).unwrap();
    writeln!(stdout, r#"{}       | |"#, Goto(1, 7)).unwrap();
    writeln!(stdout, r#"{}       |_| Steward: Match Job Orchistrator"#, Goto(1, 8)).unwrap();
}

// TODO: Work on the flicker.

pub fn display(stdout: &mut RawTerminal<StdoutLock>, state: &mut State, mut terminal_size: (u16, u16)/* , selected: Option<usize> */) -> (u16, u16) {

    // If the terminal has been resized then clear it.
    terminal_size = clear_if_resized(terminal_size, stdout);

    // How many controls per column?
    let rows = terminal_size.1 - BANNER_HEIGHT;

    // Maximum that will fit
    let max = rows as usize;

    // Get the widest content in each column - use the captions as the starting width for each column.
    let mut widths: [usize; COLUMNS.len()] = [
        CONTROL.len(),
        STATUS.len(),
        DURATION.len(),
        INBOX.len(),
        OUTBOX.len(),
        DISK_USAGE.len(),
        MESSAGES.len()
    ];

    let control_captions: Vec<[String; COLUMNS.len()]> = state
        .controls_mut()
        .take(max)
        .map(|con| captions(con))
        .collect::<_>();

    let state_colours: Vec<color::Rgb> = state
        .controls()
        .iter()
        .take(max)
        .map(|c| match c.state() {
            ControlState::StartedIdle => color::Rgb(200, 200, 200),
            ControlState::StartedMatching => color::Rgb(255, 255, 255),
            ControlState::Stopped => color::Rgb(100, 100, 100),
            ControlState::Suspended => color::Rgb(255, 69, 0),
        })
        .collect::<_>();

    // Look at everything we want to display, and set each column width to the widest thing.
    for captions in &control_captions {
        for (idx, caption) in captions.iter().enumerate() {
            if caption.len() > widths[idx] {
                widths[idx] = caption.len();
            }
        }

        // TODO: Truncate the last column width to fit in the screen.
    }

    // Display headers for each column.
    write!(stdout, "{pos}{heading}{name:0w_name$}   {status:0w_status$}   {duration:>0w_duration$}   {inbox:>0w_inbox$}   {outbox:>0w_outbox$}   {usage:>0w_usage$}   {messages:0w_messages$}{reset}",
        pos = Goto(2, BANNER_HEIGHT),
        heading = Fg(color::Rgb(240, 230, 140)),
        reset = Fg(color::Reset),
        w_name = widths[0],     name = "CONTROL",
        w_status = widths[1],   status = "STATUS",
        w_duration = widths[2], duration = "DURATION",
        w_inbox = widths[3],    inbox = "INBOX",
        w_outbox = widths[4],   outbox = "OUTBOX",
        w_usage = widths[5],    usage = "DISK USAGE",
        w_messages = widths[6], messages = "MESSAGES")
        .expect("cant write stdout");

    // Display controls across the available columns.
    for (idx, captions) in control_captions.iter().enumerate() {
        let row = idx as u16 + BANNER_HEIGHT + 1;
        // let (l_anchor, r_anchor) = anchors(idx, selected);

        write!(stdout, "{pos}{name:0w_name$}   {state_colour}{state:0w_state$}{reset}   {duration:>0w_duration$}   {inbox:>0w_inbox$}   {outbox:>0w_outbox$}   {usage:>0w_usage$}   {messages:0w_messages$}",
            pos = Goto(2, row),
            state_colour = Fg(state_colours[idx]),
            reset = Fg(color::Reset),
            // l_anchor = l_anchor,
            // r_anchor = r_anchor,
            w_name = widths[0],     name = captions[0],
            w_state = widths[1],    state = captions[1],
            w_duration = widths[2], duration = captions[2],
            w_inbox = widths[3],    inbox = captions[3],
            w_outbox = widths[4],   outbox = captions[4],
            w_usage = widths[5],    usage = captions[5],
            w_messages = widths[6], messages = captions[6])
            .expect("cant write stdout");
    }

    terminal_size
}

///
/// Get the selection highlight arrows if the current row is selected.
///
// fn anchors(current: usize, selected: Option<usize>) -> (&'static str, &'static str) {
//     let is_selected = match selected {
//         Some(sel) => current == sel,
//         None => false,
//     };
//     match is_selected {
//         true  => (">", "<"),
//         false => ("", ""),
//     }
// }

///
/// Get all the captions from the control to display in column order - without any formatting.
///
fn captions(control: &mut Control) -> [String; COLUMNS.len()] {
    [
        // Name.
        control.name().to_string(),

        // State.
        match control.state() {
            ControlState::StartedIdle     => "Running - idle",
            ControlState::StartedMatching => "Running - matching",
            ControlState::Stopped         => "Stopped - disabled",
            ControlState::Suspended       => "Suspended - Errors",
        }.into(),

        // Duration.
        humantime::format_duration(Duration::from_secs(control.duration().as_secs())).to_string(),

        // Inbox.
        if control.inbox_len() == 0 {
            "-".into()
        } else {
            bytesize::to_string(control.inbox_len() as u64, false)
        },

        // Outbox.
        if control.outbox_len() == 0 {
            "-".into()
        } else {
            bytesize::to_string(control.outbox_len() as u64, false)
        },

        // Disk usage.
        if control.root_len() == 0 {
            "-".into()
        } else {
            bytesize::to_string(control.root_len() as u64, false)
        },

        // Message(s).
        control.next_message().unwrap_or_default(),
    ]
}

///
/// If the terminal is resized, we'll clear it so the next render doesn't leave any
/// left-over output in the wrong place. This means we don't have to clear the terminal
/// on each render which can cause flicker.
///
fn clear_if_resized(prev_terminal_size: (u16, u16), stdout: &mut RawTerminal<StdoutLock>) -> (u16, u16) {
    let terminal_size = terminal_size().unwrap();

    if terminal_size != prev_terminal_size {
        init(stdout);
    }

    terminal_size
}

// const BANNER: &str = r#" _____                 ______
// |  _  |                | ___ \
// | | | |_ __   ___ _ __ | |_/ /___  ___
// | | | | '_ \ / _ \ '_ \|    // _ \/ __|
// \ \_/ / |_) |  __/ | | | |\ \  __/ (__
//  \___/| .__/ \___|_| |_\_| \_\___|\___|
//       | |
//       |_| Steward: Match Job Orchistrator
// "#;
