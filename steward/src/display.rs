use termion::clear;
use std::io::Write;
use crate::AppState;
use termion::color::Bg;
use std::{time::Duration, io::StdoutLock};
use crate::state::{ControlState, State, Control};
use termion::{cursor::Goto, color::{self, Fg}, terminal_size, raw::RawTerminal};

// Column headers for the display.
const CONTROL: &str = "CONTROL";
const STATUS: &str = "STATUS";
const DURATION: &str = "DURATION";
const UNMATCHED: &str = "UNMATCHED";
const INBOX: &str = "INBOX";
const OUTBOX: &str = "OUTBOX";
const DISK_USAGE: &str = "DISK USAGE";
const MESSAGES: &str = "MESSAGES";

const COLUMNS: [&str; 8] = [
    CONTROL,
    STATUS,
    DURATION,
    UNMATCHED,
    INBOX,
    OUTBOX,
    DISK_USAGE,
    MESSAGES
];

const BANNER_HEIGHT: u16 = 10; // Includes a padding row.

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

pub fn display(stdout: &mut RawTerminal<StdoutLock>, state: &mut State, app_state: &AppState, mut terminal_size: (u16, u16)/* , selected: Option<usize> */) -> (u16, u16) {

    // If the terminal has been resized then clear it.
    terminal_size = clear_if_resized(terminal_size, stdout);

    // Render the banner stats.
    display_stats(stdout, state, app_state);

    // How many controls per column?
    let rows = terminal_size.1 - BANNER_HEIGHT;

    // Maximum that will fit
    let max = rows as usize;

    // Get the widest content in each column - use the captions as the starting width for each column.
    let mut widths: [usize; COLUMNS.len()] = [
        CONTROL.len(), // TODO: Limit as long paths use too much room
        STATUS.len(),
        DURATION.len(),
        UNMATCHED.len(),
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
    }

    // Display headers for each column.
    write!(stdout, "{pos}{heading}{name:0w_name$}   {status:0w_status$}   {duration:>0w_duration$}   {unmatched:>0w_unmatched$}   {inbox:>0w_inbox$}   {outbox:>0w_outbox$}   {usage:>0w_usage$}   {messages:0w_messages$}{reset}",
        pos = Goto(2, BANNER_HEIGHT),
        heading = Fg(color::Rgb(240, 230, 140)),
        reset = Fg(color::Reset),
        w_name = widths[0],     name = "CONTROL",
        w_status = widths[1],   status = "STATUS",
        w_duration = widths[2], duration = "DURATION",
        w_unmatched = widths[3], unmatched = "UNMATCHED",
        w_inbox = widths[4],    inbox = "INBOX",
        w_outbox = widths[5],   outbox = "OUTBOX",
        w_usage = widths[6],    usage = "DISK USAGE",
        w_messages = widths[7], messages = "MESSAGES")
        .expect("cant write stdout");

    // Display controls across the available columns.
    for (idx, captions) in control_captions.iter().enumerate() {
        let row = idx as u16 + BANNER_HEIGHT + 1;

        write!(stdout, "{pos}{name:0w_name$}   {state_colour}{state:0w_state$}{reset}   {duration:>0w_duration$}   {unmatched:>0w_unmatched$}   {inbox:>0w_inbox$}   {outbox:>0w_outbox$}   {usage:>0w_usage$}   {messages:0w_messages$}{clear}",
            pos = Goto(2, row),
            state_colour = Fg(state_colours[idx]),
            reset = Fg(color::Reset),
            clear = clear::UntilNewline,
            w_name = widths[0],     name = captions[0],
            w_state = widths[1],    state = captions[1],
            w_duration = widths[2], duration = captions[2],
            w_unmatched = widths[3], unmatched = captions[3],
            w_inbox = widths[4],    inbox = captions[4],
            w_outbox = widths[5],   outbox = captions[5],
            w_usage = widths[6],    usage = captions[6],
            w_messages = widths[7], messages = captions[7])
            .expect("cant write stdout");
    }

    // Display keyboard shortcuts in the footer.
    display_shortcuts(stdout, terminal_size);

    terminal_size
}

fn display_shortcuts(stdout: &mut RawTerminal<StdoutLock>, terminal_size: (u16, u16)) {
    write!(stdout, "{pos}{style}[Q]uit | [R]efresh (re-load register){reset}",
        pos = Goto(1, terminal_size.1),
        style = Fg(color::Rgb(100, 149, 237)),
        reset = Fg(color::Reset))
        .expect("cant write shortcuts");
}

///
/// Display overall stats in the header region.
///
fn display_stats(stdout: &mut RawTerminal<StdoutLock>, state: &mut State, app_state: &AppState) {

    let suspended = state.controls().iter().filter(|cn| cn.state() == ControlState::Suspended).count();
    let status = match app_state {
        AppState::Running     => {
            if suspended > 0 {
                format!("{style} BAD {reset}{clear}", style = Bg(color::Rgb(255, 69, 0)), reset = Bg(color::Reset), clear = clear::UntilNewline)
            } else {
                format!("{style} GOOD {reset}{clear}", style = Bg(color::Rgb(85, 107, 47)), reset = Bg(color::Reset), clear = clear::UntilNewline)
            }
        },
        AppState::Reloading   => format!("Refreshing Controls"),
        AppState::Terminating => format!("Terminating..."),
    };

    write!(stdout, "{pos}Running  : {running}/{total}",
        pos = Goto(45, 2),
        running = state.controls().iter().filter(|cn| cn.is_running()).count(),
        total = state.controls().len(),
    ).expect("cant write running header");

    write!(stdout, "{pos}Stopped  : {stopped}",
        pos = Goto(45, 3),
        stopped = state.controls().iter().filter(|cn| cn.state() == ControlState::Stopped).count(),
    ).expect("cant write stopped header");

    write!(stdout, "{pos}Suspended: {suspended}",
        pos = Goto(45, 4),
        suspended = suspended,
    ).expect("cant write suspended header");

    write!(stdout, "{pos}Status    : {status}",
        pos = Goto(65, 2),
        status = status,
    ).expect("cant write status header");

    write!(stdout, "{pos}Disk Usage: {usage}",
        pos = Goto(65, 3),
        usage = bytesize::to_string(state.controls().iter().map(|cn| cn.root_len()).sum::<usize>() as u64, false),
    ).expect("cant write status header");

    write!(stdout, "{pos}Register  : {register}",
        pos = Goto(65, 4),
        register = state.register()
    ).expect("cant write register header");
}

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

        // Umatched record count.
        format!("{}", control.unmatched()),

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

pub fn _debug(msg: String) {
    println!("{}{}", Goto(40, 1), msg);
}