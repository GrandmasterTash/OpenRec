use std::time::Duration;

pub mod charter;
pub mod data_type;
pub mod error;
pub mod lua;

///
/// Provide a consistent formatting for durations and rates.
///
/// The format_duration will show micro and nano seconds but we typically only need to see ms.
///
pub fn formatted_duration_rate(amount: usize, elapsed: Duration) -> (String, String) {
    let duration = Duration::new(elapsed.as_secs(), elapsed.subsec_millis() * 1000000); // Keep precision to ms.
    let rate = (elapsed.as_millis() as f64 / amount as f64) as f64;
    (
        humantime::format_duration(duration).to_string(),
        format!("{:.3}ms", rate)
    )
}

///
/// Highlight some log output with ansi colour codes.
///
pub fn blue(msg: &str) -> ansi_term::ANSIGenericString<'_, str> {
    ansi_term::Colour::RGB(70, 130, 180).paint(msg)
}