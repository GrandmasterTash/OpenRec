use parking_lot::Mutex;
use lazy_static::lazy_static;
use std::{time::{Instant, Duration}, collections::HashMap};
use prometheus::{IntGauge, register_int_gauge};
use crate::{state::{State, ControlState}, display};

lazy_static! {
    static ref CONTROLS_GAUGE: IntGauge = register_int_gauge!("controls_total", "Total number of controls in the register.").expect("cannot create controls_total gauge");
    static ref RUNNING_GAUGE: IntGauge = register_int_gauge!("controls_running", "Total number of controls which are running").expect("cannot create controls_running gauge");
    static ref MATCHING_GAUGE: IntGauge = register_int_gauge!("controls_matching", "Total number of controls which are matching data now").expect("cannot create controls_matching gauge");
    static ref DISABLED_GAUGE: IntGauge = register_int_gauge!("controls_disabled", "Total number of controls which are disabled and not running").expect("cannot create controls_disabled gauge");
    static ref SUSPENDED_GAUGE: IntGauge = register_int_gauge!("controls_suspended", "Total number of controls which have been suspended due to errors").expect("cannot create controls_suspended gauge");
    static ref UNMATCHED_GAUGE: IntGauge = register_int_gauge!("unmatched_total", "Total number of unmatched transactions across the system").expect("cannot create unmatched_total gauge");
    static ref DISKUSAGE_GAUGE: IntGauge = register_int_gauge!("disk_usage_total", "The total amount of disk space consumed by all control data (in bytes)").expect("cannot create disk_usage_total gauge");

    // Prohibit metrics being pushed to frequently.
    static ref TIME_BARRIER: Mutex<Instant> = Mutex::new(Instant::now());
}

///
/// Update the pushgateway with control metrics - only actioned every n seconds.
///
pub fn push(pushgateway: Option<&str>, state: &mut State) {

    if let Some(address) = pushgateway {
        let mut lock = TIME_BARRIER.lock();

        if lock.elapsed() > Duration::from_secs(5) {
            *lock = Instant::now();

            CONTROLS_GAUGE.set(state.controls().len() as i64);
            RUNNING_GAUGE.set(state.controls().iter().filter(|cn| cn.is_running()).count() as i64);
            MATCHING_GAUGE.set(state.controls().iter().filter(|cn| cn.state() == ControlState::StartedMatching).count() as i64);
            DISABLED_GAUGE.set(state.controls().iter().filter(|cn| cn.state() == ControlState::Stopped).count() as i64);
            SUSPENDED_GAUGE.set(state.controls().iter().filter(|cn| cn.state() == ControlState::Suspended).count() as i64);
            UNMATCHED_GAUGE.set(state.controls().iter().map(|cn| cn.unmatched()).sum::<usize>() as i64);
            DISKUSAGE_GAUGE.set(state.controls().iter().map(|cn| cn.root_len()).sum::<usize>() as i64);

            let metric_families = prometheus::gather();

            if let Err(err) = prometheus::push_metrics(
                "overview",
                prometheus::labels! { "instance".to_owned() => "OpenRec_Steward".to_owned(),},
                address,
                metric_families,
                None, // Credentials.
            ) {
                display::error(format!("Metrics error: {}", err));
            }

            for control in state.controls_mut() {
                let registry = control.update_metrics();

                let metric_families = registry.gather();

                if let Err(err) = prometheus::push_metrics(
                    &control.name().replace("/", "_"), // '/' (in unparseable filenames) is a prohibited job character
                    HashMap::new(),
                    address,
                    metric_families,
                    None) {

                    display::error(format!("Control {} metrics error: {}", control.name(), err));
                }
            }
        }
    }
}
