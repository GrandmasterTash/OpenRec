use lazy_static::lazy_static;
use prometheus::{IntGauge, register_int_gauge};

/*
Per control registry
--------------------
unmatched txs
unmatched mbs
matched txs
matched mbs
matched groups
jetwash job duration - capture from process cmd
celerity job duration - capture from process cmd
jobs triggered
errors (failed jobs)

default registry steward
------------------------
GAUGE # controls
GAUGE # running controls
GAUGE # disabled controls
GAUGE # suspended controls
GAUGE # total unmatched txs
GAUGE disk usage
COUNTER # match jobs.
COUNTER # failed jobs.



https://github.com/tikv/rust-prometheus/blob/master/examples/example_push.rs
*/

lazy_static! {
    pub static ref CONTROL_GAUGE: IntGauge = register_int_gauge!(
        "controls_total",
        "Total number of controls in the register."
    )
    .unwrap();
}