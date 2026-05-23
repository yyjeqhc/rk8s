use opentelemetry::metrics::Counter;
use utils::define_metrics;

define_metrics! {
    "curp_quic",
    quic_connect_attempts_total: Counter<u64> = meter()
        .u64_counter("quic_connect_attempts")
        .with_description("The total number of QUIC connection attempts.")
        .init(),
    quic_connect_failures_total: Counter<u64> = meter()
        .u64_counter("quic_connect_failures")
        .with_description("The total number of QUIC connection failures.")
        .init()
}
