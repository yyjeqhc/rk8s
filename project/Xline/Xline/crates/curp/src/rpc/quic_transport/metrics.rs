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
        .init(),
    curp_conn_cache_hits_total: Counter<u64> = meter()
        .u64_counter("curp_conn_cache_hits")
        .with_description("The total number of CURP connection cache hits.")
        .init(),
    curp_conn_cache_misses_total: Counter<u64> = meter()
        .u64_counter("curp_conn_cache_misses")
        .with_description("The total number of CURP connection cache misses.")
        .init(),
    curp_conn_cache_evictions_total: Counter<u64> = meter()
        .u64_counter("curp_conn_cache_evictions")
        .with_description("The total number of CURP connection cache evictions.")
        .init()
}
