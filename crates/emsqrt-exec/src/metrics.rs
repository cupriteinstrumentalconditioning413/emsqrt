//! Metrics/tracing hooks (starter).
//!
//! This module purposefully avoids pulling heavy telemetry stacks.
//! Wire these up to OpenTelemetry/Prometheus in the binary layer.

#[cfg(feature = "tracing")]
pub fn emit_span(event: &str, key_values: &[(&str, String)]) {
    let mut span = tracing::span!(tracing::Level::TRACE, "emsqrt", event);
    for (k, v) in key_values {
        tracing::trace!(%event, %k, %v, "metric");
    }
    drop(span);
}

#[cfg(not(feature = "tracing"))]
pub fn emit_span(_event: &str, _key_values: &[(&str, String)]) { /* no-op */
}
