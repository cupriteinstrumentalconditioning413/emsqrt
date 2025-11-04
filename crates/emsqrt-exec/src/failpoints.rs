//! Chaos/failpoint hooks (feature: `failpoints`).
//!
//! Keep this extremely light: the macro expands to nothing unless the feature
//! is enabled. When enabled, it can panic on named points.

#[cfg(feature = "failpoints")]
#[macro_export]
macro_rules! fail_point {
    ($name:expr) => {{
        // Simple, deterministic "fail some points" behavior:
        // You can switch to a seeded RNG later based on manifest seed.
        if $name.starts_with("panic_") {
            panic!("failpoint triggered: {}", $name);
        }
    }};
}

#[cfg(not(feature = "failpoints"))]
#[macro_export]
macro_rules! fail_point {
    ($name:expr) => {
        // no-op
        let _ = $name;
    };
}
