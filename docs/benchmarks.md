# Benchmarking

We use `Criterion` to exercise representative operators (window functions, sort, etc.) under a fixed memory footprint.

## Running the benchmarks

```bash
./scripts/benchmarks/run_benchmarks.sh
```

The script runs `cargo bench --bench performance` (the bench targets a window operator evaluating row numbers and sum). Add `--bencher <name>` arguments to `cargo bench` if you want to focus on a specific scenario.

## Memory cap knobs

Benchmarks reuse the runtime's memory budget (`MemoryBudgetImpl::new(4 * 1024 * 1024)`). To explore different caps:

- edit `benches/performance.rs` and adjust the budget.
- or fork the bench and call the runtime pipeline under different `EngineConfig` settings to emulate real workloads.

Profiling with `perf` or `Instruments` is encouraged; the benchmark harness prints durations via Criterion's reports (in `target/criterion`). Use that output to validate SIMD or parallel optimizations before pushing changes.

