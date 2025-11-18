#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "Running Criterion benchmarks..."
cargo bench --bench performance

echo "Benchmarks completed."

