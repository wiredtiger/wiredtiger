# wt11126_compile_config — Precompiled configuration string correctness and benchmark

**Path:** `test/csuite/wt11126_compile_config/`
**Language:** C
**Storage mode:** General (WiredTiger connection opened; no data tables)
**Jira ticket:** WT-11126
**Components under test:** `conn->compile_configuration`, `session->bind_configuration`, `session->begin_transaction`, configuration string parsing

## What This Test Does
This test validates and benchmarks WiredTiger's precompiled configuration string API for `begin_transaction`. It exercises five variants of calling `begin_transaction` with four boolean/enum parameters (ignore_prepare, roundup_timestamps.prepared, roundup_timestamps.read, no_timestamp), verifies that each variant sets the correct transaction flags, and measures throughput across 1,000,000 calls per variant per thread. The primary correctness check happens on the first run of each variant; subsequent runs accumulate timing.

## Test Scenarios / Cases

### Scenario: Variant 0 — format string on every call (baseline)
- **What it tests:** Correctness of `begin_transaction` with a dynamically formatted config string. Establishes baseline timing.
- **Components:** `session->begin_transaction`, `__wt_snprintf`.
- **Notes:** Slowest but most straightforward; used as 1.0x baseline for speedup comparisons.

### Scenario: Variant 1 — pre-made config string table (medium)
- **What it tests:** Correctness and timing when the caller pre-formats all 24 possible config strings at init time and selects by index.
- **Components:** `session->begin_transaction`, pre-allocated string table.
- **Notes:** Eliminates per-call sprintf cost.

### Scenario: Variant 2 — single precompiled string with `bind_configuration` (fast)
- **What it tests:** That `conn->compile_configuration` produces a precompiled string usable with `session->bind_configuration` + `session->begin_transaction`, and that the transaction flags match expectations.
- **Components:** `conn->compile_configuration`, `session->bind_configuration`, `session->begin_transaction`.
- **Notes:** Core API under test; validates the precompile/bind/use lifecycle.

### Scenario: Variant 3 — precompiled string per combination (fast alternate)
- **What it tests:** That precompiling all 24 config combinations individually and selecting by index produces the same flags as variants 0–2.
- **Components:** `conn->compile_configuration` (×24), `session->begin_transaction`.

### Scenario: Variant 4 — null configuration (comparison baseline)
- **What it tests:** The minimum overhead of `begin_transaction(session, NULL)` for timing reference only.
- **Components:** `session->begin_transaction(NULL)`.
- **Notes:** Not correctness-checked (flags are not set to known values).

## LazyFS Variant
None.
