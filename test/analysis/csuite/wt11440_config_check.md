# wt11440_config_check — Configuration string parsing correctness and benchmark

**Path:** `test/csuite/wt11440_config_check/`
**Language:** C
**Storage mode:** General (WiredTiger connection opened; no data tables)
**Jira ticket:** WT-11440
**Components under test:** `session->begin_transaction` configuration parsing, pre-formatted config string optimization

## What This Test Does
This test validates and benchmarks two implementation strategies for calling `begin_transaction` with four variable parameters (ignore_prepare, roundup_timestamps.prepared, roundup_timestamps.read, no_timestamp). It measures the performance difference between formatting the config string on every call versus pre-formatting all 24 possible strings at initialization time. Both variants verify that the correct transaction flags are set on the first run.

## Test Scenarios / Cases

### Scenario: Variant 0 — format string on every call (base)
- **What it tests:** That dynamically formatting the configuration string on each call correctly sets `WT_TXN_IGNORE_PREPARE`, `WT_TXN_READONLY`, `WT_TXN_TS_ROUND_PREPARED`, `WT_TXN_TS_ROUND_READ`, and `WT_TXN_TS_NOT_SET` according to the requested parameters.
- **Components:** `session->begin_transaction`, `__wt_snprintf`, internal transaction flags.
- **Notes:** Baseline for timing comparison.

### Scenario: Variant 1 — advance-format (pre-formatted string table)
- **What it tests:** That selecting from a pre-formatted table of 24 config strings (all combinations of the four boolean/enum parameters) produces identical transaction flag results.
- **Components:** `session->begin_transaction`, pre-allocated string table.
- **Notes:** Eliminates the per-call sprintf overhead. Timing is compared against the baseline.

## LazyFS Variant
None.
