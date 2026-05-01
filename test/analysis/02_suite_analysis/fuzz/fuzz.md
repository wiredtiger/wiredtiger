# fuzz — LibFuzzer-based fuzz targets for configuration parsing and cursor modify

**Path:** `test/fuzz/`
**Language:** C
**Storage mode:** General
**Components under test:** `__wt_config_getones` (configuration string parser), `__wt_modify_pack`, `__wt_modify_max_memsize_unpacked`, `__wt_modify_apply_item`, LibFuzzer integration harness

## Overview

This suite provides two LibFuzzer fuzz targets and the shared infrastructure needed to run them. `fuzz_config.c` fuzzes WiredTiger's internal configuration string parser by feeding arbitrary key/config pairs. `fuzz_modify.c` fuzzes the modify (partial-value update) packing and application path by feeding arbitrary buffer contents split between an existing value and a modify delta. A shared utility library (`fuzz_util.c`/`fuzz_util.h`) handles per-worker database setup and multi-slice input splitting. Shell scripts (`fuzz_run.sh`, `fuzz_coverage.sh`) orchestrate corpus-based runs and coverage measurement.

## Test Scenarios / Cases

### Scenario: Configuration string parser fuzzing (`fuzz_config/fuzz_config.c`)
- **What it tests:** The fuzzer splits its input on the `|` byte separator into two slices: a key string and a configuration string. Both are passed to `__wt_config_getones` (the internal config lookup function). The target exercises all branches of the config parser including malformed syntax, unexpected characters, nested structures, and very long strings.
- **Components:** `__wt_config_getones`, `WT_CONFIG_ITEM`, internal config parser
- **Notes:** Uses `fuzzutil_sliced_input_init` with a single-byte `|` separator to give the fuzzer a hint about the two-input structure. Any crash, assertion failure, or sanitiser alarm is treated as a bug. The function result is deliberately discarded (the goal is to find crashes/sanitiser violations, not verify correctness of the returned value).

### Scenario: Modify pack/apply fuzzing (`fuzz_modify/fuzz_modify.c`)
- **What it tests:** The fuzzer's input is split at byte 0 (used as a size modulus) into a base value buffer and a modify delta. `__wt_modify_pack` packs the modify descriptor, `__wt_modify_max_memsize_unpacked` computes the maximum memory needed, `__wt_buf_set_and_grow` grows the target buffer, and `__wt_modify_apply_item` applies the packed modify to the base value. The target exercises all paths in the modify encoding and application logic.
- **Components:** `__wt_modify_pack`, `__wt_modify_apply_item`, `__wt_modify_max_memsize_unpacked`, `__wt_buf_set_and_grow`, metadata cursor (used to obtain a cursor handle)
- **Notes:** Inputs smaller than 10 bytes are rejected early. A metadata cursor is opened solely to obtain a `WT_CURSOR *` (needed by `__wt_modify_pack`). The session and connection are reused across fuzzer iterations via the global `fuzz_state`.

### Scenario: Shared fuzzer infrastructure
- **What it tests:** `fuzzutil_setup` initialises a per-process WiredTiger database on first call (keyed by PID to isolate LibFuzzer workers). `fuzzutil_sliced_input_init` implements the magic-separator split-input pattern, enabling the fuzzer to discover structured inputs over time. `fuzzutil_slice_to_cstring` converts a data+size slice to a null-terminated C string.
- **Components:** `wiredtiger_open`, session management, multi-worker isolation
- **Notes:** The setup function is idempotent; subsequent calls within the same worker process reuse the existing connection and session.

## Coverage Notes

The fuzz suite uniquely exercises WiredTiger's internal config parser and modify-apply path under completely arbitrary byte-stream inputs, which is difficult to achieve with hand-written unit tests. The config fuzzer is especially valuable because the config parser accepts freeform strings from application code and is an attack surface for memory safety issues. The modify fuzzer exercises a complex memory-manipulation path (grow, pack, apply) that involves pointer arithmetic and length calculations. Gaps: only two targets (no fuzzing of cursor operations, packing codecs, B-tree page parsing, or schema operations); no corpus of known-interesting inputs is shipped in the repository; the `fuzz_coverage.sh` script provides coverage measurement but results are not automatically checked; running these targets requires a LibFuzzer-instrumented build.
