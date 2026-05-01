# test_disagg_failover_perf — Measures disaggregated storage failover (step-up) latency

**File:** `test/cppsuite/tests/test_disagg_failover_perf.cpp`
**Storage mode:** Disagg (disaggregated storage — palite page log)
**Components under test:** Disaggregated storage role transitions (leader → follower → leader), `pl_get_complete_checkpoint_ext`, `conn->reconfigure()` disagg role change, `WT_STAT_CONN_DISAGG_STEP_UP_TIME`, metrics writer

## Overview

This test measures how long it takes for a WiredTiger instance to "step up" from follower to leader mode in a disaggregated storage configuration. It simulates a failover scenario: the database is first populated and checkpointed in leader mode (or loaded from an existing backup), the connection is then closed and reopened in follower mode, the latest checkpoint is picked up via the page log API, and finally the instance is reconfigured back to leader mode. The step-up latency statistic (`WT_STAT_CONN_DISAGG_STEP_UP_TIME`) is read from connection statistics and written to a JSON perf file for comparison across runs. An optional workload phase (append or update) can run while in follower mode to test step-up under load.

## Configuration

This test does **not** use a cppsuite config file. All parameters are provided via command-line flags:

| Flag | Default | Description |
|---|---|---|
| `-c N` | 3 | Number of collections |
| `-g N` | 16 | Cache size in GB |
| `-h PATH` | `DEFAULT_DIR` | Home directory |
| `-i N` | 1 | Ingest size in MB (0 = skip workload) |
| `-k N` | 5000 | Keys per collection |
| `-s N` | 10 | Key size in bytes |
| `-v N` | 1000 | Value size in bytes |
| `-V N` | 0 | Verbose level (0=off; 1=WT_VERB_DISAGG; 2=palite module logging) |
| `-w N` | 0 | Cache warm-up percentage of initial data |
| `-C` | false | Save a backup copy of loaded data |
| `-L` | false | Skip populate; use existing data from `WT_TEST.back` |
| `-S SHAPE` | `updates` | Workload shape (`append` or `updates`) |

## Test Scenarios

### Scenario: Populate (leader mode)
- **What it tests:** Creates collections, inserts `key_count` keys per collection in individual single-key transactions with commit timestamps, advancing stable and oldest timestamps every 1,000 keys. Takes a checkpoint after each collection.
- **Components:** Leader-mode disagg storage, B-tree insert, checkpoint, timestamp management.
- **Notes:** Using `precise_checkpoint=true` and the palite page log extension. A final checkpoint is taken at the end of populate so no work needs to be abandoned on reconnect. Optionally, a backup copy of the database is saved with `-C` for repeated use with `-L`.

### Scenario: Connection close and reopen as follower
- **What it tests:** Closes the leader connection and reopens in follower mode (`disaggregated=(role="follower")`). This is the initial state for measuring step-up time.
- **Components:** Disagg role configuration, page log, connection lifecycle.
- **Notes:** Statistics logging (`statistics_log=(json,wait=1,on_close)`) and file manager settings are applied at follower open for FTDC compatibility.

### Scenario: Checkpoint pick-up (follower mode)
- **What it tests:** Calls `pl_get_complete_checkpoint_ext` on the palite page log to retrieve the latest completed checkpoint metadata, then reconfigures the connection with that metadata via `conn->reconfigure(disaggregated=(checkpoint_meta="..."))`.
- **Components:** `WT_PAGE_LOG::pl_get_complete_checkpoint_ext`, `conn->reconfigure`, page log API.
- **Notes:** This simulates what a new leader would do after detecting that the previous leader failed.

### Scenario: Cache warming (optional)
- **What it tests:** If `warm_cache_pct > 0`, scans the specified percentage of the initial dataset into the cache before the workload and step-up measurement. Biases toward lower-numbered collections.
- **Components:** Cursor traversal, cache warm path.
- **Notes:** Affects step-up measurement by changing how much data needs to be read from the page log vs served from cache during the role transition.

### Scenario: Workload phase (optional — follower mode)
- **What it tests:** Runs `ingest_size_mb` worth of either append (new key insertions) or update (random key overwrites) operations in follower mode before the step-up. 10 operations are batched per collection visit.
- **Components:** B-tree insert/update, cursor management, timestamp advancement.
- **Notes:** Testing step-up latency with a dirty cache / pending changes provides a more realistic failover scenario.

### Scenario: Step-up to leader — latency measurement
- **What it tests:** Calls `conn->reconfigure(disaggregated=(role="leader"))` and sets the stable timestamp to the checkpoint's timestamp. Reads `WT_STAT_CONN_DISAGG_STEP_UP_TIME` from the connection statistics and writes it to a JSON perf output file via `metrics_writer`.
- **Components:** Disagg role transition, `WT_STAT_CONN_DISAGG_STEP_UP_TIME`, metrics writer.
- **Notes:** A 10-second sleep after reconfiguration is included to allow FTDC files to flush before the connection closes. The step-up time is the primary metric of interest for this test.

## Key Observations

- This is the only disagg-specific test in the cppsuite test suite; it requires the palite page log extension (`libwiredtiger_palite.so`) to be present.
- The test is a standalone program (its own `main()`) rather than a cppsuite framework test class. It does not use the workload manager, operation tracker, or timestamp manager component.
- The `-L` (load skip) and `-C` (load copy) flags allow the expensive populate phase to be run once and reused across many measurement runs, making it practical for iterative performance testing.
- Step-up latency depends heavily on the size of the dataset, cache warmth, and whether a workload was running during follower mode. The `-w` and `-i` flags control these variables.
- The test does not assert a specific latency bound; it only records the measurement. Pass/fail is determined externally by comparing the output JSON against a baseline.
- Verbose level `-V 2` enables palite module logging in addition to WiredTiger disagg verbosity, useful for diagnosing step-up delays.
