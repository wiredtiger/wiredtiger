# random_abort — Crash-recovery correctness test with insert/modify/delete workload

**Path:** `test/csuite/random_abort/`
**Language:** C
**Storage mode:** General
**Jira ticket:** N/A
**Components under test:** Log-based recovery, cursor insert/modify/remove, column-store and row-store tables, LazyFS integration

## What This Test Does
This test forks a child process that runs multiple writer threads (5–12) continuously performing inserts, deletes, and modify operations on both a row-store and a column-store table. The parent process waits a random amount of time (10–40 s), then sends SIGKILL to the child. The database is then reopened with log recovery and verified: every operation that was recorded in the per-thread log files must be present and consistent in the recovered database, with no missing records appearing after the last observed gap.

## Test Scenarios / Cases

### Scenario: Default (no-sync) log write mode
- **What it tests:** Recovery completeness when transactions are written to the log without `fsync`, simulating a typical low-latency workload. Every inserted record must survive recovery.
- **Components:** Log recovery, row-store and column-store B-trees.
- **Notes:** In-memory log buffering mode (`-m`) is also supported; with that flag, records missing at the tail are tolerated.

### Scenario: LazyFS power-failure simulation
- **What it tests:** Recovery under a file-system that does not persist writes until explicitly flushed (`fsync`). Enabled by `-l` or implicitly when `lazyfs_is_implicitly_enabled()` returns true.
- **Components:** LazyFS, log with `method=fsync`, cache-clear before recovery.
- **Notes:** The `smoke_lazyfs.sh` script drives this variant. Cache is explicitly cleared before reopening so only committed-to-disk data is seen.

### Scenario: Compaction co-stress (`-c` flag)
- **What it tests:** That background compaction running alongside heavy inserts does not break log recovery. Compaction is triggered every 100,000 ops per thread.
- **Components:** `session->compact()`, concurrent inserts, log recovery.
- **Notes:** `EBUSY` from compaction is treated as acceptable.

### Scenario: Compatibility mode (`-C` flag)
- **What it tests:** Recovery correctness when the database is opened with compatibility constraints (`TESTUTIL_ENV_CONFIG_COMPAT`).
- **Components:** Compatibility configuration, log format.
- **Notes:** Orthogonal to all other scenarios.

## LazyFS Variant
Yes — `smoke_lazyfs.sh` exercises the LazyFS power-failure path.
