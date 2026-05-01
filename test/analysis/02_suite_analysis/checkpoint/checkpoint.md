# checkpoint — Multi-threaded checkpoint correctness and recovery test

**Path:** `test/checkpoint/`
**Language:** C
**Storage mode:** General (with optional tiered/disagg storage variants)
**Components under test:** checkpoint engine, transaction timestamps, history store, prepared transactions, precise checkpoint, rollback-to-stable, tiered/disaggregated storage, eviction

## Overview

This test concurrently runs multiple worker threads performing insert, modify, range-remove, and search operations against a set of tables (row-store and/or column-store), while a dedicated checkpoint thread continuously takes checkpoints and verifies cross-table consistency. An optional clock thread advances stable and oldest timestamps. The test validates that every checkpoint produces a consistent, coherent snapshot across all tables and that the database survives crash-recovery correctly. A companion shell script (`recovery-test.sh`) snapshots the live database at arbitrary points during the run and then verifies recovery.

## Test Scenarios / Cases

### Scenario: Basic multi-table checkpoint consistency
- **What it tests:** Worker threads write the same key/value to every table in a single transaction. The checkpoint thread opens cursors on all tables at the last checkpoint and walks them in parallel, asserting that every table contains the same keys and values in the same order.
- **Components:** Checkpoint engine, cursor API, row-store and column-store B-tree, session/transaction layer
- **Notes:** Covers both `WiredTigerCheckpoint` (default) and named checkpoints (`-c` flag). Runs with row, column-store variable-length, or mixed table types.

### Scenario: Timestamp-driven checkpointing
- **What it tests:** A clock thread monotonically increments the stable timestamp. Workers commit at or above the current stable timestamp. The checkpoint thread verifies consistency at the exact stable timestamp and at a randomly chosen timestamp between oldest and stable.
- **Components:** Transaction timestamp subsystem (`set_timestamp`, `query_timestamp`), history store, checkpoint with `use_timestamp=true`
- **Notes:** Enabled by `-x` (timestamps) or `-X` (race timestamps: oldest advanced atomically with stable). The `-R` predictable-replay mode uses deterministically assigned per-thread/per-iteration timestamps to allow exact replay up to a given `-S stop_ts`.

### Scenario: Prepared transaction discovery
- **What it tests:** When `precise_checkpoint=true` and `prepare=true`, after the run completes the test opens a `prepared_discover:` cursor to enumerate all dangling prepared transactions, then randomly commits or rolls each one back, takes a final checkpoint, and re-verifies consistency.
- **Components:** Prepared transactions, `prepare_transaction`, `claim_prepared_id`, `prepared_discover` cursor, precise checkpoint
- **Notes:** Only exercised when `-p` and `-e` flags are both present. Simulates crash recovery of prepared-but-not-resolved transactions.

### Scenario: Timing stress variants
- **What it tests:** Various internal timing-stress and failpoint configurations are injected to expose races in the checkpoint and eviction paths.
- **Components:** Checkpoint slow path (`checkpoint_slow`), history store checkpoint delay, eviction reposition, eviction split failpoint, `failpoint_history_store_delete_key_from_ts`, `failpoint_rec_before_wrapup`, aggressive sweep
- **Notes:** Each stress mode is enabled individually via `-s 1`…`-s 8`. Sweep stress adds aggressive file-handle closure to exercise cursor reopen paths.

### Scenario: Cursor reopen under live transactions
- **What it tests:** Workers randomly close and reopen cursors both within and between transactions to verify that cursor lifecycle management does not corrupt ongoing operations.
- **Components:** Cursor open/close, session cursor cache (disabled in this test via `cache_cursors=false`)
- **Notes:** Reopen probability is ~1/13 across transactions and ~1/15 during a transaction.

### Scenario: Crash recovery via shell script
- **What it tests:** `recovery-test.sh` runs the test binary in the background, periodically suspends it with SIGSTOP, copies the live database directory, and then re-runs the binary in verify-only mode (`-v`) against the copy to confirm recovery produces a consistent state.
- **Components:** WAL (logging), crash recovery, checkpoint cursor
- **Notes:** Tests the invariant that any mid-run snapshot of the database directory is recoverable to a consistent checkpoint. The `-e`/`-x` flags are forwarded to the recovery run when precise_checkpoint is active.

### Scenario: Disaggregated / tiered storage
- **What it tests:** When `--disagg` or `--tiered` options are passed (via `testutil_parse`), tables are created with `type=layered,block_manager=disagg` and the test uses `flush_tier` in addition to checkpoint. Only row-store tables are supported. `precise_checkpoint` is automatically enabled.
- **Components:** Tiered storage, disaggregated block manager, layered tables, flush_tier
- **Notes:** Checkpoint cursor verification is skipped for disagg (FIXME-WT-15357). Cursor `modify` is skipped for disagg (FIXME-WT-16479).

## Coverage Notes

This test uniquely combines concurrent writes, multi-table cross-consistency verification at both live and checkpoint snapshots, timestamp validation at arbitrary points in the oldest–stable window, and crash-recovery confirmation. It is one of the few tests that exercises prepared-transaction discovery (`prepared_discover:` cursor) and its interaction with precise checkpoints. The predictable-replay mode (`-R`) adds the ability to reproduce a specific execution from seed values, which is valuable for debugging timestamp-related checkpoint failures. Gaps: no explicit test of named-checkpoint rollback-to-stable interaction; eviction-path coverage depends on randomised timing rather than deterministic injection (except for the `-s` stress modes).
