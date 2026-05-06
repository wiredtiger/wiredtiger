# wt8246_compact_rts_data_correctness — Compact + rollback-to-stable data correctness after crash

**Path:** `test/csuite/wt8246_compact_rts_data_correctness/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-8246
**Components under test:** `session->compact`, rollback-to-stable (RTS) during recovery, `compact_slow` timing stress, foreground and background compaction, timestamped updates, row-store, column-store

## What This Test Does
This test verifies that data is correct after compaction is interrupted by a crash (SIGKILL) and rollback-to-stable is applied during recovery. A child process populates 800,000 records, applies four rounds of timestamped updates (value_a at ts=20, value_b at ts=30, value_c at ts=40, value_d at ts=50), pins the stable timestamp at 30, removes records to give compaction work, and then starts either foreground or background compaction. The parent kills the child as soon as compaction begins (sentinel file). After recovery (which runs RTS to stable=30), the parent verifies that all records are visible with value_a at ts=20 and value_b at ts=30/40/50, confirming that RTS correctly rolled back the post-stable updates.

## Test Scenarios / Cases

### Scenario: Row-store foreground compact interrupted, RTS after recovery
- **What it tests:** That after a crash during foreground `session->compact` and RTS applied at stable=30, records show value_a at ts=20 and value_b at ts≥30 (ts=40 and ts=50 rolled back to value_b, the stable version).
- **Components:** `session->compact`, fork/SIGKILL, RTS, `compact_slow` timing stress, `debug_mode=(background_compact)`, timestamped `cursor->insert`/`cursor->remove`.
- **Notes:** NUM_RECORDS=800,000, TIMEOUT=1s (parent kills very quickly after compaction starts). `free_space_target=1MB` compact config.

### Scenario: Row-store background compact interrupted, RTS after recovery
- **What it tests:** Same data correctness check when background compaction is enabled (`background=true`). The sentinel file is created after the compact API returns (since background compact returns immediately), then the parent waits 5 seconds before killing.
- **Components:** Background compact (`session->compact(NULL, "background=true,free_space_target=1MB")`), RTS recovery.

### Scenario: Column-store foreground compact interrupted, RTS after recovery
- **What it tests:** Same correctness check using column-store (`key_format=r`).
- **Components:** Column-store, foreground compact, RTS recovery.

### Scenario: Column-store background compact interrupted, RTS after recovery
- **What it tests:** Same correctness using column-store with background compaction.
- **Components:** Column-store, background compact, RTS recovery.

## LazyFS Variant
None.
