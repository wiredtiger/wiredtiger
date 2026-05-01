# background_compact — Exercises the background compaction server under a mixed insert/truncate workload

**File:** `test/cppsuite/tests/background_compact.cpp`
**Storage mode:** General
**Components under test:** Background compaction server, truncation, insert, checkpoint, connection statistics (`WT_STAT_CONN_BACKGROUND_COMPACT_*`, `WT_STAT_CONN_BLOCK_BYTE_WRITE_COMPACT`)

## Overview

This test drives the WiredTiger background compaction server by creating conditions under which compaction has meaningful work to do: it inserts data continuously to grow files, then truncates 20% of each table's keys whenever free space falls below 10%, creating reclaimable space for the compactor to consolidate. A simulated "maintenance window" periodically pauses all write activity to allow the background compaction server to make progress uncontested. At the end of the run, a custom validator checks that compaction statistics show non-zero bytes recovered, files tracked, and successful compaction events.

## Configuration

**Config files:**
- `test/cppsuite/configs/background_compact_default.txt` — 30-second smoke run, 3 collections, 1M keys each
- `test/cppsuite/configs/background_compact_long.txt` — 2-hour stress run, 100 collections, 1M keys each

### Default config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 30 | Short smoke test |
| `cache_size_mb` | 10000 | 10 GB to accommodate large dataset |
| `background_compact_debug_mode` | true | Enables debug logging for compaction |
| `validate` | false | Custom validate() used instead |
| `collection_count` | 3 | |
| `key_count_per_collection` | 1,000,000 | Large initial dataset |
| `insert_config.thread_count` | 3 | One per collection |
| `insert_config.op_rate` | 10ms | |
| `remove_config.thread_count` | 1 | Truncation thread |
| `remove_config.op_rate` | 2s | |
| `custom_config.op_rate` | 10s | Maintenance window toggle interval |
| `background_compact_config.thread_count` | 1 | |
| `background_compact_config.free_space_target_mb` | 1 | |
| `background_compact_config.op_rate` | 60s | Enable/disable cycle |
| `checkpoint_config.op_rate` | 5s | |

### Long config key differences

| Parameter | Value |
|---|---|
| `duration_seconds` | 7200 (2 hours) |
| `collection_count` | 100 |
| `insert_config.thread_count` | 50 |
| `custom_config.op_rate` | 600s (10-minute windows) |
| `background_compact_config.op_rate` | 1200s |
| `checkpoint_config.op_rate` | 60s |

## Test Scenarios

### Scenario: Insert operation (continuous data growth)
- **What it tests:** Steady insertion of new key/value pairs to ensure files continue to grow, giving compaction material to work with.
- **Components:** B-tree insert path, transaction commit.
- **Notes:** Threads are paused entirely during the maintenance window via a `volatile bool` flag. Each thread has its own assigned collection to avoid lock contention.

### Scenario: Remove operation (truncation to free space)
- **What it tests:** Range truncation of up to 20% of a table's entries to create reclaimable space. Skips truncation if free space already exceeds 10% of file size. Takes a checkpoint after each truncation so statistics are up to date on the next iteration.
- **Components:** `session->truncate()`, B-tree cursor, per-file block statistics (`WT_STAT_DSRC_BTREE_ENTRIES`, `WT_STAT_DSRC_BLOCK_REUSE_BYTES`, `WT_STAT_DSRC_BLOCK_SIZE`).
- **Notes:** Uses random cursors (`next_random=true`) to pick a starting key; truncation range is randomly 0–100 records. Also paused during the maintenance window.

### Scenario: Custom operation (maintenance window toggle)
- **What it tests:** Periodically flips a shared `volatile bool` flag that pauses all insert and remove threads, simulating a database maintenance window. This gives the background compaction server uncontested CPU and I/O to do work.
- **Components:** None (pure coordination logic).
- **Notes:** The toggle period is controlled by `custom_config.op_rate`.

### Scenario: Background compact operation (enable/disable cycling)
- **What it tests:** Alternately enables and disables background compaction (`compact background=true/false`) at the `free_space_target_mb` threshold.
- **Components:** Background compaction server.
- **Notes:** Expected never to fail on enable/disable calls.

### Scenario: Validate (post-run statistics check)
- **What it tests:** Asserts that all of the following connection-level statistics are positive after the run: `BACKGROUND_COMPACT_BYTES_RECOVERED`, `BACKGROUND_COMPACT_EMA`, `BLOCK_BYTE_WRITE_COMPACT`, `BACKGROUND_COMPACT_FILES_TRACKED`, `BACKGROUND_COMPACT_SKIPPED`, `BACKGROUND_COMPACT_SUCCESS`.
- **Components:** Background compaction statistics.
- **Notes:** Custom `validate()` override; the default framework validator is not used.

## Key Observations

- The maintenance window pattern is the key design feature: without it, constant writes would prevent compaction from reclaiming space fast enough to show measurable progress within the test duration.
- The `background_compact_debug_mode=true` flag in the default config enables internal compaction logging that is not present in the long config.
- The long config omits `validate=false`, meaning the default framework validation runs in addition to the custom one — however the custom `validate()` override replaces the default, so only the custom check executes.
- The 10% free-space threshold in the remove operation prevents unnecessary truncations when compaction is already making progress.
- The test is sensitive to disk speed; on slow storage the short default config may not generate enough reclaimable space for the assertions to pass, which is why `background_compact_debug_mode=true` is set only there.
