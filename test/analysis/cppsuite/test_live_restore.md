# test_live_restore — Validates live restore file system behaviour under concurrent CRUD operations

**File:** `test/cppsuite/tests/test_live_restore.cpp`
**Storage mode:** General (live restore mode — `live_restore_fs.c`)
**Components under test:** Live restore file system (`live_restore=(enabled=true)`), background data migration threads, connection reopen/recovery, cursor insert/update/read, `session->truncate()`, `session->compact()` (file truncation), `WT_STAT_CONN_LIVE_RESTORE_STATE`

## Overview

This test validates WiredTiger's live restore feature, which copies data from a source directory to a new home directory in the background while allowing concurrent database operations. The test creates an initial database, backs it up as the live restore source, then repeatedly opens WiredTiger in live restore mode and performs random CRUD operations while the background migration threads copy files from source to destination. The test verifies that the engine reaches `WT_LIVE_RESTORE_COMPLETE` state, at which point the source directory is deleted to confirm no further source accesses occur. Optional modes include: simulated crash (SIGKILL mid-run), recovery from a crashed run, subdirectory layout (simulating MongoDB directory-per-db), and iterative runs where each restored database becomes the next source.

## Configuration

This test does **not** use a cppsuite config file. All parameters are command-line options:

| Flag | Default | Description |
|---|---|---|
| `-c N` | `INT64_MAX` | Maximum number of collections |
| `-D` | false | Subdirectory mode (simulates MongoDB directory-per-db) |
| `-d` | false | Death mode: send SIGKILL at a random iteration |
| `-H PATH` | `DEFAULT_DIR` | Home directory |
| `-h` | — | Print usage and exit |
| `-i N` | 2 | Number of full restore iterations |
| `-l N` | 0 | Log level (0=ERROR, 1=WARN, 2=INFO, 3=TRACE) |
| `-o N` | 20,000 | Number of CRUD operations per restore run |
| `-r` | false | Recovery mode: reopen existing home after crash |
| `-t N` | 4 | Background migration thread count |
| `-v N` | 0 | WiredTiger verbose level for live restore logging |

**Hardcoded parameters:**

| Parameter | Value |
|---|---|
| `key_size` | 100 |
| `value_size` | 50,000 |
| `cache_size` | 5 GB |
| `read_size` | 2 MB (live restore block size) |
| Page sizes | `allocation_size=512B`, `internal_page_max=512B`, `leaf_page_max=512B` (forces many pages) |

## Test Scenarios

### Scenario: Initial database creation (first run)
- **What it tests:** Creates a fresh database, runs `op_count` random CRUD operations (heavily biased toward inserts for a warmup period of `op_count/3` operations), then closes the connection.
- **Components:** B-tree insert, update, checkpoint, connection create.
- **Notes:** Tiny page sizes (512B) are used to maximise the number of files and pages, exercising the live restore file system more extensively.

### Scenario: Backup and source setup
- **What it tests:** Reopens the home directory with logging enabled, uses a `backup:` cursor to copy all files (including WAL log files) to the source path, then deletes the original home. The backup becomes the source for the next live restore iteration.
- **Components:** `backup:` cursor, file copy, WAL log handling, subdirectory creation.
- **Notes:** Log files are placed in a `journal/` subdirectory. In subdirectory mode (`-D`), nested directories are created to simulate MongoDB's collection directory structure.

### Scenario: Live restore run — concurrent CRUD during background migration
- **What it tests:** Opens WiredTiger in live restore mode (`live_restore=(enabled=true,read_size=2MB,threads_max=N,path=SOURCE)`), then runs 90% of `op_count` random CRUD operations concurrently with background file migration. Operations include:
  - Writes (90%): random insert or update.
  - Reads (10%): random `next_random=true` cursor access.
  - Checkpoints (rare): explicit `session->checkpoint()`.
  - Connection reopens (rare): close and reopen in live restore mode mid-run.
  - File truncations (very rare): truncate from a random key to end-of-collection, then compact.
- **Components:** Live restore file system, background migration threads, cursor operations, checkpoint.
- **Notes:** Connection reopens test that live restore survives session/connection lifecycle events during migration.

### Scenario: Wait for live restore completion
- **What it tests:** Polls `WT_STAT_CONN_LIVE_RESTORE_STATE` every second until `WT_LIVE_RESTORE_COMPLETE` is returned.
- **Components:** `WT_LIVE_RESTORE_STATE` statistic, background migration state machine.
- **Notes:** Completion is the gate: after this point, the source directory is deleted.

### Scenario: Post-completion source deletion and remaining CRUD
- **What it tests:** Deletes the source directory, then runs the remaining 10% of CRUD operations. Any access to source files after deletion would cause a crash, making this an implicit correctness check that live restore fully migrated all needed data.
- **Components:** Live restore completion guarantee, post-migration file system independence.
- **Notes:** Connection reopens are disabled for this phase (since the source is gone, a reopen with the live restore path would fail).

### Scenario: Death mode (optional)
- **What it tests:** If `-d` is specified, a random iteration sends SIGKILL to the process mid-run. The next run with `-r` starts recovery from the partially restored database, testing that live restore + WAL recovery correctly handles a crash during migration.
- **Components:** Recovery (`reopen` in recovery mode), live restore crash consistency.
- **Notes:** Recovery mode is restricted to 1 iteration (`-r` and `-i > 1` is an error).

### Scenario: Subdirectory mode (optional)
- **What it tests:** Creates collection files in a nested subdirectory structure (`SUB_DIR/SUB_DIR/collection_N`) to simulate MongoDB's directory-per-db and directory-for-indexes configurations.
- **Components:** Live restore file system directory handling, nested path resolution.
- **Notes:** The backup logic explicitly creates the nested subdirectory in the source path.

## Key Observations

- This test directly exercises `live_restore_fs.c`, the WiredTiger virtual file system layer that intercepts file operations during live restore.
- The tiny page sizes (512B) are intentional to create many small files and pages, maximising the complexity of the file migration.
- The iterative design (each restored database becomes the next source) provides progressively deeper testing of the restore chain without requiring external setup.
- The death mode + recovery combination is the only cppsuite test that intentionally kills the process mid-run and validates recovery, making it uniquely suited for testing crash consistency in the live restore path.
- No cppsuite framework components (operation tracker, metrics monitor, timestamp manager) are used; correctness is validated implicitly by the absence of crashes and by the post-completion source-deletion check.
- A known limitation: `remove()` is not yet enabled in the write path (commented out with a TODO), so the workload is insert/update only.
