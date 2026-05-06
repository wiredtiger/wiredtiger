# test_disagg_checkpoint_size02 — Database-level size in the checkpoint completion record

**File:** `test/suite/test_disagg_checkpoint_size02.py`
**Storage mode:** Disagg (via `@disagg_test_class` decorator; always disagg)
**Components under test:** src/checkpoint, src/block_disagg (checkpoint completion record), ext/page_log/palite, src/conn/conn_layered*.c

## Infrastructure notes

`test_disagg_checkpoint_size02` is decorated with `@disagg_test_class`, which provides the
same infrastructure as described in test_disagg_checkpoint_size01.md:
- Mixes in `DisaggConfigMixin`.
- Sets up `follower/` and `kv_home/` directories via `early_setup`.
- Loads the page log extension via `conn_extensions`.
- Suppresses `WT_VERB_RTS` at shutdown.

`conn_config` is set at the class level:
`'disaggregated=(role="leader",lose_all_my_data=true)'`

The helper `get_database_size()` calls `self.disagg_get_complete_checkpoint_meta()` (from
`DisaggConfigMixin`), which invokes `conn.get_page_log(...).pl_get_complete_checkpoint_ext()`
to retrieve the page log's checkpoint completion record, and then parses the
`database_size=N` field from that string via regex.

`disagg_size_buffer = 1024 * 1024` (1 MiB) represents a fixed overhead added to all
database size calculations to account for new-database initialization.

`simulate_crash_restart` (imported from `helper`) is used in one test to simulate an
unclean shutdown and verify size durability.

There is no `make_scenarios` — runs as a single scenario per environment.

## Test Cases

### `test_disagg_checkpoint_size02.test_new_database`
- **What it tests:** Verifies two things: (1) before any checkpoint has been taken on an
  empty database, `pl_get_complete_checkpoint_ext` raises `WiredTigerError` (no completed
  checkpoint yet); (2) after creating one layered table and taking the first checkpoint,
  the `database_size` in the completion record is strictly greater than 1 MiB (the
  `disagg_size_buffer`), confirming initial metadata pages are accounted for.
- **Components:** `ext/page_log/palite` (`pl_get_complete_checkpoint_ext` error path),
  `src/checkpoint` (first checkpoint completion record write), `src/block_disagg`
- **Notes:**
  - The 1 MiB lower bound comes from the class constant `disagg_size_buffer`. The comment
    explains this represents the root page, leaf page of the shared metadata file, and the
    1 MiB buffer added for new databases.
  - Failure of the error-before-checkpoint assertion means the page log returns stale data
    from a previous run, which would corrupt size tracking.

### `test_disagg_checkpoint_size02.test_database_size_increases`
- **What it tests:** After an empty-database checkpoint, inserts 1000 rows and checkpoints
  (checks size grew), then inserts 2000 more rows and checkpoints again (checks size grew
  further). Verifies the `database_size` field increases monotonically with each batch of
  inserts and checkpoints.
- **Components:** `src/checkpoint`, `ext/page_log/palite`, `src/block_disagg`,
  `src/conn/conn_layered*.c`
- **Notes:**
  - Uses three sequential checkpoints and checks size at each step, giving stronger
    confidence than a single before/after comparison.
  - Failure means the checkpoint completion record's `database_size` is not updated, is
    computed incorrectly, or decreases unexpectedly.

### `test_disagg_checkpoint_size02.test_database_size_decreases`
- **What it tests:** Inserts 1000 rows and checkpoints (size grows), then removes 900 of
  them (keys 100–999) in a single transaction and checkpoints. Asserts that the size after
  truncation is (a) strictly less than the size with data, and (b) still at or above
  `disagg_size_buffer` (1 MiB), confirming that deletes reclaim space in the size
  accounting while the minimum buffer is preserved.
- **Components:** `src/checkpoint`, `src/block_disagg`, `src/btree` (reconciliation of
  deleted pages), `ext/page_log/palite`
- **Notes:**
  - The `disagg_size_buffer` lower-bound check ensures the implementation never reports a
    size smaller than the minimum valid database footprint.
  - Failure means either deletions are not reflected in `database_size` (leak) or the size
    drops below the mandatory buffer (undercount).

### `test_disagg_checkpoint_size02.test_database_size_multiple_btrees`
- **What it tests:** Creates three separate layered tables, inserts 500 rows into each one
  sequentially, and takes a checkpoint after each batch. Records the size delta after each
  insert-and-checkpoint cycle (`delta1`, `delta2`, `delta3`). Asserts all three deltas are
  positive and within 10% of their mean, confirming that the database-level size accounts
  for all B-trees and that similar-sized writes produce proportionally similar size increases
  regardless of which table receives them.
- **Components:** `src/checkpoint`, `src/block_disagg`, `src/conn/conn_layered*.c`
  (multi-table size aggregation in the checkpoint completion record), `ext/page_log/palite`
- **Notes:**
  - The 10% variance tolerance (`avg_delta * 0.9` to `avg_delta * 1.1`) accommodates minor
    differences in B-tree internal node overhead across tables.
  - Failure indicates that not all B-trees contribute their bytes to the total, or that the
    size aggregation has an off-by-one / table-count error.

### `test_disagg_checkpoint_size02.test_database_size_persists_across_restart`
- **What it tests:** Inserts 1000 rows, checkpoints, records the size, then reopens the
  connection (triggering the "Removing local file" disagg restart path). Reads the size
  again from the page log's completion record and asserts the two values are within 10%
  of each other (using `assertAlmostEqual` with a relative delta).
- **Components:** `src/checkpoint`, `ext/page_log/palite`, `src/conn` (restart and
  checkpoint meta pickup), `src/block_disagg`
- **Notes:**
  - The 10% tolerance is intentional: a checkpoint may run during shutdown or startup,
    slightly changing the size.
  - The `expectedStdoutPattern("Removing local file")` guard confirms the correct disagg
    restart path is taken.
  - Failure means the `database_size` stored in the page log is either lost or not read
    back correctly after a clean restart.

### `test_disagg_checkpoint_size02.test_failed_checkpoint_no_size_change`
- **What it tests:** Inserts 1000 rows, checkpoints (records size), then inserts 100 more
  rows without checkpointing, and immediately calls `simulate_crash_restart` to simulate
  an unclean shutdown. After restart, reads the size from the page log and asserts it
  equals the pre-crash size exactly, confirming that uncommitted (not-yet-checkpointed)
  data does not pollute the `database_size` field.
- **Components:** `ext/page_log/palite` (checkpoint completion record durability),
  `src/checkpoint`, `src/block_disagg`, `helper.simulate_crash_restart`
- **Notes:**
  - `simulate_crash_restart` copies the home directory to `RESTART/` and reopens it,
    simulating a crash (no clean shutdown checkpoint).
  - This is a correctness test for crash recovery: the size field must reflect only
    completed checkpoints.
  - The `expectedStdoutPattern("Removing local file")` guard applies to the crash-restart
    path.
  - Failure means the size can be inflated by writes that were never durably checkpointed,
    corrupting capacity reporting after a crash.
