# test_stat01 — Basic statistics cursor operations

**File:** `test/suite/test_stat01.py`
**Storage mode:** General
**Components under test:** statistics cursor, block manager, btree, backup stats

## Test Cases

### `test_stat01.test_basic_conn_stats`
- **What it tests:** Opens a connection-level statistics cursor and verifies that `block-manager: blocks written` has a value >= 10 after populating a dataset and checkpointing; verifies key/value type consistency and that stat index lookup is self-consistent.
- **Components:** `stat.c`, `block_mgr.c`
- **Notes:** Parameterized over `file:` and `table:` URI types and column vs. string-row key formats.

### `test_stat01.test_basic_data_source_stats`
- **What it tests:** Opens a data-source statistics cursor on a URI; checks `btree: maximum leaf page size` >= 8192, `btree: maximum internal page size` >= 4096, `btree: overflow pages` >= 10 after writing large overflow values; verifies `statistics=(size)` cursor also works; also reads backup stats without running a backup.
- **Components:** `stat.c`, `btree`, `block_mgr.c`
- **Notes:** Forces the table to disk with `reopen_conn()` to produce on-disk overflow pages. Parameterized by URI type and key format.

### `test_stat01.test_checkpoint_stats`
- **What it tests:** Creates named checkpoints `first`, `second`, `third`; opens a per-checkpoint statistics cursor and verifies `btree_entries` equals the expected entry count.
- **Components:** `stat.c`, `checkpoint.c`, `btree`
- **Notes:** Skipped for timestamp hook. Parameterized by URI type and key format.

### `test_stat01.test_missing_file_stats`
- **What it tests:** Verifies that opening a statistics cursor on a non-existent file raises `WiredTigerError`.
- **Components:** `stat.c`
- **Notes:** Error path test; no parameterization.
