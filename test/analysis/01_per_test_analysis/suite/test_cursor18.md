# test_cursor18 — Version cursor: update chain, on-disk, history store, timestamps, prepare states

**File:** `test/suite/test_cursor18.py`
**Storage mode:** General
**Components under test:** version cursor (dump_version), history store, MVCC, prepared transactions, cross_key, start_timestamp

## Test Cases

### `test_cursor18.test_update_chain_only`
- **What it tests:** Version cursor reading updates that exist only in the in-memory update chain (not yet reconciled).
- **Components:** `src/cursor/cur_version.c`, `src/btree/`

### `test_cursor18.test_ondisk_only`
- **What it tests:** Version cursor reading a single committed value that has been reconciled to disk.
- **Components:** `src/cursor/cur_version.c`, `src/btree/bt_read.c`

### `test_cursor18.test_ondisk_only_with_deletion`
- **What it tests:** Version cursor reading an on-disk value that was subsequently deleted (tombstone on disk).
- **Components:** `src/cursor/cur_version.c`, `src/btree/`

### `test_cursor18.test_ondisk_with_deletion_on_update_chain`
- **What it tests:** Version cursor reading on-disk value with a deletion in the in-memory update chain.
- **Components:** `src/cursor/cur_version.c`, `src/btree/`

### `test_cursor18.test_ondisk_with_hs`
- **What it tests:** Version cursor reading a key with values in both on-disk and history store locations.
- **Components:** `src/cursor/cur_version.c`, `src/history/hs_cursor.c`

### `test_cursor18.test_update_chain_ondisk_hs`
- **What it tests:** Version cursor reading a key with values across all three locations: update chain, on-disk, and history store.
- **Components:** `src/cursor/cur_version.c`, `src/history/hs_cursor.c`

### `test_cursor18.test_prepare`
- **What it tests:** Version cursor reading a prepared (uncommitted) update.
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor18.test_reuse_version_cursor`
- **What it tests:** Reusing a version cursor across multiple searches without closing/reopening.
- **Components:** `src/cursor/cur_version.c`

### `test_cursor18.test_prepare_tombstone`
- **What it tests:** Version cursor reading a prepared tombstone (prepared delete).
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor18.test_search_when_positioned`
- **What it tests:** Version cursor search when already positioned; verifies repositioning behavior.
- **Components:** `src/cursor/cur_version.c`

### `test_cursor18.test_concurrent_insert`
- **What it tests:** Version cursor behavior when another session concurrently inserts into the same key.
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor18.test_skip_invisible_updates`
- **What it tests:** With `visible_only=true`, version cursor skips updates not visible to the reading transaction.
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor18.test_skip_prepare_update_chain` / `test_skip_prepare_on_disk` / `test_skip_prepare_tombstone_and_full_value_on_disk` / `test_skip_tombstone_on_disk`
- **What it tests:** Version cursor with `visible_only=true` skipping various types of prepared/tombstone updates at different storage locations.
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor18.test_unpositioned_cursor`
- **What it tests:** Calling next/get_value on an unpositioned version cursor; expects error.
- **Components:** `src/cursor/cur_version.c`

### `test_cursor18.test_multiple_keys`
- **What it tests:** Version cursor with `cross_key=true` iterating across multiple keys in a single traversal.
- **Components:** `src/cursor/cur_version.c`

### `test_cursor18.test_update_chain_start_timestamp` / `test_update_chain_start_timestamp_with_remove` / `test_update_chain_start_timestamp_with_remove_exclusive`
- **What it tests:** Version cursor with `start_timestamp` filtering to read only versions at or after a given timestamp from the update chain.
- **Components:** `src/cursor/cur_version.c`, `src/txn/txn_timestamp.c`

### `test_cursor18.test_ondisk_start_timestamp` / `test_ondisk_with_deletion_on_update_chain_start_timestamp` / `test_ondisk_with_deletion_on_update_chain_start_timestamp_exclusive` / `test_ondisk_only_with_deletion_start_timestamp` / `test_ondisk_only_with_deletion_start_timestamp_exclusive` / `test_ondisk_with_hs_start_timestamp` / `test_ondisk_with_hs_start_timestamp_exclusive`
- **What it tests:** Version cursor with `start_timestamp` filtering on on-disk and history store values, including exclusive timestamp mode.
- **Components:** `src/cursor/cur_version.c`, `src/history/hs_cursor.c`, `src/txn/txn_timestamp.c`
- **Notes:** File URI only. Scenarios: row (`key_format=S`) and var (`key_format=r`). `cross_key` default and `cross_key=true` scenarios.
