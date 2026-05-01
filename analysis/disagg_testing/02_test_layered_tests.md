# Disagg CI Testing — test_layered*.py Tests

> Category: Dedicated Python tests for the layered table implementation

---

## Overview

The `test_layered*.py` files are disagg-native Python tests — they are written specifically to test the layered table type directly, without relying on the hook mechanism. They use `helper_disagg.py` utilities to set up leader/follower connections, control checkpoints, switch roles, and verify consistency.

There are **103 files** total (some numbers are missing in the sequence):
- `test_layered01.py` through `test_layered98.py` (with gaps: missing 06, 10-13, 42, 95-96)
- `test_layered_cursor01.py`
- `test_layered_fast_truncate01.py` through `test_layered_fast_truncate08.py`
- `test_layered_modify01.py`

---

## How They Run in CI

These tests are part of the standard Python test suite (`test/suite/`). They run when the Python test suite runs. Under the **disagg hook**, they are re-run in layered mode (the hook is a no-op on `layered:` URIs since they're already layered).

**They are not separately scheduled** — they are caught by the general `unit-test` and `unit-test-hook-disagg-leader-bucket*` tasks.

---

## Complete Test Inventory

### test_layered01–09: Basic Layered Table APIs

| File | Description | Test Methods |
|---|---|---|
| test_layered01.py | Basic layered tree creation | `test_layered01` |
| test_layered02.py | Basic layered cursor creation | `test_layered02` |
| test_layered03.py | Basic cursor insert and read | `test_layered03` |
| test_layered04.py | Checkpoint in stable table | `test_layered04` |
| test_layered05.py | `search_near` edge cases: deleted keys, tombstones, iteration after search_near | 30 test methods covering all search_near scenarios |
| test_layered07.py | Second WT instance becomes leader; content appears in first | `test_layered07` |
| test_layered08.py | Read/write via page log API | `test_layered_read_write` |
| test_layered09.py | Leaf page delta: read/write, modify, delete, insert, multiple deltas | `test_layered_read_write`, `test_layered_read_modify`, `test_layered_read_delete`, `test_layered_read_insert`, `test_layered_read_multiple_delta`, `test_layered_read_delete_insert` |

### test_layered14–20: Cursors, Timestamps, Deltas

| File | Description | Test Methods |
|---|---|---|
| test_layered14.py | Layered random cursor | `test_layered_random_cursor`, `test_empty_table` |
| test_layered15.py | Start without local files | `test_layered15` |
| test_layered16.py | Modify operation on layered table | `test_modify` |
| test_layered17.py | Timestamp checking | `test_layered17` |
| test_layered18.py | Long delta chains | `test_layered18` |
| test_layered19.py | Adjustable consecutive deltas | `test_layered_read_write` |
| test_layered20.py | 32 consecutive deltas | `test_layered_read_write` |

### test_layered21–31: Leader/Follower Role Interactions

| File | Description | Test Methods |
|---|---|---|
| test_layered21.py | Insert on follower (role changing) | `test_insert_changing_roles` |
| test_layered22.py | Secondary reads/writes to ingest without stable | `test_secondary_reads_without_stable`, `test_secondary_modifies_without_stable`, `test_secondary_search_without_stable`, `test_largest_key_without_stable`, `test_getrandom_without_stable` |
| test_layered23.py | Leader-follower basic interaction | `test_leader_follower` |
| test_layered24.py | Follower dropping table doesn't fall back to stable | `test_layered24` |
| test_layered25.py | Start without local files + historical reads | `test_layered25` |
| test_layered26.py | Follower picks up checkpoint and adds stable component | `test_layered26` |
| test_layered27.py | Drain ingest table (insert/update/remove) | `test_drain_insert_update`, `test_drain_remove`, `test_drain_insert_remove_within_same_transaction`, `test_drain_remove_insert` |
| test_layered28.py | Drop layered tables; sweep doesn't crash | `test_create_drop`, `test_create_drop_checkpoint`, `test_create_drop_follower` |
| test_layered29.py | Create large number of layered tables | `test_create_tables` |
| test_layered30.py | Create empty tables | `test_layered30` |
| test_layered31.py | Follower picking up new checkpoints | `test_layered31` |

### test_layered32–50: Internal Page Deltas, GC, Eviction

| File | Description | Test Methods |
|---|---|---|
| test_layered32.py | Internal page deltas written to page log | `test_internal_page_delta_simple`, `test_internal_page_delta_split_internal` |
| test_layered33.py | Delete on ingest table | `test_delete` |
| test_layered34.py | Materialization frontier | `test_layered34` |
| test_layered35.py | Skip empty delta in leaf page delta | `test_layered_skip_empty_delta` |
| test_layered36.py | Create missing stable tables | `test_layered36` |
| test_layered37.py | Pin content in ingest table | `test_ping_ingest_table` |
| test_layered38.py | GC of redundant content in ingest table | `test_gc_ingest_table`, `test_gc_ingest_table_with_remove`, `test_gc_ingest_with_cursor`, `test_gc_ingest_with_no_open_cursor` |
| test_layered39.py | Don't evict pages ahead of materialization frontier | `test_layered39` |
| test_layered40.py | Layered table metadata has logging disabled | `test_layered40` |
| test_layered41.py | Duplicate key | `test_dup_key` |
| test_layered43.py | Disagg storage with block cache | `test_layered43` |
| test_layered44.py | Don't read freed pages | `test_layered44` |
| test_layered45.py | Durable entries not included in new delta | `test_normal_update`, `test_delete`, `test_delete_update_restore`, `test_prepare_update`, `test_prepare_delete`, `test_prepare_update_delete` |
| test_layered46.py | (no description) | `test_layered46` |
| test_layered47.py | Prune ingest tables on follower during checkpoint pick-up | `test_prune_timestamp_initialization`, `test_checkpoint_order_mismatch`, `test_first_gc_with_cursor_on_previous_checkpoint` |
| test_layered48.py | No overflow keys/values in disagg storage | `test_layered48` |
| test_layered49.py | User tombstones not removed until in checkpoint | `test_remove`, `test_truncate` |
| test_layered50.py | Evict on follower without page materialization frontier | `test_evict_on_standby` |

### test_layered51–75: Internals, Compression, Timestamps

| File | Description | Test Methods |
|---|---|---|
| test_layered51.py | Error if logging configured for layered table | `test_create_logged` |
| test_layered52.py | Internal page deltas with deleted leaf page | `test_layered52` |
| test_layered53.py | Checkpoint to capture stable timestamp only | `test_layered53` |
| test_layered54.py | Prefix/suffix compression for page deltas | `test_page_split_delta`, `test_verify_compression` |
| test_layered55.py | No obsolete time window review for readonly btree on follower | `test_obsolete_time_window` |
| test_layered56.py | No page delta on page split | `test_page_split_delta` |
| test_layered57.py | Follower never uses app threads to evict pages with updates | `test_follower_not_do_app_evict` |
| test_layered58.py | Cursor walk with delta | `test_cursor_walk_with_delta` |
| test_layered59.py | No internal page delta if first key modified | `test_single_update`, `test_inserts_to_split` |
| test_layered60.py | Create empty tables while checkpoint running | `test_layered60` |
| test_layered61.py | Ingest table timestamps not cleared when globally visible | `test_layered61` |
| test_layered62.py | Step down concurrent with checkpoint | `test_layered62` |
| test_layered63.py | Internal page deltas (regression) | `test_internal_page_deltas` |
| test_layered64.py | Checksum of checkpoint metadata | `test_layered64` |
| test_layered65.py | GC: prepared/aborted updates not removed if rollback timestamp newer than checkpoint | `test_prepared_insert` |
| test_layered66.py | Error evicting non-materialized pages on file close | `test_layered66` |
| test_layered67.py | Write update-restore page even when deltas disabled | `test_uncommit_eviction` |
| test_layered68.py | Address cookie upgrade/downgrade safety | `test_layered68` |
| test_layered69.py | Reconciliation with prepared rollback | `test_rollback_prepared_update` |
| test_layered70.py | Skip write when reconciliation makes no progress | `test_skip_write_full_page` |
| test_layered71.py | Drop empty tables while checkpoint running | `test_layered71` |
| test_layered72.py | Read pinned history store on standby | `test_layered72` |
| test_layered73.py | Cursor prepare conflict handling | `test_search_near_key_preserved_on_prepare_conflict` |
| test_layered74.py | Internal page deltas (additional scenarios) | `test_internal_page_deltas` |
| test_layered75.py | File IDs for tables with predefined IDs | `test_layered75` |

### test_layered76–98: Role Transitions, Bounds, Followers

| File | Description | Test Methods |
|---|---|---|
| test_layered76.py | Checkpoint size verification | `test_ckpt_size_verify_simple`, `test_ckpt_size_verify_multi_insert`, `test_ckpt_size_verify_large_dataset`, `test_ckpt_size_verify_many_ckpt` |
| test_layered77.py | Leader-to-follower transition with dirty eviction during prior checkpoint split | `test_step_down_dirty_eviction` |
| test_layered78.py | Remove returns WT_NOTFOUND for non-existent key | `test_delete_non_existent_key` |
| test_layered79.py | GC during eviction removes on-disk value for key | `test_updated_key_on_disk_value_removed_after_gc` |
| test_layered80.py | Sweep server doesn't close ingest/layered dhandles during step-up | `test_layered_dhandle_not_swept_during_stepup` |
| test_layered81.py | Follower picks up updated data after new checkpoint | `test_unpositioned_cursor_sees_new_data`, `test_cursor_position_preserved`, `test_read_timestamp_triggers_advance`, `test_data_changes_visible` |
| test_layered82.py | Cursor bounds on layered cursors (1000-key dataset) | `test_cursor_bounds_basic`, `test_cursor_bounds_insert` |
| test_layered83.py | Cursor iteration and iteration after search/search_near on layered cursors | `test_cursor_iteration`, `test_search_near_iteration` |
| test_layered84.py | Cursor walks on follower with advanced checkpoint | `test_overwrite_update_forces_stable_open`, `test_prepared_conflict_mid_walk` |
| test_layered85.py | Mid-scan checkpoint advances on follower cursor | `test_mid_scan_advance_with_read_timestamp` |
| test_layered86.py | Follower picks up and applies new file IDs | `test_standby_uses_table_id_high_water_mark` |
| test_layered87.py | RTS does nothing in disagg context | `test_layered87` |
| test_layered88.py | Unsupported cursor/table operations return clear errors | `test_readonly`, `test_reverse_collator` |
| test_layered89.py | Follower cursor next/prev/search return committed values, no PREPARE_CONFLICT | `test_search_committed`, `test_next_committed`, `test_prev_committed` |
| test_layered90.py | Follower picks up multiple checkpoints for same table | `test_follower_picks_up_updated_checkpoint` |
| test_layered91.py | Layered cursor iteration | `test_layered_iteration` |
| test_layered92.py | reserve() on layered cursors for various key states | `test_leader_key_exists`, `test_leader_key_missing`, `test_follower_key_in_stable_only`, `test_follower_key_in_ingest_only` |
| test_layered93.py | Cursor operations on follower for stable-only keys | `test_cursor_operations` |
| test_layered94.py | Prepared transaction at step-up time can commit or roll back | `test_prepared_insert`, `test_prepared_update`, `test_prepared_delete` |
| test_layered97.py | Follower insert/update: only open stable when overwrite=false | `test_follower_insert_overwrite_does_not_open_stable`, `test_follower_update_with_overwrite_false_opens_stable` |
| test_layered98.py | Cached cursors on follower | `test_standby_open_cursor` |

### Specialized test_layered_* Tests

| File | Description | Test Methods |
|---|---|---|
| test_layered_cursor01.py | Cursor operations (empty, populated, updates, removes) | 12 test methods covering various mix of updates/removes by percentage |
| test_layered_fast_truncate01.py | Basic fast truncate: truncate, rollback, write conflicts | `test_truncate_basic`, `test_truncate_rollback`, `test_truncate_write_conflict_1`, `test_truncate_write_conflict_2` |
| test_layered_fast_truncate02.py | Follower picks up checkpoint with fast-truncated pages: visibility and cursor behavior | `test_visibility`, `test_pre_truncation_read_sees_all_rows`, `test_cursor_scanning` |
| test_layered_fast_truncate03.py | Follower: stable pages not dirtied; deleted state survives eviction and reopen | `test_no_dirty_on_read`, `test_page_split_with_ingest_writes`, `test_state_preserved_on_reopen`, `test_instantiation_not_globally_visible` |
| test_layered_fast_truncate04.py | Follower cursor read-path over fast-truncated ranges (next/prev, search_near, open-ended) | 13 test methods |
| test_layered_fast_truncate05.py | cursor.next_random over fast-truncated ranges on follower | `test_random_cursor_skips_truncated_range`, `test_random_cursor_skips_truncated_range_with_live_ingest` |
| test_layered_fast_truncate06.py | Regression WT-17267: verify() forces dhandle close/reopen; truncate list survives | `test_verify_preserves_follower_truncate` |
| test_layered_fast_truncate07.py | Follower truncate with NULL start/stop resolved to first/last key | `test_bounded_range`, `test_null_start_resolves_to_first_key`, `test_null_stop_resolves_to_last_key`, `test_both_null_is_full_table`, and 3 more |
| test_layered_fast_truncate08.py | Follower range truncate writes layered tombstone sentinel to ingest file | `test_follower_truncate_writes_tombstone_to_ingest` |
| test_layered_modify01.py | Modify remains valid across checkpoint update | `test_layered_modify01` |

---

## Missing Numbers (Gaps in Sequence)

Files 06, 10, 11, 12, 13, 42, 95, 96 do not exist — numbers were likely reused or deleted.

---

## Coverage Gap

**None of the `test_layered*.py` files are included in the code coverage test list** (`code_coverage_config.json`). The only disagg hook coverage is a single entry: `python3 ../test/suite/run.py --hook disagg --skip-tests-in-file ../test/suite/hook_disagg.fail base` — which runs `base01` only through the hook, not the dedicated test_layered tests.
