# test_cell_prepared_time_window — Cell packing/unpacking of prepared transaction time windows

**File:** `test/catch2/misc_tests/test_cell_prepared_time_window.cpp`
**Storage mode:** General
**Components under test:** `__cell_pack_value_validity`, `__wt_cell_unpack_kv`, `WT_TIME_WINDOW`, prepared transaction time window fields
**Test type:** Unit

## TEST_CASE: "Cell prepared time window: empty window" [cell_prepared_tw]
- **What it tests:** A `WT_TIME_WINDOW` with all zero/default fields packs to minimal cell representation and unpacks without error.
- **Components:** `__cell_pack_value_validity`, `__wt_cell_unpack_kv`
- **Notes:** Requires `WT_CONN_PRESERVE_PREPARED` flag on the connection and a configured btree.

## TEST_CASE: "Cell prepared time window: start-prepared only" [cell_prepared_tw]
- **What it tests:** A time window with only `start_prepare_ts` and `start_prepared_id` set packs and unpacks with those fields preserved.
- **Components:** `__cell_pack_value_validity`, `__wt_cell_unpack_kv`, `start_prepare_ts`, `start_prepared_id`
- **Notes:** Exercises the start-prepare encoding path.

## TEST_CASE: "Cell prepared time window: stop-prepared only" [cell_prepared_tw]
- **What it tests:** A time window with only `stop_prepare_ts` and `stop_prepared_id` set packs and unpacks correctly.
- **Components:** `__cell_pack_value_validity`, `__wt_cell_unpack_kv`, `stop_prepare_ts`, `stop_prepared_id`
- **Notes:** Exercises the stop-prepare encoding path.

## TEST_CASE: "Cell prepared time window: both prepared (same txn)" [cell_prepared_tw]
- **What it tests:** A time window where both start and stop prepared fields belong to the same transaction ID packs and unpacks with all four prepare fields correctly.
- **Components:** `__cell_pack_value_validity`, `__wt_cell_unpack_kv`
- **Notes:** The same-transaction optimization may affect encoding.

## TEST_CASE: "Cell prepared time window: regular (non-prepared)" [cell_prepared_tw]
- **What it tests:** A time window with ordinary (non-prepared) timestamps and transaction IDs packs and unpacks without the prepare fields being set.
- **Components:** `__cell_pack_value_validity`, `__wt_cell_unpack_kv`
- **Notes:** Regression test ensuring non-prepared cells are unaffected by the prepare encoding logic.
