# model_basic — Basic KV Model Correctness

## Overview

`model_basic` verifies the foundational behaviour of the `model::kv_database` and
`model::kv_table` abstractions. It checks that the model accurately implements WiredTiger's
timestamped visibility rules, non-timestamped (globally-visible) updates, truncation semantics,
and oldest-timestamp enforcement — then cross-checks each scenario against a live WiredTiger
instance and/or by replaying the WiredTiger debug log through the model verifier.

The test also contains a self-contained unit test for `model::data_value`, the model's type-safe
key/value wrapper.

Source: `/data/work/git/wiredtiger4/test/model/test/model_basic/main.cpp`

---

## Test Cases / Scenarios

### test_data_value
- **What it verifies:** The `model::data_value` class correctly implements comparison operators
  (`<`, `>`, `<=`, `>=`, `==`, `!=`), string streaming, the sentinel `model::NONE` value, and
  the three supported WiredTiger wire types: string (`S`), signed 64-bit integer (`q`), and
  unsigned 64-bit integer (`Q`). Also verifies that `q` and `Q` are distinct types even for the
  same numeric value.
- **Model components:** `model::data_value`.
- **Notes:** Pure in-process unit test; no WiredTiger connection opened.

### test_model_basic
- **What it verifies:** Core timestamped visibility rules in model-only mode:
  - Insert/remove/insert sequences at specific timestamps and reads at timestamps before, at, and
    after each write.
  - Globally-visible (non-timestamped) inserts and removes.
  - Missing-key reads return `model::NONE` and `remove` on a missing key returns `WT_NOTFOUND`.
  - Multiple inserts at the same timestamp: the last one wins when read at that timestamp.
  - `contains_any` — checks whether any version of a key visible at a given timestamp matches a
    value, including historical versions when multiple writes share the same timestamp.
  - `insert` without overwrite (`WT_DUPLICATE_KEY` when key exists; succeeds after a remove).
  - `update` without overwrite (`WT_NOTFOUND` when key is absent).
- **Model components:** `kv_database`, `kv_table` (timestamped and non-timestamped insert/remove/
  update/get/contains_any).
- **Notes:** Model-only; no WiredTiger connection.

### test_model_basic_wt
- **What it verifies:** The same sequence as `test_model_basic` executed in parallel on both the
  model and a real WiredTiger row-store table, asserting that every read returns the same result
  in both (`wt_model_assert`). Includes a deliberate model/WT divergence injection
  (`table->remove(key2, 1000)`) to confirm that `verify_noexcept` catches the mismatch. Also
  re-verifies by replaying the WiredTiger debug log into the model (`verify_using_debug_log`).
- **Model components:** `kv_database`, `kv_table`, `debug_log_parser`, model verifier.
- **Notes:** Row-store, `key_format=S,value_format=S`, logging disabled.

### test_model_basic_column_wt
- **What it verifies:** Same as `test_model_basic_wt` but for a column-store table
  (`key_format=r`). Verifies that the model correctly handles record-number keys and the same
  timestamped/non-timestamped visibility rules apply. Also checks deliberate divergence detection
  and debug-log replay.
- **Model components:** `kv_database`, `kv_table` (column type), debug log verifier.
- **Notes:** Column-store, `key_format=r,value_format=S`, logging disabled.

### test_model_basic_logged
- **What it verifies:** For tables with `log_enabled=true`, timestamps are ignored at read time
  (the latest committed value is always returned regardless of read timestamp). Exercises the same
  insert/remove/update/contains_any APIs as `test_model_basic` but under logging semantics.
- **Model components:** `kv_table` with `log_enabled=true`.
- **Notes:** Model-only; no WiredTiger connection.

### test_model_basic_logged_wt
- **What it verifies:** Same as `test_model_basic_logged` cross-checked against WiredTiger
  (`log=(enabled=true)` table). Debug-log replay also verified.
- **Model components:** `kv_database`, `kv_table` (logged), `debug_log_parser`.
- **Notes:** Row-store, `key_format=S,value_format=S`, logging enabled.

### test_model_truncate (non-logged and logged variants)
- **What it verifies:** Range truncation semantics in the model:
  - Truncate a middle range [key2, key4]; keys outside the range are preserved.
  - One-sided truncates: [start, key2] and [key4, end] using `model::NONE` as the open bound.
  - Full-table truncate (both bounds `model::NONE`).
  - Truncation with start/stop keys that do not exist in the table (keys adjacent to, but not
    in, the table).
  - For logged tables, same tests confirm that truncation works without timestamp constraints.
- **Model components:** `kv_table::truncate`, timestamped and non-timestamped variants.
- **Notes:** Model-only; called twice, once with `logging=false` and once with `logging=true`.

### test_model_truncate_wt (non-logged and logged variants)
- **What it verifies:** Same truncation scenarios cross-checked against WiredTiger. Also injects a
  deliberate divergence (`table->insert(key2, value1, 1000)`) to confirm verify catches it. Debug-
  log replay is verified.
- **Model components:** `kv_database`, `kv_table::truncate`, model verifier, `debug_log_parser`.
- **Notes:** Row-store, both `log=(enabled=false)` and `log=(enabled=true)` runs.

### test_model_truncate_column_wt (non-logged and logged variants)
- **What it verifies:** Same truncation scenarios as `test_model_truncate_wt` but for a column-
  store table (`key_format=r`). Verifies record-number range truncation and boundary conditions.
- **Model components:** `kv_database`, `kv_table` (column, truncate), `debug_log_parser`.
- **Notes:** Column-store, both logging variants.

### test_model_oldest
- **What it verifies:** Oldest-timestamp enforcement in the model:
  - After `set_oldest_timestamp(30)`, reads at timestamps < 30 return `EINVAL`.
  - Moving oldest forward is legal; moving it backward returns `EINVAL`.
  - Setting stable timestamp below oldest returns `EINVAL`.
  - `database.restart()` without a stable timestamp resets oldest to 0; with a stable timestamp,
    oldest is preserved.
  - Oldest cannot be set past stable.
- **Model components:** `kv_database::set_oldest_timestamp`, `kv_database::restart`.
- **Notes:** Model-only.

### test_model_oldest_wt
- **What it verifies:** Same oldest-timestamp rules cross-checked against WiredTiger. Verifies
  that `database.oldest_timestamp() == wt_get_oldest_timestamp(conn)` after every operation.
  Database is restarted (close/reopen) to validate persistence of oldest and stable timestamps.
  Debug-log replay verified.
- **Model components:** `kv_database`, `debug_log_parser`, model verifier.
- **Notes:** Row-store, timestamps enabled.

### test_model_debug_log_verify_wt
- **What it verifies:** The debug-log parser correctly reconstructs model state from a WiredTiger
  debug log. Uses uint64 key/value pairs (WiredTiger type `Q`) and a non-trivial key sequence
  generated by `i = (i * 7) + 1` to exercise the variable-length integer unpacking in the log
  parser. A checkpoint is taken before closing so the log contains checkpoint records.
- **Model components:** `debug_log_parser::from_debug_log`, model verifier.
- **Notes:** Exercises the `key_format=Q,value_format=Q` parsing path specifically.
