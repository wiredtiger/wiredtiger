# test_timestamp18 — Mixing timestamped and non-timestamped writes at scale

**File:** `test/suite/test_timestamp18.py`
**Storage mode:** General
**Components under test:** non-timestamped inserts/deletes over 9999 keys, history store, checkpoint

## Test Cases

### `test_timestamp18.test_ts_writes_with_non_ts_write`
- **What it tests:** For 9999 keys writes value1 at ts=2, value2 at ts=3, value3 at ts=4; then for every even key performs a non-timestamped operation (either delete or insert of value4); checkpoints; reads at timestamps 2 and 3: even keys show the no-timestamp result (WT_NOTFOUND if delete, value4 if insert), odd keys show timestamped value1 or value2 as expected.
- **Components:** `txn.c`, `txn_timestamp.c`, `history_store.c`, `checkpoint.c`
- **Notes:** Parameterized over string-row/column × delete/insert non-ts write. Tagged as `verify:prepare`. Large-scale test (9999 keys × 500-byte values) exercising history store and checkpoint interactions.
