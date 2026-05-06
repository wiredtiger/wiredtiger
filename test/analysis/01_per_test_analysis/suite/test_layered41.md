# test_layered41 — Duplicate key error on layered table insert with overwrite=false

**File:** `test/suite/test_layered41.py`
**Storage mode:** Disagg/Layered
**Components under test:** cur_layered.c, ingest btree (duplicate key detection)

## Test Cases

### `test_layered41.test_dup_key`
- **What it tests:** Verifies that inserting a key that already exists into a layered table opened with `overwrite=false` raises `WT_DUPLICATE_KEY`, and that after the error the cursor's value is the existing value (not the attempted new value). Inserts 100 records (keys 0–99), then attempts to insert key "10" with value "20" (existing value "10").
- **Components:** cur_layered.c (overwrite=false / duplicate detection path), ingest btree
- **Notes:** Parametrized over two roles: `leader` and `follower`. The test verifies that duplicate detection works in both roles. Uses `assertRaisesHavingMessage` with the pattern `/WT_DUPLICATE_KEY/`. After the failed insert, `cursor.get_value()` must return the original value ("10"), confirming the cursor position is correctly preserved on error. Disagg-only.
