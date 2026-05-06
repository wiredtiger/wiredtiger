# test_drop04 — Regression test for WT-15225: drop after create+checkpoint on empty logged table

**File:** `test/suite/test_drop04.py`
**Storage mode:** General (logging enabled)
**Components under test:** schema drop, logging, checkpoint cleanup, checkpoint cursor

## Test Cases

### `test_drop04.test_drop_after_bulk_load`
- **What it tests:** Regression for WT-15225. Runs 100 iterations of: create a logged table, wait for checkpoint cleanup to run (`wait_for_cc_to_run`), then drop the table. The table is created with only a schema — no data is inserted — making it an empty logged table. Verifies that repeated create-checkpoint-drop cycles on an empty logged table do not crash or error.
- **Components:** `src/schema/schema_drop.c`, `src/checkpoint/`, `src/log/`
- **Notes:** Extends `test_cc_base` which provides `wait_for_cc_to_run()`. Connection config has `log=(enabled=true)`. The bug was a use-after-free or similar corruption in the checkpoint cleanup path when dealing with empty logged tables.
