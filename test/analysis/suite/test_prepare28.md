# test_prepare28 — Race between ignore_prepare read and prepare commit resolution

**File:** `test/suite/test_prepare28.py`
**Storage mode:** General (skipped for tiered)
**Components under test:** prepared transactions, ignore_prepare, thread safety, timing stress, statistics

## Test Cases

### `test_prepare28.test_ignore_prepare`
- **What it tests:** Uses the `prepare_resolution_2` timing stress failpoint to expose a race between a reader using `ignore_prepare=true` and the commit resolution of a prepared transaction; verifies that the stat `txn_read_race_prepare_commit` is greater than 0 (indicating the race condition was triggered and handled correctly)
- **Components:** `txn/txn_prepare.c`, `txn/txn.c`, `cursor/cur_std.c`
- **Notes:** Threaded test: the main thread commits a prepared transaction while a reader thread with `ignore_prepare=true` reads the same key mid-resolution; `prepare_resolution_2` timing stress pauses the commit at a critical point to make the race reproducible; verifies no corruption or incorrect values are returned during the race; skipped for tiered storage hook
