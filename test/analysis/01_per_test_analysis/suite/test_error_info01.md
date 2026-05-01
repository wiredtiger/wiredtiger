# test_error_info01 — Session get_last_error() tracks per-session error state

**File:** `test/suite/test_error_info01.py`
**Storage mode:** General
**Components under test:** session API (get_last_error), background compaction, transaction, schema (drop)

## Test Cases

### `test_error_info01.test_success`
- **What it tests:** Performs a successful create + insert + search sequence and asserts `get_last_error()` returns `(0, WT_NONE, "last API call was successful")`.
- **Components:** `src/session/`
- **Notes:** Uses `error_info_util.assert_error_equal`.

### `test_error_info01.test_einval_wt_background_compaction_already_running`
- **What it tests:** Enables background compaction, then tries to reconfigure it while it is running. Asserts `get_last_error()` returns `(EINVAL, WT_BACKGROUND_COMPACT_ALREADY_RUNNING, ...)`.
- **Components:** `src/compact/`, `src/session/`

### `test_error_info01.test_ebusy_wt_uncommitted_data`
- **What it tests:** Begins a transaction, updates a key (leaving transaction open), then attempts to drop the table. Asserts `get_last_error()` returns `(EBUSY, WT_UNCOMMITTED_DATA, ...)`. Then rolls back and drops cleanly.
- **Components:** `src/schema/`, `src/session/`

### `test_error_info01.test_ebusy_wt_dirty_data`
- **What it tests:** Commits a transaction on a table (without checkpointing), then tries to drop it. Asserts `get_last_error()` returns `(EBUSY, WT_DIRTY_DATA, ...)`. Includes a 1-second sleep to let the oldest ID advance. Then checkpoints and drops successfully.
- **Components:** `src/schema/`, `src/session/`

### `test_error_info01.test_api_call_alternating`
- **What it tests:** Calls the four basic scenarios above in alternating order (success → einval → ebusy-uncommitted → ebusy-dirty → success → ...) and verifies `get_last_error()` always reflects the most recent API call outcome.
- **Components:** `src/session/`
- **Notes:** Exercises that stored error state is fully overwritten on each call.

### `test_error_info01.test_api_call_doubling`
- **What it tests:** Calls each scenario twice in succession (success, success, einval, einval, ebusy-uncommitted, ebusy-uncommitted, ebusy-dirty, ebusy-dirty) and confirms that repeated calls with the same result still produce correct error reporting.
- **Components:** `src/session/`
