# test_cursor_bound01 — Cursor bound API basic validation (EINVAL, clear, incompatibilities)

**File:** `test/suite/test_cursor_bound01.py`
**Storage mode:** General + Disagg (layered URI scenario)
**Components under test:** cursor bound API, layered/disaggregated cursor, index cursor, column group cursor

## Test Cases

### `test_cursor_bound01.test_bound_api`
- **What it tests:** Exercises basic cursor bound API validation: calling `cursor.bound()` without a config string returns `EINVAL`; setting lower and upper bounds; clearing bounds via `action=clear`; verifying that `largest_key()` is incompatible with bounds (`EINVAL`); verifying that `next_random=true` cursor is incompatible with bounds; config string edge cases (action without bound specified, invalid action substring).
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_layered.c` (for layered scenario)
- **Notes:** Skipped for tiered hook. Scenarios: file/table/colgroup/index/layered × string/var (key_format=S or r) × disagg_storages. The `layered` scenario uses `DisaggConfigMixin` and a layered: URI. Tests both lower-only, upper-only, and both bounds.
