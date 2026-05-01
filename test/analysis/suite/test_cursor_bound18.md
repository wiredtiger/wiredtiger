# test_cursor_bound18 — Column group cursor: bounds restoration after invalid bound set

**File:** `test/suite/test_cursor_bound18.py`
**Storage mode:** General
**Components under test:** cursor bound API, column group cursor, bound validation and atomicity

## Test Cases

### `test_cursor_bound18.test_bound_api`
- **What it tests:** With column group tables: (1) sets a valid lower bound, then attempts to set an invalid upper bound (lower > upper) — verifies the original lower bound is preserved and the invalid upper bound is rejected; (2) sets a valid upper bound, then attempts to set an invalid lower bound (lower > upper) — verifies original upper bound is preserved. Ensures that a failed bound set is atomic (old bound not corrupted).
- **Components:** `src/cursor/cur_bound.c`
- **Notes:** `use_colgroup=True` only. Scenarios: 7 key formats × 2 value formats × 4 inclusive combos × prev/next. Tests that bound-setting is transactional: if the new bound would violate ordering constraints, the previous bound is unchanged.
