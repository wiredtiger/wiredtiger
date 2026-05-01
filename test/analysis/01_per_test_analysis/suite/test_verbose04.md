# test_verbose04 — Verbose configuration: `all` category enables every verbose category

**File:** `test/suite/test_verbose04.py`
**Storage mode:** General (skipped for tiered)
**Components under test:** `verbose=[all]`, `wiredtiger_get_verbose_categories()`, verbose category enumeration

## Test Cases

### `test_verbose04.test_verbose_categories`
- **What it tests:** Calls `wiredtiger.wiredtiger_get_verbose_categories()` and verifies: (1) the count equals `WT_VERB_NUM_CATEGORIES - 1` (excluding `WT_VERB_DEFAULT`); (2) the returned names exactly match all `WT_VERB_*` attributes in the `wiredtiger` module (excluding `WT_VERB_DEFAULT` and `WT_VERB_NUM_CATEGORIES`).
- **Components:** `verbose.c`
- **Notes:** No parameterization. Validates the Python API for category enumeration.

### `test_verbose04.test_verbose_all`
- **What it tests:** Opens with `verbose=[all:1]`; performs compaction; asserts all messages match any known verbose category; opens with `verbose=[all:0]`; performs API ops; asserts messages still appear but from INFO level only; tests all 6 levels (`all:0` through `all:5`) each with a compaction to confirm output is produced at every level.
- **Components:** `verbose.c`, all subsystems
- **Notes:** Parameterized over flat/JSON. Skipped for tiered. Confirms that `all` enables every category simultaneously.

### `test_verbose04.test_verbose_multiple`
- **What it tests:** Tests `verbose=[api:0,all:1,version:0]` and `verbose=[version:0,all,api:0]`; for each, the expected matching set is all verbose categories *except* `WT_VERB_API` and `WT_VERB_VERSION` (since those are set at level 0 / INFO which produces no messages for the simple operations performed). Asserts all messages match the reduced category set.
- **Components:** `verbose.c`
- **Notes:** Parameterized over flat/JSON. Tests that per-category level overrides within `all` work correctly.
