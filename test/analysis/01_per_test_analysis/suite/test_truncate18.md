# test_truncate18 — Verify correctness when globally visible fast-truncated pages leave empty leftmost leaf

**File:** `test/suite/test_truncate18.py`
**Storage mode:** General
**Components under test:** fast delete, verify, empty page optimization, internal page reconciliation, leftmost leaf handling

## Test Cases

### `test_truncate18.test_truncate18`
- **What it tests:** Creates a table with small internal pages (4096 bytes); writes 10,000 rows at ts=10; reopens; fast-truncates 87.5% of the tree starting from either the beginning or end at ts=20; makes all stable and reopens; ages out the baseline data (oldest=30); writes a scratch value at ts=35 to key 1 then deletes it at ts=40 to force reconciliation of the first leaf and internal page to empty; advances both stable and oldest to 40 and checkpoints; reopens; calls `verifyUntilSuccess` on the table. This regression test ensures verify does not assert when an empty-page optimization is applied to fast-deleted leftmost pages.
- **Components:** `btree.c`, `verify.c`, `evict.c`, `checkpoint.c`
- **Notes:** Parameterized over column/row × front/back truncation range. Skipped on disagg if fast truncate not built. Targets the scenario where the optimization that replaces obsolete deleted pages with physically empty pages causes verify's key-order check to fail for the leftmost leaf page.
