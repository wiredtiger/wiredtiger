# test_layered56 — No page delta is written when a page split occurs

**File:** `test/suite/test_layered56.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, reconciliation (page split vs. delta decision), page log, checkpoint

## Test Cases

### `test_layered56.test_page_split_delta`
- **What it tests:** Verifies that reconciliation does not write a leaf page delta when a page split is required. Two scenarios: (1) `page_split=True` — after inserting 35 records that fill a 4 KB page, makes an update plus appends 10 more records to overflow the page. At checkpoint, no delta is written (`rec_page_delta_leaf == 0`); after reopen, confirms 2 leaf pages exist. (2) `page_split=False` — makes a single small update that does not cause a split; checkpoint writes exactly 1 delta (`rec_page_delta_leaf == 1`); after reopen, still 1 leaf page.
- **Components:** reconciliation (delta vs. full-page decision on split), block_disagg, page log, checkpoint, `btree_row_leaf` stat
- **Notes:** Uses `layered:` URI with `block_manager=disagg`, 4 KB page sizes, `split_pct=75`, `delta_pct=100`, both leaf and internal deltas enabled. The test comment explicitly warns that it depends on reconciliation producing specific page sizes; failures might indicate a page-size change. Parametrized over `page_split` boolean. Uses `reopen_conn()` (not `reopen_disagg_conn`) to check post-split state. Disagg-only.
