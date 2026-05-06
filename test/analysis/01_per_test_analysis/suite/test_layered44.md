# test_layered44 — No freed pages are read by a follower

**File:** `test/suite/test_layered44.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, stable btree, page log, follower reads, block free tracking, verbose logging

## Test Cases

### `test_layered44.test_layered44`
- **What it tests:** Verifies correctness of page lifecycle management: no page that has been freed on the leader is ever read by a follower. Part 1 creates a layered table, inserts 10,000 records, and performs three checkpoints with progressively smaller update sets. It parses verbose `WT_VERB_BLOCK` output to collect the set of freed page IDs, asserts no page is freed twice, and verifies `disagg_block_page_discard > 0`. Part 2 opens a follower, advances the checkpoint, scans all 10,000 records, and parses `WT_VERB_READ` output to confirm none of the read page IDs appear in the freed set.
- **Components:** block_disagg (`disagg_block_page_discard` stat), stable btree (page free tracking via `WT_VERB_BLOCK`), page log, follower reads (`WT_VERB_READ`), checkpoint, `verifyUntilSuccess`
- **Notes:** Uses verbose block and read logging to inspect page IDs at the wire level. Scans stdout.txt to parse freed/read page IDs from log lines containing "block free" and "WT_VERB_READ". No parametrization (single disagg storage, no scenario list). Calls `verifyUntilSuccess()` at the end as an additional data integrity check. Disagg-only.
