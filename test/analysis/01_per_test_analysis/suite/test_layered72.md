# test_layered72 — Pinned history store dhandle on follower survives checkpoint advance

**File:** `test/suite/test_layered72.py`
**Storage mode:** Disagg/Layered
**Components under test:** History store dhandle pinning, follower checkpoint advance, timestamp reads across checkpoint boundaries

## Test Cases

### `test_layered72.test_layered72`
- **What it tests:** Leader writes key="1" (ts=2) and key="2" (ts=3), checkpoints at stable=3 oldest=1. Follower advances checkpoint and opens a transaction at read_timestamp=2, reads key="1" and gets "value1". Leader then updates key="2" to "value3" (ts=4) and checkpoints at stable=4 oldest=3 (making the old ts=2 version obsolete). Follower advances checkpoint again. The follower cursor (still inside the ts=2 transaction) re-reads key="1" and verifies it still returns "value1", proving the history store dhandle is pinned by the open transaction and the old value is accessible even after the leader made it obsolete.
- **Components:** `src/history/hs_cursor.c`, `src/conn/conn_dhandle.c` (history store dhandle pinning), follower checkpoint advance
- **Notes:** Tests that an open follower transaction prevents the history store dhandle from being evicted/replaced when a newer checkpoint arrives. The old value at ts=2 must remain readable even though the leader's oldest_timestamp has advanced past it. `layered:` URI, `precise_checkpoint=true`.
