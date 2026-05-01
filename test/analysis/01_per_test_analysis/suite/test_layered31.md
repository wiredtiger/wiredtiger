# test_layered31 — Follower cursor stability across checkpoint pick-ups and role transitions

**File:** `test/suite/test_layered31.py`
**Storage mode:** Disagg/Layered
**Components under test:** cur_layered.c, conn_layered_ingest.c, checkpoint, page log, follower checkpoint pick-up, leader/follower role transitions

## Test Cases

### `test_layered31.test_layered31`
- **What it tests:** Verifies that a follower node correctly picks up new checkpoints and that open cursors on layered tables behave correctly across checkpoint advances and role transitions. The test covers seven sequential scenarios: (1) basic follower data verification, (2) cursors left open during checkpoint pick-up, (3) cursor close-and-reopen after pick-up, (4) cursor reset after pick-up, (5) cursor position preserved mid-scan when a new checkpoint arrives, (6) cursor position preserved mid-scan when the follower steps up to leader, (7) error handling for invalid checkpoint metadata.
- **Components:** cur_layered.c (cursor reset/position semantics), conn_layered_ingest.c, checkpoint propagation (`disagg_advance_checkpoint`), page log (`pl_get_complete_checkpoint_ext`), role reconfiguration (`role="leader"/"follower"`), stable btree, ingest btree
- **Notes:** Uses two layered URIs (`layered:test_layered31a`, `layered:test_layered31b`), 500 items. Key insight: cursor scan position on a layered cursor must be isolated from concurrent checkpoint advances and role changes. Part 6 (step-down while cursor is open) is disabled pending FIXME-WT-14545. Part 7 verifies that `reconfigure(checkpoint_meta="test")` raises `WT_NOTFOUND` and logs the error. Disagg-only scenarios (no non-disagg variant).
