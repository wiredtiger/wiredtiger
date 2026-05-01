# test_layered91 — Exhaustive layered cursor state transition coverage

**File:** `test/suite/test_layered91.py`
**Storage mode:** Disagg/Layered
**Components under test:** Layered cursor iteration and point reads across all possible per-key ingest/stable state combinations

## Test Cases

### `test_layered91.test_layered91`
- **What it tests:** Generates all sequences of length 0–5 from the 5-letter state alphabet {I, S, B, R, X} where each letter appears at most twice, yielding a large number of unique key-state sequences. For each sequence, creates one `table:` URI (named by the sequence letters) with `block_manager=disagg, type=layered`. Populates stable keys (S, B, R, X state letters) on the leader, checkpoints at stable=110, follower advances. Removes X-state keys on both leader and follower (ts=120), checkpoints again. On the follower: inserts I and B keys, tombstones R keys (ts=140). Follower advances checkpoint. Then for each table, calls `_verify_cursor()` which:
  - **Forward scan** (`cursor.next()`): collects keys; asserts they equal exactly the I/S/B keys (in string sort order, verifying key==value).
  - **Backward scan** (`cursor.prev()`): asserts reversed I/S/B keys, checks `WT_NOTFOUND` at end.
  - **Point reads** (`cursor.search()`): for each key position, asserts 0 for I/S/B keys and `WT_NOTFOUND` for R/X keys.
- **Components:** `src/cursor/cur_layered.c`, ingest/stable merge, tombstone handling, all 5 per-key states and all pairwise state transitions
- **Notes:** State definitions: I = ingest only (no stable entry), S = stable only (no ingest), B = both (ingest and stable, ingest takes precedence), R = stable has key but ingest has tombstone, X = no stable entry but ingest has tombstone. Only I/S/B keys are visible; R/X keys are hidden. The FIXME-WT-17160 comment notes that max_len=6 causes cache stuck; currently capped at 5. All tables are created in a single pass to amortize the heavyweight leader/follower setup cost.
