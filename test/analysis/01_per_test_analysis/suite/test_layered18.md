# test_layered18 — Long delta chains: repeated single-key updates across many checkpoints

**File:** `test/suite/test_layered18.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** page delta chains, checkpoint, page log (palite), follower data correctness

## Test Cases

### `test_layered18.test_layered18`
- **What it tests:** Creates a table, inserts 500 records at timestamp 100, checkpoints. Then applies `num_updates=10` sequential single-key updates (key 0) across 10 separate checkpoints (timestamps 101-110, stable timestamp advancing with each). Creates a follower and advances it to the final checkpoint. Verifies that the follower sees the latest value for key 0 (the 10th update) and the original value for all other keys.
- **Components:** page delta chain assembly in the page log (palite), checkpoint per-update, follower reconstruction of a delta chain (the GET call on the follower validates the chain), conn_layered.c
- **Notes:** Parametrized across 2 table types (layered: and table:+disagg) and disagg_storage. The test comment explicitly states that "the call to GET will validate the delta chain in page log extension." The delta chain here has depth 10 (10 consecutive deltas on the same page for key 0). Would break if the page log extension fails to assemble or return the correct head of a long delta chain, or if checkpoint metadata does not correctly track the latest page for a given key after many delta writes.
