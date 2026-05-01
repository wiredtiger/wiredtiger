# test_compact15 — Foreground compaction requires a URI

**File:** `test/suite/test_compact15.py`
**Storage mode:** General (skips tiered)
**Components under test:** compaction API, URI validation

## Test Cases

### `test_compact15.test_compact15`
- **What it tests:** Verifies that foreground compaction (`session.compact(None, None)`) raises `WiredTigerError` with the message "Compaction requires a URI" when called without a URI, and succeeds when a valid URI is provided.
- **Components:** `src/session/session_compact.c`
- **Notes:** Skip: tiered. Two scenarios: `valid_uri=True` (calls `session.compact(uri)` — succeeds) and `valid_uri=False` (calls `session.compact(None)` — expects `WT_ERROR` with specific message). Tests the basic URI validation guard for foreground compaction.
