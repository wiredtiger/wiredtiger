# test_layered20 — 32 consecutive delta checkpoints: read correctness with and without timestamps

**File:** `test/suite/test_layered20.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** deep delta chains (32 levels), page delta reconciliation, block_disagg, checkpoint, follower historical reads, encryption, compression

## Test Cases

### `test_layered20.test_layered_read_write`
- **What it tests:** Creates a table (layered: or file:+disagg), inserts 10 records at an initial timestamp (or without timestamps). Then loops 32 times: each iteration updates every 10th record (keys 0 and possibly others given nitems=10) and checkpoints, producing up to 32 consecutive delta pages. Reopens as follower. 
  - If `ts=True`: reads at the initial timestamp and verifies original values; then for each of the 32 checkpoints reads at that checkpoint's timestamp and verifies the updated keys have the value written at that specific checkpoint.
  - If `ts=False`: reads without timestamps and verifies the latest value for updated keys.
- **Components:** 32-deep delta chain in page log (palite), `delta_pct=100` (aggressive delta forcing), block_disagg, checkpoint loop, follower historical reads at each timestamp level
- **Notes:** Parametrized across 2 encryption x 2 compression x disagg_storage x 2 URI types x 2 timestamp modes = many scenarios. `delta_pct=100` forces delta writing almost always. The 32-iteration loop is the "lots of deltas" stress test — it ensures the page log extension can correctly resolve any depth of delta chain. For timestamped mode, the follower must reconstruct the exact value at each checkpoint level via the delta chain, which requires that each delta is tagged with its checkpoint's timestamp. Would break if deep delta chains are truncated, reordered, or if timestamp metadata is not stored in each delta.
