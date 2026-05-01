# wt4333_handle_locks — Data handle lock contention under concurrent checkpoint

**Path:** `test/csuite/wt4333_handle_locks/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-4333
**Components under test:** Data handle locking, cursor caching, `session->verify`, checkpoint handle sweeps, concurrent reader/writer/verifier threads

## What This Test Does
This test stresses WiredTiger's data handle locking by running many concurrent worker threads (readers and writers) and a verifier thread against up to 750 URI tables, while the file manager aggressively sweeps idle handles (`close_idle_time=1`, `close_scan_interval=1`). The test runs 5 randomly selected configurations from a 12-entry table covering 1–64 workers, 1–750 URIs, and cursor caching on/off. Each configuration runs for 60 seconds (SIGALRM). The verifier thread intermittently calls `session->verify` on random URIs and tracks EBUSY contention. The goal is to verify that no deadlocks or assertion failures occur under this high-concurrency handle-lock pressure.

## Test Scenarios / Cases

### Scenario: Single worker, single URI — cursor caching off/on
- **What it tests:** Basic read/write operations with and without the cursor cache under frequent handle sweeps and concurrent verify calls.
- **Components:** `session->open_cursor`, `cursor->search`, `cursor->insert`, `session->verify`, handle sweep, `cache_cursors`.

### Scenario: Multi-worker (8–64), single or multiple URIs — cursor caching off/on
- **What it tests:** Scaling behavior of handle lock acquisition under increasing thread counts (8, 16, 64) and URI counts (1, 100, 750), with both read-only checkpoint cursors and read-write live cursors.
- **Components:** `checkpoint=WiredTigerCheckpoint` cursor config (50% of reads), concurrent `session->verify`, handle sweeps, `EBUSY` retry logic.
- **Notes:** 75% readers, 25% writers. Cursors are randomly closed or cached (reset and held) across operations. MAXKEY=10,000 keys per URI. Sweep stats are printed if verbose mode is enabled.

## LazyFS Variant
None.
