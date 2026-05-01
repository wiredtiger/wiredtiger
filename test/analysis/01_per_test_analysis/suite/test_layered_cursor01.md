# test_layered_cursor01 — General cursor operation correctness on layered tables with Oplog-driven workloads

**File:** `test/suite/test_layered_cursor01.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** Layered cursor forward/backward full scan, positioned iteration via search/search_near/next/prev, leader and follower consistency, insert/update/remove workloads

## Test Cases

All test methods share a common structure: the `Oplog` helper generates a deterministic sequence of inserts, optional updates, and optional removes applied identically to both leader and follower sessions. After each batch and after each checkpoint advance, `check_cursor_ops()` validates:

1. **Full forward scan** (`cursor.next()` from start to `WT_NOTFOUND`) across the sorted expected key set
2. **Full backward scan** (`cursor.prev()` from end to `WT_NOTFOUND`)
3. **Positioned forward iteration**: cursor is positioned at 0%, 25%, 50%, 75%, and 100% positions (by the parametrized `pos_func`), then iterated forward to end, verifying every subsequent key/value
4. **Positioned backward iteration**: same positions, iterated backward to start

Parametrized by positioning method (search, search_near, next, prev) crossed with disagg storage variants. Keys are sorted lexicographically (string sort: "1", "10", "11", "2", ...).

### `test_layered_cursor01.test_empty_tables`
- **What it tests:** Creates the table on both leader and follower, runs `check_cursor_ops()` on an empty table (no keys), then checkpoints and advances the follower and runs `check_cursor_ops()` again. Verifies that full and positioned scans on empty tables return `WT_NOTFOUND` immediately and that no errors occur on empty checkpoints.
- **Components:** `src/cursor/cur_layered.c`, empty table scan and checkpoint advance

### `test_layered_cursor01.test_populated_tables`
- **What it tests:** Two-batch workload: first batch of 100 inserts applied and verified before checkpoint; checkpoint advanced; second batch of 100 inserts applied and verified; checkpoint advanced. After each step (post-apply and post-advance) verifies all scan and positioned-iteration invariants on both leader and follower sessions.
- **Components:** Layered cursor full scan, positioned iteration, leader/follower consistency after two sequential checkpoint rounds

### `test_layered_cursor01.test_populated_tables_with_updates_20_percent`
- **What it tests:** Inserts 100 keys, then updates 20 keys (starting at offset 0). Runs `test_populated_tables()`. Verifies that updated values are reflected correctly in both forward/backward scans and positioned iterations on leader and follower.
- **Components:** Update workload on layered table, cursor iteration correctness

### `test_layered_cursor01.test_populated_tables_with_updates_50_percent`
- **What it tests:** Same as above with 50 updates (50% of 100 inserts). Verifies cursor operations with half the table updated.
- **Components:** Mid-range update density on layered cursor

### `test_layered_cursor01.test_populated_tables_with_updates_70_percent`
- **What it tests:** Same as above with 70 updates (70% of 100 inserts).
- **Components:** High-density update workload on layered cursor

### `test_layered_cursor01.test_populated_tables_with_removes_20_percent`
- **What it tests:** Inserts 100 keys, then removes 20 (20% of inserts, offset 0). Removed keys must not appear in any scan or positioned iteration. Verifies `WT_NOTFOUND` behavior is consistent after removes.
- **Components:** Remove workload, tombstone handling in layered cursor

### `test_layered_cursor01.test_populated_tables_with_removes_50_percent`
- **What it tests:** Same with 50 removals.
- **Components:** 50% removal density; verifies scan skips all removed keys

### `test_layered_cursor01.test_populated_tables_with_removes_70_percent`
- **What it tests:** Same with 70 removals.
- **Components:** High removal density; only 30 keys visible

### `test_layered_cursor01.test_populated_tables_with_removes_20_updates_50_percent`
- **What it tests:** 50 updates and 20 removes on a 100-key base. Verifies that both update and remove operations are correctly reflected and that forward/backward scans as well as positioned iterations return exactly the expected surviving key/value pairs.
- **Components:** Mixed update + remove workload on layered cursor

### `test_layered_cursor01.test_populated_tables_with_updates_20_percent` (second definition — offset variant)
- **What it tests:** 20 updates with a non-zero `updates_offset` (20% of 100). The offset shifts which keys are updated to start from a different position in the oplog. Verifies correct positioning behavior when updated keys are not at the beginning of the keyspace.
- **Components:** Update-with-offset workload; positioned scan correctness at boundaries

### `test_layered_cursor01.test_populated_tables_with_removes_20_percent_offset`
- **What it tests:** 20 removes with `remove_offset` set to 20% of inserts. Removes a range of keys starting at an offset rather than from key 0. Verifies scans and positioned iterations skip the correct key range.
- **Components:** Offset-remove workload on layered cursor

### `test_layered_cursor01.test_populated_tables_with_removes_20_updates_20_percent_offset`
- **What it tests:** 20 updates and 20 removes, both with 20% offset. The most complex mixed-offset scenario: updates shift from offset, removes shift from a different (same-valued here) offset. Verifies full cursor correctness across a combined offset update + remove workload.
- **Components:** Combined offset update + remove workload; forward/backward scan + positioned iteration on both leader and follower
