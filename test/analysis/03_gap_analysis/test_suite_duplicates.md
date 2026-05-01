# Duplicate and Overlap Analysis: Python Test Suite

---

## Methodology

Each test's analysis file (produced in a prior analysis pass) was read in full. For each test group, the set of scenarios, what each test exercises, and which components are under test were compared. Overlap was assessed by asking: "Does removing test X leave a scenario uncovered that no other test in the group exercises?" Near-duplicates were identified where tests share the same code path, the same precondition sequence, and the same assertion — differing only in surface parameters (e.g., key format) that are already parametrized within a single test. True duplicates were identified where two separate test files exercise identical scenarios with identical assertions.

---

## RTS Test Overlaps

### [NEAR-DUP] RTS01 / RTS06 — "stable before all commits; keys disappear"

- **Tests:** `test_rollback_to_stable01`, `test_rollback_to_stable06`
- **What they share:** Both write N rows at one or more timestamps past the stable timestamp, checkpoint, and verify that RTS removes those rows. Both parametrize over column/row_integer key format, in_memory, prepare, and worker threads.
- **What distinguishes them:**
  - RTS01 focuses on removes (rows written then deleted); the check is that rows come back (restored from HS) after RTS.
  - RTS06 focuses on inserts that never had a stable version; the check is that 0 rows remain after RTS, and then a re-insert doesn't collide.
  - These are genuinely different scenarios (tombstone-rollback vs. key-removal-rollback), but the difference is subtle and both check the same overall RTS "undo unstable writes" invariant.
- **Recommendation:** KEEP BOTH — the tombstone-restoration path (RTS01) and the key-removal path (RTS06) exercise different code in `txn_rollback_to_stable.c`. However, consider merging the two test methods into a single parametrized test file with a `remove_type` parameter.

---

### [NEAR-DUP] RTS02 / RTS17 — "RTS restores prior stable value from HS"

- **Tests:** `test_rollback_to_stable02`, `test_rollback_to_stable17`
- **What they share:** Both write multiple timestamped versions of the same rows, set stable below the latest, checkpoint, and verify the correct (stable-era) value is restored. Both parametrize over key format, in_memory, and worker threads.
- **What distinguishes them:**
  - RTS02 uses 10,000 rows and 4 timestamped value rounds; verifies via stat `upd_aborted + hs_removed >= nrows*2`.
  - RTS17 uses 200 rows and 4 rounds but also verifies the intermediate HS-restored value at ts=7/9 remains "bbbb" (the HS-restored value, not the original). RTS17 also focuses on the "data in both HS and data store simultaneously" case.
- **Recommendation:** KEEP BOTH — RTS17 tests an important sub-case (HS-plus-data-store split) but could be merged into RTS02 with an `hs_split` scenario variant. Medium priority.

---

### [NEAR-DUP] RTS03 / RTS16 — "RTS removes on-disk updates for row and column store"

- **Tests:** `test_rollback_to_stable03`, `test_rollback_to_stable16`
- **What they share:** Both write timestamped rows, checkpoint/evict, call RTS, and verify rows at timestamps before and after stable. Both exercise column and row formats.
- **What distinguishes them:**
  - RTS03 focuses specifically on verifying HS entries from *reconciled pages* are removed, and makes a second RTS call to check `rts_btrees_skipped`.
  - RTS16 uses distinct row ranges (batches written at different timestamps), verifies rows from each batch are absent or present.
- **Recommendation:** KEEP BOTH — the "second RTS call / skip-pages" scenario in RTS03 is unique. The "distinct row ranges" scenario in RTS16 provides different code coverage. These are complementary.

---

### [NEAR-DUP] RTS04 / RTS14 / RTS23 — "RTS uses full update (not delta) when restoring modify chains from HS"

- **Tests:** `test_rollback_to_stable04`, `test_rollback_to_stable14`, `test_rollback_to_stable23`
- **What they share:** All three write a base value followed by a chain of modifies (Q, R, S, T...), set stable in the middle of the chain, checkpoint, and verify that RTS restores the chain-reconstructed value correctly. All use crash recovery.
- **What distinguishes them:**
  - RTS04 has up to 11 modify rounds and parametrizes on `evict` (mid-test eviction); stats check includes `hs_sweep`.
  - RTS14 specifically focuses on concurrent background checkpoint (timing stress) during the write of later modifies; also has `test_rollback_to_stable_same_ts` and `_same_ts_append` sub-tests for same-timestamp modify chains.
  - RTS23 uses explicit `cursor.search()` calls (not just cursor scan) to verify each key individually, explicitly targeting the "cursor.search reconstructs from HS" code path.
- **Assessment:** These three tests cover genuinely different sub-paths:
  - RTS04: large modify chain volume + optional mid-test eviction
  - RTS14: concurrency with checkpoint during modify chain writes + same-timestamp modifies
  - RTS23: cursor.search() reconstruction (not just scan)
- **Recommendation:** KEEP ALL THREE — they are complementary despite surface similarity. No merge needed.

---

### [NEAR-DUP] RTS07 / RTS11 — "Two crash-restart cycles"

- **Tests:** `test_rollback_to_stable07`, `test_rollback_to_stable11`
- **What they share:** Both perform two successive crash-restart cycles with a checkpoint between each. Both verify that after each restart the expected version is visible.
- **What distinguishes them:**
  - RTS07 uses 1,000 rows, 4 timestamped value rounds per table, 2 tables, and writes post-stable data before the first crash. The key insight is that after the second restart, HS stats are both 0 (nothing to redo).
  - RTS11 uses a single row with 3 small timestamped rounds per cycle; the second cycle writes additional data then crashes. RTS11 explicitly verifies the exact `hs_removed=4` stat after the second restart.
- **Recommendation:** KEEP BOTH — the multi-row / multi-table scenario (RTS07) and the precise HS stat counting scenario (RTS11) are meaningfully different. However, consider combining the two-cycle pattern into a parametrized helper.

---

### [NEAR-DUP] RTS34 / RTS36 — "RTS undoes fast-truncate (fast-delete)"

- **Tests:** `test_rollback_to_stable34`, `test_rollback_to_stable36`
- **What they share:** Both verify that RTS correctly undoes a `session.truncate()` (fast-delete) on the upper half of a table when the truncation timestamp is above stable. Both check `rec_page_delete_fast > 0`.
- **What distinguishes them:**
  - RTS34 is more general: supports multiple key formats (column/row_integer/string_row), prepare, optional second checkpoint, crash or runtime RTS.
  - RTS36 specifically verifies that `cache_read_deleted > 0` after RTS (i.e., page instantiation happened), which RTS34 does not.
- **Recommendation:** KEEP BOTH — the `cache_read_deleted` assertion in RTS36 is unique and tests the page-instantiation-during-RTS path. However, RTS36's "crash or runtime" parametrization partly overlaps with RTS34.

---

### [NEAR-DUP] RTS15 / RTS16 — "RTS removes on-disk updates, in-memory path"

- **Tests:** `test_rollback_to_stable15`, `test_rollback_to_stable16`
- **What they share:** Both verify RTS removes beyond-stable updates from on-disk/in-memory tables. Both parametrize over in_memory and key format.
- **What distinguishes them:**
  - RTS15 uses VLCS integer format and `debug=(eviction=false)` to force all data into in-memory update lists; calls RTS twice.
  - RTS16 uses distinct row ranges (different timestamps applied to different key batches).
  - Source comment in RTS16 explicitly notes it may be somewhat redundant with others.
- **Recommendation:** MERGE — The RTS16 author noted redundancy. The distinct-batch approach is not a meaningfully different code path from RTS15's two-RTS call approach. RTS16's scenarios (column/row × in_memory × worker threads) are a subset of RTS01's and RTS15's combined coverage. **Candidate for removal or subsumption into RTS01.**

---

### [NEAR-DUP] RTS08 / RTS31 / RTS41 — "RTS no-op when stable >= all commits"

- **Tests:** `test_rollback_to_stable08`, `test_rollback_to_stable31`, `test_rollback_to_stable41`
- **What they share:** All three test scenarios where RTS should not abort anything. RTS08 verifies stats (0 aborted/removed), RTS31 tests when no stable timestamp has been set at all, RTS41 tests that `dryrun=true` applies only to a single call.
- **What distinguishes them:**
  - RTS08: stable >= latest commit; checks `pages_visited=0` for on-disk (key optimization check).
  - RTS31: no stable timestamp was ever set; also parametrizes on `crash` vs. runtime RTS.
  - RTS41: verifies `dryrun` is not sticky across calls.
- **Recommendation:** KEEP ALL THREE — each tests a distinct no-op condition. RTS08's `pages_visited=0` check is unique; RTS31's "no stable timestamp" is unique; RTS41's dryrun-persistence is unique.

---

### [NEAR-DUP] RTS10 / RTS26 / RTS39 — "RTS concurrent with background checkpoint"

- **Tests:** `test_rollback_to_stable10`, `test_rollback_to_stable26`, `test_rollback_to_stable39`
- **What they share:** All use `timing_stress_for_test=[history_store_checkpoint_delay]` or similar to create a race between RTS and checkpoint. All verify data correctness after crash restart.
- **What distinguishes them:**
  - RTS10: concurrent checkpoint + write beyond stable (general case); also has `test_rollback_to_stable_prepare` sub-test for active prepared txns during checkpoint.
  - RTS26: specifically tests a rolled-back prepared transaction concurrent with a background checkpoint.
  - RTS39: eviction moves data to HS while checkpoint is running; then crash. Unique in that all RTS stats are 0 after restart (checkpoint timing stress already removed the unstable data).
- **Recommendation:** KEEP ALL THREE — each exercises a different interaction between checkpoint timing and RTS. RTS10 is the most general; RTS26 adds the prepared-txn rollback dimension; RTS39 tests the "checkpoint already did the work" case which is unique.

---

### [NEAR-DUP] RTS19 / RTS32 — "RTS with prepared transaction insert+remove, eviction"

- **Tests:** `test_rollback_to_stable19`, `test_rollback_to_stable32`
- **What they share:** Both involve eviction of pages containing prepared or post-stable updates, then RTS.
- **What distinguishes them:**
  - RTS19 specifically tests aborting a prepared txn that both inserts and removes a key (no-history and with-history variants).
  - RTS32 tests update_restore eviction with a tombstone: specifically that RTS converts the tombstone back to value_b, then eviction correctly handles the now-absent tombstone.
- **Recommendation:** KEEP BOTH — RTS19 tests prepared-txn rollback with both insert and remove; RTS32 tests the update-restore-evict code path specifically. Different sub-systems.

---

### [NEAR-DUP] RTS43 / RTS02+RTS03 — "RTS with worker threads across multiple tables"

- **Tests:** `test_rollback_to_stable43` vs. `test_rollback_to_stable02`, `test_rollback_to_stable03`
- **What they share:** RTS43 performs the same "stable below latest commit, verify HS-restored value" scenario as RTS02/RTS03 but specifically exercises 10 tables and finer worker-thread counts (0/1/2/3/4).
- **What distinguishes them:** RTS43 focuses on multi-table concurrency with fine-grained thread counts that are not in other tests (threads=1, 2, 3 in addition to 0, 4).
- **Recommendation:** KEEP — the multi-table + fine thread-count coverage is worthwhile for the worker thread scheduler path. However, the data-correctness portion duplicates RTS02. Consider making RTS43 an `extraconfig` subclass of RTS02 to reduce duplication.

---

### [TRUE DUPLICATE (partial)] RTS09 — unique (schema ops)

- **Assessment:** `test_rollback_to_stable09` is the only RTS test covering schema operations (create/drop table and index). No overlap with any other RTS test.
- **Recommendation:** KEEP as-is.

---

### Summary of RTS Unique/Non-Overlapping Tests

The following RTS tests are sufficiently unique that no overlap concern applies:
- RTS05 (non-timestamp tables), RTS09 (schema ops), RTS12 (skip subtrees / aggregated timestamps), RTS13 (tombstone restoration — 4 distinct sub-tests), RTS18 (no-aggregated-time-window pages, in-memory only), RTS20 (dhandle management), RTS22 (HS eviction stress), RTS24 (column-store RLE recno bug), RTS25 (comprehensive VLCS RLE scenarios), RTS27 (VLCS + non-timestamp mixed), RTS28 (update_restore_evict during recovery / write generation), RTS29 (HS ordering with non-timestamp update inserted to tombstone), RTS30 (API error handling), RTS33 (in-memory logged vs. non-logged), RTS35 (WAL flush for checkpoint writes), RTS37 (no-timestamp update rewrites HS), RTS38 (fast-truncate entire HS btree), RTS40 (globally-visible eviction resets time window), RTS42 (missing file handling), RTS44 (prepared txn with no stable timestamp), RTS46 (clean reconciled pages with unstable updates).

---

## Checkpoint Test Overlaps

### [NEAR-DUP] checkpoint10 / checkpoint11 — "Inconsistent checkpoint consistency guarantee"

- **Tests:** `test_checkpoint10`, `test_checkpoint11`
- **What they share:** Both verify the all-or-nothing visibility guarantee for a transaction committing concurrently with a checkpoint. Both use `timing_stress_for_test=[checkpoint_slow]`.
- **What distinguishes them:**
  - checkpoint10: non-timestamped transaction; verifies both tables are either fully visible or fully invisible.
  - checkpoint11: timestamped transaction; reads at `read_timestamp` values confirm consistent snapshot.
- **Recommendation:** MERGE — These are near-duplicates at the semantic level. The timestamped vs. non-timestamped distinction could be a single `use_timestamps=True/False` parametrization. However, the timestamp case does exercise the `read_timestamp` filtering path in the checkpoint cursor, which is genuinely different code. **Low priority merge.**

---

### [NEAR-DUP] checkpoint01 (test_checkpoint_cursor_multiple) / checkpoint13 (concurrent cursor)

- **Tests:** `test_checkpoint01.test_checkpoint_cursor_multiple`, `test_checkpoint13`
- **What they share:** Both verify that multiple cursors can be opened on the same checkpoint.
- **What distinguishes them:**
  - checkpoint01 opens two cursors and reads from both.
  - checkpoint13 opens cursors within a transaction, tests `read_timestamp < oldest_timestamp`, and tests EBUSY on drop while cursor open.
- **Recommendation:** KEEP BOTH — checkpoint13 covers API restrictions (read_timestamp, EBUSY) not in checkpoint01. The "multiple cursors" overlap is minor.

---

### [UNIQUE] checkpoint02 through checkpoint07, checkpoint09, checkpoint14, checkpoint15

- **Assessment:** Each of these tests a distinct scenario: concurrent inserts (02), HS writes during checkpoint (03), timing stats (04), checkpoint count with backup (05), RTS-truncation interaction (06), clean checkpoint timer (07), obsolete time window cleanup (09), two successive snapshots (14), per-checkpoint timestamp restrictions (15). No meaningful overlaps among these.
- **Recommendation:** KEEP ALL as-is.

---

### [NEAR-DUP] checkpoint12 — covered by test_prepare04

- **Tests:** `test_checkpoint12` (checkpoint cursor + prepared txn → PREPARE_CONFLICT), `test_prepare04` (PREPARE_CONFLICT detection)
- **What they share:** Both verify `WT_PREPARE_CONFLICT` when reading a prepared key.
- **What distinguishes them:** checkpoint12 tests this specifically via a checkpoint cursor (a different code path from a regular cursor). test_prepare04 tests it via a regular cursor.
- **Recommendation:** KEEP BOTH — the checkpoint cursor's behavior with prepared transactions is a distinct code path.

---

## History Store Test Overlaps

### [NEAR-DUP] test_hs10 / test_hs08 (phase 1) / test_hs06 (test_hs_modify_reads) — "Modify chain reconstructed from HS after eviction"

- **Tests:** `test_hs10`, `test_hs08` (Phase 1), `test_hs06.test_hs_modify_reads`
- **What they share:** All three write a base value followed by 2–3 modifies at distinct timestamps, evict the page (via a large second table or direct eviction), then read at earlier timestamps and verify the reconstructed value.
- **What distinguishes them:**
  - `test_hs10`: eviction pressure via 10,000-row second table, very small cache (2 MB). The only assertion is that reads at ts=3, 4, 5 return the correct value.
  - `test_hs08` (Phase 1): same pattern but also counts `cache_hs_insert` and `cache_hs_write_squash==0`.
  - `test_hs06.test_hs_modify_reads`: also tests forward-scan HS reconstruction (find the nearest full update, apply deltas) — documents the algorithm explicitly.
- **Recommendation:** MERGE test_hs10 into test_hs08 — `test_hs10` adds only the "small cache + eviction via second table" variant. This can be a scenario parameter in `test_hs08`. **High priority.**

---

### [NEAR-DUP] test_hs11 / test_hs32 — "Non-timestamped update/tombstone clears HS records"

- **Tests:** `test_hs11`, `test_hs32`
- **What they share:** Both apply timestamped updates (ts=1..4), then apply a non-timestamped operation (delete or update) on every other row, verify that the old HS records are gone. Both check `cache_hs_key_truncate` stats.
- **What distinguishes them:**
  - `test_hs11` is at `file:` level, covers modify, long-running reader; checks `cache_hs_key_truncate_onpage_removal`.
  - `test_hs32` is at `table:` level, covers long-running reader variant; checks `cache_hs_key_truncate`.
  - `test_hs31` (missed above — also in this group) is at `file:` level too, verifies `rec_hs_wrapup_next_prev_calls`.
- **Assessment:** These three tests cover the same semantic scenario (non-ts op clears HS) but at different storage levels and with different statistics.
- **Recommendation:** KEEP ALL THREE — different stats, different storage levels (`file:` vs. `table:`). However, the scenario-matrix (192 scenarios in hs11) is extremely large. Consider pruning scenarios in hs11 by reducing `small/large nrows` and `insert-list/update-list` combinations, which add little value beyond testing the WT internal update-chain structure (which is already well-tested).

---

### [NEAR-DUP] test_hs12 / test_hs13 / test_hs19 — "Modify append/prepend chain in HS"

- **Tests:** `test_hs12`, `test_hs13`, `test_hs19`
- **What they share:** All three test HS reconstruction of modify chains involving appending or prepending characters (extending the string).
- **What distinguishes them:**
  - `test_hs12`: pure append and prepend modifies, verified with a second session.
  - `test_hs13`: specifically tests the "walk forward through HS to find base, apply reverse deltas" code path for *prepend* modifies.
  - `test_hs19`: regression for a specific bug where an append modify at offset 102 (zero-replacement, extends string) corrupted reconstruction of earlier timestamps.
- **Recommendation:** KEEP ALL THREE — each tests a distinct code path or corner case. `test_hs19` is a targeted regression test.

---

### [NEAR-DUP] test_hs01 (scenario 2) / test_hs06.test_hs_multiple_modifies / test_hs08 — "Multiple modifies in one txn go to HS"

- **Tests:** `test_hs01` scenario 2, `test_hs06.test_hs_multiple_modifies`, `test_hs08` Phase 3
- **What they share:** All three apply multiple modifies in one transaction to the same key and verify HS behavior.
- **What distinguishes them:**
  - `test_hs01` focuses on crash/recovery.
  - `test_hs06.test_hs_multiple_modifies` verifies exact byte positions in the result.
  - `test_hs08` Phase 3 verifies the `cache_hs_write_squash==1` stat (same-txn same-ts modifies are squashed).
- **Recommendation:** KEEP ALL THREE — the squash stat check (hs08), the exact reconstruction check (hs06), and the recovery path (hs01) are different.

---

### [NEAR-DUP] test_hs06.test_hs_instantiated_modify / test_hs06.test_hs_modify_stable_is_base_update

- **Tests:** Both within `test_hs06`
- **What they share:** Both test that three sequential modifies with `stable_timestamp=1` (forcing the base update behind stable) are correctly reconstructed at ts=5.
- **What distinguishes them:** The descriptions say "same as above but stable_timestamp=1 forces the base update behind stable" — this is essentially duplicated within the same file.
- **Recommendation:** MERGE into a single parametrized test method within test_hs06.

---

### [UNIQUE] Tests with no meaningful overlap

The following HS tests are sufficiently unique: `test_hs02` (truncation with HS), `test_hs03` (HS read minimization during checkpoint), `test_hs04` (file_max config), `test_hs07` (sweep server), `test_hs09` (checkpoint partitioning), `test_hs14` (performance — invisible vs. visible HS), `test_hs15` (eviction/checkpoint interaction), `test_hs16` (non-timestamped update panic regression), `test_hs18` (multiple older readers), `test_hs20` (overflow values), `test_hs21` (idle file handle sweep), `test_hs24` (missing-timestamp races), `test_hs25` (prepared update structure), `test_hs26` (VLCS RLE groups), `test_hs27` (VLCS heterogeneous timestamps), `test_hs28` (full update when squashed), `test_hs29` (three concurrent HS cursors), `test_hs30` (non-timestamped with older readers), `test_hs33` (recovery before metadata sync), `test_hs_evict_race01` (race condition regression).

---

## Prepare Test Overlaps

### [NEAR-DUP] test_prepare02 / test_prepare03 — "Operations forbidden after prepare_transaction"

- **Tests:** `test_prepare02`, `test_prepare03`
- **What they share:** Both exhaustively test that operations after `prepare_transaction()` return the correct error.
- **What distinguishes them:**
  - `test_prepare02`: session-level operations (open_cursor, alter, create, compact, drop, etc.).
  - `test_prepare03`: cursor-level operations (insert, next, prev, get_key, search, etc.).
- **Recommendation:** KEEP BOTH — session vs. cursor operations go through completely different code paths.

---

### [NEAR-DUP] test_prepare09 / test_prepare10 — "Rollback of prepared transaction does not corrupt state"

- **Tests:** `test_prepare09`, `test_prepare10`
- **What they share:** Both verify that rolling back a prepared transaction does not leave incorrect state (tombstone or wrong time window).
- **What distinguishes them:**
  - `test_prepare09`: two sub-tests — on-disk value present vs. in-memory only; verifies no spurious tombstone.
  - `test_prepare10`: verifies time window metadata is correctly reverted on rollback; uses a second session to verify snapshot visibility.
- **Recommendation:** KEEP BOTH — tombstone vs. time-window are different internal structures.

---

### [UNIQUE] Prepare tests with no meaningful overlap

`test_prepare01` (visibility), `test_prepare04` (prepare conflict + write conflict), `test_prepare05` (timestamp ordering constraints), `test_prepare06` (roundup_timestamps), `test_prepare07` (non-durability in backup), `test_prepare08` (tombstones evicted to data store).

---

## Layered Test Overlaps and Low-Value Ports

The layered tests fall into distinct categories: core disagg-specific features, cursor mechanics, role transitions, page delta mechanics, and regression tests. The vast majority are genuinely novel disagg scenarios. Below are the true overlaps and the few lower-value redundancies.

---

### [NEAR-DUP] test_layered01 / test_layered02 — "Basic create and cursor lifecycle"

- **Tests:** `test_layered01`, `test_layered02`
- **What they share:** Both are pure smoke tests. layered01 creates a table and checks metadata; layered02 creates a table, opens and closes a cursor.
- **What distinguishes them:** Metadata verification (01) vs. cursor lifecycle (02).
- **Recommendation:** MERGE — layered02's cursor open/close is trivially covered by the start of layered03's test. The metadata verification in layered01 is more meaningful. **Low priority, minor CI saving.**

---

### [NEAR-DUP] test_layered33 (delete) / test_layered49 (tombstone retention) / test_layered78 (WT_NOTFOUND on remove)

- **Tests:** `test_layered33`, `test_layered49`, `test_layered78`
- **What they share:** All three test the delete/remove path on the ingest table.
- **What distinguishes them:**
  - layered33: basic insert-then-delete of all 100 rows; verifies empty table.
  - layered49: specifically tests that tombstones are NOT discarded by eviction before a checkpoint. The correctness concern is different.
  - layered78: minimal test — remove on a nonexistent key returns `WT_NOTFOUND`. A pure API contract test.
- **Recommendation:** KEEP ALL THREE — different assertions. layered78 is a one-line regression test for a specific return code. layered49's eviction-pin scenario is unique.

---

### [NEAR-DUP] test_layered22 / test_layered83 (ingest-only scans)

- **Tests:** `test_layered22.test_secondary_reads_without_stable`, `test_layered83.test_next_ingest_only`, `test_layered83.test_prev_ingest_only`
- **What they share:** All verify full forward and backward scans on an ingest-only table (no stable checkpoint).
- **What distinguishes them:** layered22 uses 30,000 rows and verifies count; layered83 uses 1,000 rows and focuses on correctness of key order.
- **Recommendation:** KEEP BOTH — layered22 tests scale; layered83 tests correctness. Different scale objectives.

---

### [NEAR-DUP] test_layered05 (search_near) / test_layered83 (search_near sub-tests)

- **Tests:** `test_layered05`, select methods in `test_layered83`
- **What they share:** Both cover `search_near` on interleaved stable/ingest data, boundary cases, and tombstone handling.
- **What distinguishes them:**
  - layered05 is exhaustive at the search_near API level (28 distinct test methods covering all structural cases).
  - layered83's `search_near` sub-tests focus on iteration order after `search_near` (the ordering invariant) and mixing `search_near` with `search`.
- **Recommendation:** KEEP BOTH — the iteration-after-search_near ordering tests in layered83 are genuinely additive to the pure search_near API coverage in layered05. However, the following layered83 sub-tests are fully covered by layered05 and add no value:
  - `test_layered83.test_next_after_search_near_exact` — covered by `layered05.test_search_near_then_iterate`
  - `test_layered83.test_prev_after_search_near_exact` — covered by `layered05.test_search_near_then_iterate`

  **Recommendation:** REMOVE these two sub-tests from layered83 or note them as vestigial.

---

### [NEAR-DUP] test_layered82 (cursor bounds) / test_layered83 (bounds + search_near)

- **Tests:** `test_layered82`, `test_layered83.test_bounds_*` (none — bounds are in layered82; layered83 does not use bounds directly)
- **Assessment:** No overlap — layered82 is the dedicated bounds test; layered83 does not exercise bounds.

---

### [NEAR-DUP] test_layered09 (delta: write/modify/delete/insert) / test_layered32 (leaf and internal deltas) / test_layered63 (delta correctness across merge)

- **Tests:** `test_layered09`, `test_layered32`, `test_layered63`
- **What they share:** All three test page delta writing and correctness on followers.
- **What distinguishes them:**
  - layered09: tests leaf deltas specifically for write, modify, delete, insert, and multi-delta chains with follower timestamp reads. The simplest, most direct test.
  - layered32: tests internal page deltas (not covered by layered09), delta produced during page split/merge, and the `cache_read_internal_delta` stat.
  - layered63: tests delta merge correctness across complex multi-round patterns (keys at end of base image, base image with trailing keys, repeated same-key updates in multiple delta rounds).
- **Recommendation:** KEEP ALL THREE — internal deltas (layered32) and delta merge edge cases (layered63) are not covered by layered09.

---

### [NEAR-DUP] test_layered20 (32 consecutive deltas) / test_layered18 (10 consecutive deltas)

- **Tests:** `test_layered18`, `test_layered20`
- **What they share:** Both test repeated single-key updates across many sequential checkpoints to verify long delta chain correctness.
- **What distinguishes them:**
  - layered18: 10 delta rounds, simple scenario, leader+follower, comment says "validates delta chain in page log extension."
  - layered20: 32 delta rounds, also covers timestamp and non-timestamp modes, multiple encoding/compression parametrizations.
- **Assessment:** layered18 is a strict subset of layered20's scenarios.
- **Recommendation:** REMOVE test_layered18 (absorbed by layered20). layered20 is parametrized more completely and covers 32 rounds. **High priority.**

---

### [NEAR-DUP] test_layered35 (empty delta when only uncommitted update) / test_layered45 (durable entries excluded from deltas) / test_layered70 (skip full-page write when no stable progress)

- **Tests:** `test_layered35`, `test_layered45`, `test_layered70`
- **What they share:** All three test the "do not write a page delta/full page when there is no stable progress" optimization.
- **What distinguishes them:**
  - layered35: leaf page delta skipped when only an uncommitted update exists; encryption/compression parametrized.
  - layered45: 6 sub-tests covering many combinations (prepared vs. committed, delete vs. update, with eviction); verifies delta count across multiple checkpoints.
  - layered70: full-image write skipped when commit_timestamp > stable_timestamp; specifically for the no-delta (full-image) path.
- **Recommendation:** KEEP ALL THREE — layered35 tests the leaf delta path; layered70 tests the full-image path; layered45 covers the interaction with prepared transactions. Different skip conditions.

---

### [NEAR-DUP] test_layered76 (verify correctness) — low value

- **Tests:** `test_layered76.test_ckpt_size_verify_simple`, `test_ckpt_size_verify_multi_insert`
- **What they share:** Both insert a handful of keys and call `verifyUntilSuccess()`. The 1-key and 10-key cases add negligible coverage over the 100K-key case.
- **Recommendation:** MERGE `test_ckpt_size_verify_simple` and `test_ckpt_size_verify_multi_insert` into `test_ckpt_size_verify_large_dataset`. The simple cases add no value when a larger case passes. **Low priority.**

---

### [NEAR-DUP] test_layered81 / test_layered85 — Checkpoint advance during cursor scans

- **Tests:** `test_layered81`, `test_layered85`
- **What they share:** Both test that a cursor that has been positioned can still receive a mid-scan checkpoint advance correctly.
- **What distinguishes them:**
  - layered81 covers 11 sub-scenarios (full scan, updated values, deleted keys, search_near, timestamp reads, bounds, tombstone persistence, leader unaffected).
  - layered85 covers 5 sub-scenarios that are strictly "while cursor is scanning, a NEW checkpoint arrives and the stable cursor is atomically swapped" — testing the mid-scan swap event specifically. Checks `layered_curs_advance_stable` stat.
- **Assessment:** layered81 tests "after checkpoint advance, cursor sees correct data" (post-advance state). layered85 tests "during an active scan, a checkpoint advance occurs" (during-advance transition). These are different code paths.
- **Recommendation:** KEEP BOTH — the mid-scan swap is a distinct code path not covered by layered81.

---

### [NEAR-DUP] test_layered91 (exhaustive state matrix) / test_layered83 (comprehensive cursor ops)

- **Tests:** `test_layered91`, `test_layered83`
- **What they share:** Both perform extensive cursor operations (forward/backward scan, search, tombstone skipping) on layered tables with various combinations of stable and ingest data.
- **What distinguishes them:**
  - layered83: 40+ sub-tests focused on ordering invariants, direction switching, and iteration after search_near.
  - layered91: generates all per-key state sequences of length ≤5 from {I, S, B, R, X} and verifies forward scan, backward scan, and point reads for each combination. Exhaustive state coverage.
- **Recommendation:** KEEP BOTH — the exhaustive state machine approach in layered91 complements but does not replace the iteration-ordering tests in layered83.

---

### [NEAR-DUP] test_layered_cursor01 / test_layered83

- **Tests:** `test_layered_cursor01`, `test_layered83`
- **What they share:** Both run forward/backward full scans with various insert/update/remove workloads on leader and follower, with cursor repositioning.
- **What distinguishes them:**
  - layered_cursor01: uses the Oplog helper for timestamped operations; verifies positioned iteration (5 position anchors × 4 positioning methods); covers update/remove percentages (20%/50%/70%); leader+follower consistency after checkpoint advance.
  - layered83: does not use Oplog; focuses on ordering invariants, direction switching, tombstone skipping in `next`/`prev`, and mixing `search` + `search_near` on the same cursor.
- **Recommendation:** KEEP BOTH — the Oplog timestamped workload with checkpoint propagation in layered_cursor01 is not present in layered83.

---

### [DUPLICATE] test_layered43 — fully skipped

- **Assessment:** `test_layered43` is entirely skipped via `self.skipTest("FIXME-WT-15663")`. It adds zero CI value.
- **Recommendation:** REMOVE or convert to a stub marked `@unittest.skip`. **High priority — eliminates test runner overhead entirely.**

---

### [NEAR-DUP] test_layered28 / test_layered24 — Drop semantics

- **Tests:** `test_layered24`, `test_layered28`
- **What they share:** Both test drop semantics for layered tables — that after dropping, cursors can no longer be opened.
- **What distinguishes them:**
  - layered24: specifically tests that a follower drop does NOT fall back to reading from stable; unique correctness concern.
  - layered28: covers leader drop, sweep thread interaction, and the critical "follower drop does not propagate to shared metadata" invariant.
- **Recommendation:** KEEP BOTH — different drop invariants (stable read isolation vs. shared metadata isolation).

---

### [NEAR-DUP] test_layered30 / test_layered36 — Empty table recovery

- **Tests:** `test_layered30`, `test_layered36`
- **What they share:** Both verify that empty tables survive restart without local files.
- **What distinguishes them:**
  - layered30: also tests follower checkpoint pickup (not just cold restart), and the "one empty table + one data table" scenario.
  - layered36: specifically verifies one data-containing table alongside an empty table.
- **Recommendation:** MERGE — `test_layered36` is a strict subset of the scenarios in `test_layered30`. The cold-restart + empty-table case is already in layered30. **Medium priority.**

---

### Tests with no overlap — Unique to the disagg/layered suite

The following tests cover scenarios with no non-layered equivalent and no intra-suite overlap:
- layered07 (leader/follower role switch with data propagation)
- layered08 (encryption + compression + follower reread)
- layered15 (restart without local files — metadata, shared metadata)
- layered17 (timestamp propagation through checkpoint to follower)
- layered19 (max_consecutive_delta enforcement)
- layered21 (role transition: leader→follower→leader)
- layered23 (oplog simulation, checkpoint pickup stats)
- layered25 (historical reads after restart without local files)
- layered26 (follower sees stable data only after checkpoint advance)
- layered27 (drain: insert/update/remove during follower-to-leader promotion)
- layered29 (10,000 tables at scale — @longtest)
- layered31 (cursor stability across checkpoint pick-ups)
- layered34 (materialization frontier controls eviction)
- layered37 (pinned ingest pages not evicted prematurely)
- layered38 (ingest GC with/without cursors)
- layered39 (eviction blocked ahead of materialization frontier)
- layered40 (layered tables have logging disabled)
- layered41 (duplicate key on insert with overwrite=false)
- layered44 (freed pages never read by follower)
- layered45 (durable entries excluded from new leaf deltas)
- layered46 (local files deleted on restart)
- layered47 (prune-timestamp initialization regression)
- layered48 (no overflow keys/values in disagg)
- layered49 (tombstones not discarded before checkpoint inclusion)
- layered50 (follower evicts without materialization frontier)
- layered51 (logging rejected for layered tables)
- layered52 (internal delta with deleted leaf pages)
- layered53 (checkpoint for stable timestamp advance only)
- layered54 (prefix/suffix compression in deltas)
- layered55 (obsolete time window not reviewed on follower)
- layered56 (no delta on page split)
- layered57 (follower not use app threads for dirty eviction)
- layered58 (cursor walk with delta pages)
- layered59 (internal delta not built when first key modified)
- layered60 (empty table creation during checkpoint)
- layered61 (ingest timestamps not cleared when globally visible)
- layered62 (checkpoint/role-change synchronization)
- layered64 (checkpoint metadata checksum)
- layered65 (GC of prepared updates on ingest table)
- layered66 (verify fails for unmaterialized pages)
- layered67 (update-restore eviction with deltas disabled)
- layered68 (address cookie upgrade/downgrade compatibility)
- layered69 (prepared rollback reconciliation with disagg)
- layered71 (drop empty table during checkpoint)
- layered72 (pinned HS dhandle on follower survives checkpoint advance)
- layered73 (cursor key state after WT_PREPARE_CONFLICT)
- layered74 (internal delta with encryption and compression)
- layered75 (metadata file ID correctness)
- layered77 (leader→follower with split pages in eviction)
- layered79 (on-disk ingest value removed after GC)
- layered80 (sweep server does not close ingest dhandle during step-up)
- layered84 (cursor walks with prepared conflicts)
- layered86 (file ID high-water mark on step-up)
- layered87 (RTS skipped at startup, works at runtime)
- layered88 (unsupported operations return errors)
- layered89 (checkpointed prepared cells do not raise PREPARE_CONFLICT on follower)
- layered90 (follower picks up multiple sequential checkpoints)
- layered92 (cursor.reserve() on all key states)
- layered93 (cursor ops on stable-only keys on follower)
- layered94 (prepared transactions survive follower step-up)
- layered96 (stale alternate cursor regression)
- test_layered_cursor01 (general cursor correctness with Oplog)
- test_layered_fast_truncate01/02/03 (fast truncate on layered)
- test_layered_modify01 (modify across checkpoint)

---

## Cursor Test Overlaps

### [NEAR-DUP] test_cursor02 / test_cursor03 — "Insert/remove with TestCursorTracker"

- **Tests:** `test_cursor02`, `test_cursor03`
- **What they share:** Both use `TestCursorTracker` to verify insert+remove operations on row and column store tables.
- **What distinguishes them:**
  - cursor02: small tables (no explicit size; default), covers empty-table edge cases and single-item edge cases.
  - cursor03: larger tables (1,000 and 10,000 entries) with variable key/value sizes (up to 10,000 bytes), specifically exercising overflow items.
- **Recommendation:** KEEP BOTH — cursor03's overflow item coverage is not in cursor02.

---

### [NEAR-DUP] test_cursor07 / test_cursor08 — "Log cursor reading"

- **Tests:** `test_cursor07`, `test_cursor08`
- **What they share:** Both open a log cursor (`log:` URI) after inserting into logged tables and verify WAL entries.
- **What distinguishes them:**
  - cursor07: tests logged vs. non-logged tables; verifies non-logged tables produce no WAL entries.
  - cursor08: tests log reading with compression (snappy, zlib, nop, none).
- **Recommendation:** KEEP BOTH — non-logged table WAL behavior (cursor07) and compressed log reading (cursor08) are distinct.

---

### [UNIQUE] cursor01, cursor04, cursor05, cursor06, cursor09

- **Assessment:** Each covers a unique API surface: iteration + duplicate cursors (01), search/search_near exact match (04), endpoint/reset behavior with column groups (05), reconfigure overwrite/readonly (06), key state after insert (09 — specific WT-2217 regression).
- **Recommendation:** KEEP ALL as-is.

---

## Summary: High-Priority Consolidation Candidates

| Priority | Tests to Merge/Remove | Reason | Est. CI Time Saving |
|---|---|---|---|
| HIGH | Remove `test_layered43` | 100% skipped at runtime (FIXME-WT-15663); no CI value | ~10 s per run |
| HIGH | Remove `test_layered18`, keep `test_layered20` | layered18 is a strict subset of layered20 scenarios (10 rounds vs. 32, fewer parametrizations) | ~30 s per run |
| HIGH | Merge `test_hs10` into `test_hs08` | test_hs10 is test_hs08 phase 1 with a different eviction mechanism; add `eviction_method` scenario to hs08 | ~20 s per run |
| MEDIUM | Merge `test_layered36` into `test_layered30` | layered36 tests empty+one-data-table recovery, already covered by layered30's `another_table=True` scenario | ~15 s per run |
| MEDIUM | Remove 2 redundant sub-tests from `test_layered83` | `test_next_after_search_near_exact` and `test_prev_after_search_near_exact` are fully covered by `test_layered05.test_search_near_then_iterate` | ~5 s per run |
| MEDIUM | Merge `test_layered76.test_ckpt_size_verify_simple` + `_multi_insert` into `_large_dataset` | 1-key and 10-key cases add no coverage over 100K-key case | ~10 s per run |
| MEDIUM | Merge `test_hs06.test_hs_instantiated_modify` + `test_hs_modify_stable_is_base_update` | Near-duplicate methods within the same file; parametrize `stable_is_base=True/False` | ~10 s per run |
| LOW | Merge `test_rollback_to_stable16` into `test_rollback_to_stable01` | RTS16 source admits redundancy; scenarios are subset of RTS01 plus distinct-batch-range pattern which doesn't add code coverage | ~25 s per run |
| LOW | Merge `test_checkpoint10` + `test_checkpoint11` into single parametrized test | Timestamped vs. non-timestamped inconsistent checkpoint; same structure, single `use_timestamps` parameter | ~15 s per run |
| LOW | Merge `test_layered01` + `test_layered02` | layered01 metadata check + layered02 cursor lifecycle are both covered by the setup in layered03 | ~5 s per run |
| LOW | Merge `test_layered_cursor01.test_populated_tables_with_updates_20_percent` (both definitions) | Two methods with same name; the offset variant should be renamed `_with_updates_20_percent_offset` and tested as one combined parametrization | Correctness fix, no CI saving |
