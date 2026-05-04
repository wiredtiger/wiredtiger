# Scenario Gap Analysis — Master Synthesis

> Pass 4 (May 2026) · Scenario-based analysis of all public APIs on layered/disagg tables  
> Covers supported features only. Unsupported feature gaps are tracked separately in `08_unsupported_features.md`.

---

## What Changed in Pass 4

- Per-API scenario analysis completed for all six API groups (`01`–`06`).
- All gaps for unsupported features removed from HIGH/MEDIUM/LOW tiers and moved to `08_unsupported_features.md`. Unsupported features include: elegant step-down, RTS, session.alter(), named checkpoints, salvage, compact, import, bulk cursors, backup cursors, RECNO/column store, index creation, table drop (WT-14503), fast truncate, and prepared transactions (disagg-specific behavior).
- cursor.modify() gaps remain HIGH/MEDIUM but annotated **BLOCKED** by `ops.pct.modify=0` in `CONFIG.disagg` (FIXME-WT-16479); first tests to write once that flag is removed.
- Two gap claims refuted by source verification: read_timestamp + iteration *is* tested in `test_layered73.py`; atomic committed cross-table transactions *are* tested in `test_layered94.py` (prepared path only; regular committed path remains a CRITICAL gap).
- `session.rename()` confirmed absent — `WT_SESSION` has no rename method in this codebase.
- *Slow* truncate IS supported (Private Preview); truncate gaps cover slow-truncate scenarios. The existing `test_layered_fast_truncate01-03` tests exercise an unsupported code path.

---

## Summary Statistics

### Supported-Feature Gaps by Priority

| Priority | CR | CW | TT | SO | CP | CS | Total |
|----------|----|----|----|----|----|----|-------|
| CRITICAL | 0 | 0 | 0 | 0 | 0 | 1 | **1** |
| HIGH | 6 | 9 | 3 | 6 | 5 | 7 | **36** |
| MEDIUM | 13 | 11 | 9 | 5 | 5 | 11 | **54** |
| LOW | 7 | 2 | 1 | 1 | 4 | 4 | **19** |
| **Total** | **26** | **22** | **13** | **12** | **14** | **23** | **110** |

**Column keys:** CR = cursor reads, CW = cursor writes, TT = transactions/timestamps,
SO = schema/session ops, CP = checkpoint/roles, CS = connection/concurrency/stats

Unsupported-feature gaps are listed in `08_unsupported_features.md` and are **not** counted above.

---

## Unsupported Features — Not Counted Here

Gaps for features that are not currently supported in disagg are tracked in
[`08_unsupported_features.md`](08_unsupported_features.md). For each unsupported feature the
test goal is a **negative/behavior test** (confirm correct error) or **DEFERRED** (implement
when the feature lands). Key items:

| Feature | Status | Tracking IDs |
|---------|--------|--------------|
| Elegant step-down (reconfigure without restart) | DEFERRED — Public Preview | SD-1–SD-5 |
| Prepared transactions (disagg-specific behavior) | DEFERRED — Public Preview | PT-1–PT-5 |
| Table drop (`session.drop()`) | DEFERRED — Public Preview, WT-14503 | DRP-1–DRP-5 |
| Fast truncate | DEFERRED — Public Preview | FT-1 |
| RTS (`rollback_to_stable`) | Never | RTS-1–RTS-5 |
| Named checkpoints | No plan | NC-1 |
| session.alter() | No plan | ALT-1–ALT-2 |
| session.salvage(), compact(), import() | No plan / Never | SAL-1, CMP-1, IMP-1 |
| Bulk cursors, backup cursors | Not planned / Never | BLK-1, BAK-1 |
| key_format=r (RECNO/column store) | Never | REC-1 |
| Index creation on layered tables | Skipped | IDX-1 |

---

## CRITICAL Gaps

Only one CRITICAL gap remains after removing all unsupported-feature items.

### CS-C1 — Atomic committed (non-prepared) transaction spanning two layered tables
`test_layered94` covers *prepared* cross-table transactions, but no test covers a plain
`begin_transaction() → write table_a → write table_b → commit_transaction()`. The two
`*.wt_ingest` btrees are physically separate objects. Cross-table durability atomicity for
regular committed transactions is entirely unverified: a crash between the two ingest writes
could produce a checkpoint where one table has the key but the other does not.  
**Suggested test:** `test_layered_multi_txn01.py`

---

## HIGH Gaps — By API Group

Full descriptions are in the per-API files; brief summaries below.

### Cursor Reads (01)
1. `search()` on key with tombstone in ingest (stable value hidden by ingest tombstone)
2. `search_near()` equidistant-neighbor determinism (non-determinism breaks resume semantics)
3. `search_near()` on exact match covered by tombstone in both layers
4. `cursor.bound()` + `read_timestamp` combined (MongoDB range-query-in-snapshot pattern)
5. `next()`/`prev()` concurrent with a writer session (snapshot isolation of merge cursor)
6. `cursor.bound()` + step_up role transition (`__clayered_adjust_state` bounds interaction)

### Cursor Writes (02)
7. `update()` on key that exists only in stable (core disagg mutation pattern)
8. `remove()` on key that exists only in stable
9. `insert()` on key with tombstone in ingest (re-insert after delete)
10. `modify()` on stable-only key — delta base must come from stable *(BLOCKED: FIXME-WT-16479)*
11. `modify()` on tombstone in ingest *(BLOCKED)*
12. `modify()` on version-split key (stable=v1, ingest=v2; must use ingest as delta base) *(BLOCKED)*
13. `reserve()` end-to-end: reserve → write → commit (all current tests roll back)
14. `reserve()` + `modify()` in same transaction *(BLOCKED)*
15. Commit-timestamp < stable-timestamp enforcement for all write operations

### Transactions and Timestamps (03)
16. Read timestamp older than `oldest_timestamp` — disagg GC may have discarded old ingest versions
17. `oldest_timestamp` advancement and ingest garbage collection (GC correctness)
18. Commit timestamp < stable_timestamp enforcement

### Schema and Session Ops (04)
19. Truncate full-table URI form `session.truncate('layered:X', None, None, None)` — zero coverage
20. Truncate on leader (all existing truncate tests run on follower)
21. Truncate of stable-only data (existing tests only truncate ingest data)
22. `verify()` on a follower connection (READONLY stable btrees — correct behavior under verify)
23. `verify()` on table with only ingest data (stable btree empty or non-existent)
24. `session.create()` during active drain (`manager->entries` race; FIXME-WT-14734)

### Checkpoint and Roles (05)
25. Follower calls `disagg_advance_checkpoint` before leader has produced any checkpoint
26. Checkpoint immediately after step_up on connection that picked up a follower checkpoint
27. `session.checkpoint('force=true')` disagg-specific semantics (one incidental call, not validated)
28. Connection close during multithreaded drain (`drain_threads > 1` — zero Python test coverage)
29. `conn.close()` concurrent with in-progress step_up (checkpoint lock contention race)

### Connection, Concurrency, and Stats (06)
30. `disagg_block_get` and `disagg_block_put` counters never asserted (core I/O path invisible)
31. `disagg_role_leader` counter never asserted (role misreporting goes undetected)
32. `disagg_conn_reconfig` counter never asserted
33. Concurrent reader + active writer on same layered table (snapshot isolation of merge cursor)
34. Concurrent plain-insert write conflict detection (`WT_ROLLBACK`) on layered table
35. `cursor.dup()` on a positioned layered cursor — zero coverage (may not propagate merged position)
36. Page-log write error fault injection — error propagation from page-log API entirely untested

---

## Top 20 — Actionable Priority Order

Ranked by production risk and implementation urgency. All are testable today except item 20.

| # | ID | Gap | Suggested Test |
|---|----|-----|----------------|
| 1 | CS-C1 | Atomic committed transaction spanning two layered tables | `test_layered_multi_txn01.py` |
| 2 | CW-H1 | Update on stable-only key (core disagg mutation path) | `test_layered_update_stable01.py` |
| 3 | CW-H3 | Remove on stable-only key | `test_layered_remove_stable01.py` |
| 4 | CW-H6 | Re-insert after tombstone (`insert()` on deleted key) | `test_layered_insert01.py` |
| 5 | TT-H3 | Commit timestamp < stable_timestamp enforcement for write ops | `test_layered_timestamps03.py` |
| 6 | TT-H2 | `oldest_timestamp` advancement + ingest GC verification | `test_layered_timestamps02.py` |
| 7 | TT-H1 | Read timestamp older than `oldest_timestamp` | `test_layered_timestamps01.py` |
| 8 | CP-H1 | Follower `disagg_advance_checkpoint` before leader has any checkpoint | `test_layered_follower_first_advance.py` |
| 9 | CP-H4 | Close connection during multithreaded drain (`drain_threads > 1`) | new test with `drain_threads=4` |
| 10 | CP-H5 | `conn.close()` concurrent with in-progress step_up | new threading test |
| 11 | CS-H5 | Concurrent reader + active writer on same layered table | `test_layered_concurrent01.py` |
| 12 | CS-H8 | Concurrent write conflict (WT_ROLLBACK) on layered table | `test_layered_concurrent02.py` |
| 13 | SO-H1 | Truncate full-table URI form (non-cursor, slow truncate) | `test_layered_truncate_full_table.py` |
| 14 | SO-H4 | `verify()` on a follower connection (READONLY stable btrees) | `test_layered_verify_on_follower.py` |
| 15 | CS-H6 | `cursor.dup()` on positioned layered cursor | `test_layered_cursor_dup01.py` |
| 16 | CW-H7 | `reserve()` end-to-end: reserve + write + commit | `test_layered_reserve_update01.py` |
| 17 | CS-H2 | `disagg_block_get` and `disagg_block_put` counters never asserted | extend `test_layered02` or `test_layered04` |
| 18 | CS-H3 | `disagg_role_leader` counter never asserted | extend any reconfigure test |
| 19 | CS-H7 | Page-log write error fault injection | fault-injecting page-log wrapper |
| 20 | CW-H2 | Modify on stable-only key **(BLOCKED: FIXME-WT-16479)** | `test_layered_modify_stable01.py` — write now, run after flag removed |

---

## Coverage Level by API Group

| API Group | CRITICAL | HIGH | MEDIUM | LOW | Overall Level |
|-----------|----------|------|--------|-----|---------------|
| [Cursor Reads (01)](01_cursor_reads.md) | 0 | 6 | 13 | 7 | **MODERATE** — basic paths covered; adversarial/concurrent cases missing |
| [Cursor Writes (02)](02_cursor_writes.md) | 0 | 9 | 11 | 2 | **PARTIAL** — stable-only key mutations untested; modify fully disabled |
| [Transactions/Timestamps (03)](03_transactions_timestamps.md) | 0 | 3 | 9 | 1 | **PARTIAL** — basic timestamps covered; GC edge cases missing |
| [Schema/Session Ops (04)](04_schema_session_ops.md) | 0 | 6 | 5 | 1 | **PARTIAL** — truncate and verify edge cases missing; drop/fast-truncate deferred |
| [Checkpoint/Roles (05)](05_checkpoint_roles.md) | 0 | 5 | 5 | 4 | **MODERATE** — happy path good; multithreaded drain and startup race missing |
| [Connection/Concurrency/Stats (06)](06_connection_concurrency_stats.md) | 1 | 7 | 11 | 4 | **PARTIAL** — multi-table atomicity unverified; most stats dark; cursor.dup absent |

---

## Implementation Order

### Batch 1 — CRITICAL: write immediately

1. `test_layered_multi_txn01.py` — atomic committed cross-table transaction

### Batch 2 — HIGH: next iteration

2. `test_layered_update_stable01.py` — update on stable-only key
3. `test_layered_remove_stable01.py` — remove on stable-only key
4. `test_layered_insert01.py` — re-insert after tombstone
5. `test_layered_timestamps03.py` — commit_ts < stable_ts enforcement
6. `test_layered_timestamps02.py` — `oldest_timestamp` advancement + GC
7. `test_layered_timestamps01.py` — read_ts < oldest_ts
8. `test_layered_follower_first_advance.py` — follower advance before any leader checkpoint
9. close-during-multithreaded-drain test (configure `drain_threads=4`)
10. close-concurrent-with-step_up threading test
11. `test_layered_concurrent01.py` — concurrent reader + writer
12. `test_layered_concurrent02.py` — concurrent write conflict (WT_ROLLBACK)
13. `test_layered_truncate_full_table.py` — table-URI slow truncate
14. `test_layered_verify_on_follower.py` — verify() on follower with READONLY btrees
15. `test_layered_cursor_dup01.py` — cursor.dup on layered cursor
16. `test_layered_reserve_update01.py` — reserve end-to-end
17. stat assertions: `disagg_block_get`, `disagg_block_put`, `disagg_role_leader`, `disagg_conn_reconfig`
18. page-log fault injection test
19. `test_layered_modify_stable01.py` + sibling modify tests [write now; run after `ops.pct.modify=0` removed]

### Batch 3 — MEDIUM: following sprint

Remaining 54 MEDIUM gaps, prioritized by API group:
- Checkpoint/roles: step_up with uncommitted ingest data, step_up `__wt_panic` failure path, concurrent checkpoint advance with multiple reader snapshots
- Transactions: durable_ts vs commit_ts in drain filtering, concurrency and isolation scenarios
- Connection/stats: timing stat for step_up, abandon-checkpoint stats, database-size stat, 10+ session stress
- Schema ops: truncate scenarios (concurrent drain, unbounded range, re-insert), verify with config options
- Cursor reads: boundary/sparse/concurrency cases
- Cursor writes: concurrent conflict variants, remove/update ingest-only scenarios

---

## Per-API Detail Files

| File | API Group | Supported Gaps | Unsupported (see 08) |
|------|-----------|---------------|----------------------|
| [01_cursor_reads.md](01_cursor_reads.md) | cursor.search, search_near, next/prev, bound, reset | 26 | PT-2 (prepared txn) |
| [02_cursor_writes.md](02_cursor_writes.md) | cursor.insert, update, remove, modify, reserve | 22 | PT-3–PT-5 (prepared key conflicts) |
| [03_transactions_timestamps.md](03_transactions_timestamps.md) | timestamps, isolation/concurrency | 13 | PT-1–PT-5 (prepared txn) |
| [04_schema_session_ops.md](04_schema_session_ops.md) | create, truncate, verify, alter, rename | 12 | DRP-1–DRP-5 (drop), FT-1 (fast truncate), SAL-1, CMP-1, IMP-1 |
| [05_checkpoint_roles.md](05_checkpoint_roles.md) | checkpoint, step_up/step_down, follower advance, crashes | 14 | SD-1–SD-3 (step-down), NC-1 (named ckpt) |
| [06_connection_concurrency_stats.md](06_connection_concurrency_stats.md) | multi-table txns, stats, concurrency, cursor.dup, errors | 23 | SD-4–SD-5 (step-down) |
| [07_verification.md](07_verification.md) | Source-level verification of 10 top claims | — | — |
| [08_unsupported_features.md](08_unsupported_features.md) | Unsupported: drop, fast truncate, prepared txn, step-down, RTS, alter, compact, etc. | — | ~28 DEFERRED + ~13 MEDIUM/LOW |
