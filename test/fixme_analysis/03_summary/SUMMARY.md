# WiredTiger FIXME Analysis — Consolidated Summary

> Analysis date: 2026-05-04  
> Scope: All FIXME-WT-XXXXX comments in src/ and test/  
> Per-ticket files: `test/fixme_analysis/02_per_ticket/WT-XXXXX.md`

---

## Statistics

| Metric | Count |
|--------|-------|
| Unique tickets analyzed | 123 |
| **HIGH** importance | **34** |
| MEDIUM importance | 56 |
| LOW importance | 33 |
| COVERED | 10 |
| PARTIAL | 70 |
| **UNCOVERED** | **43** |

**HIGH + UNCOVERED** (worst combined category): **19 tickets**

---

## Top Findings by Risk Category

### 1. Silent Data Loss / Data Corruption Bugs

These are the most dangerous: incorrect data returned or data silently dropped.

| Ticket | Status | Coverage | Risk |
|--------|--------|----------|------|
| **WT-14806** | Open | UNCOVERED | Layered cursor tombstone marker `"\x14\x14"` can collide with app data values — `__wt_clayered_deleted()` returns WT_NOTFOUND for live records |
| **WT-17272** | Open | UNCOVERED | Fast truncate write-conflict detection misses uncommitted ingest updates via `search_near`; conflicting ops silently commit |
| **WT-17311** | Backlog | PARTIAL | `modify` returns `WT_NOTFOUND` instead of `WT_ROLLBACK` when tombstone cleared by checkpoint; callers treat it as silent no-op |
| **WT-16812** | Open | PARTIAL | Follower fast truncate write-conflict detection is unvalidated — conflicting operations can commit simultaneously |
| **WT-14730** | unavailable | UNCOVERED | Incomplete btree ID conflict check during checkpoint pickup — two tables could share one physical file |
| **WT-14545** | unavailable | PARTIAL | Double leader→follower→leader role-change is invisible to cursor state machine; writes from a stepped-down node may commit |
| **WT-16660** | Open | UNCOVERED | `bytes_total` incremented outside reconciliation boundary in disagg write path; size verification check disabled with `if (false)` |
| **WT-14608** | unavailable | UNCOVERED | Delta pages from `read_multiple` silently dropped; `WT_ASSERT(count == 1)` fires in debug, returns stale data in release |
| **WT-16857** | Needs Scheduling | UNCOVERED | HS cursors from non-cached sessions lack `WT_CURSTD_IGNORE_TOMBSTONE` — all HS records invisible from those sessions |

### 2. Confirmed Data Races (TSAN-Suppressed Instead of Fixed)

These races are real C11 undefined behavior; suppressions mask them from CI.

| Ticket | Status | Coverage | Race |
|--------|--------|----------|------|
| **WT-16310** | Backlog | PARTIAL | Plain writes to `oldest_timestamp`/`stable_timestamp` vs. atomic reads; commit could observe timestamp below stable |
| **WT-16319** | Backlog | PARTIAL | Plain writes to `upd_start_ts`/`upd_durable_ts` vs. relaxed atomic reads in hot read path |
| **WT-15708** | Open | PARTIAL | `log->sync_lsn` written without lock in WAL release hot path; TSAN suppressor masking a real race |
| **WT-16778** | Open | UNCOVERED | `txn_global->has_durable_timestamp` written non-atomically during RTS while companion field uses atomics |

### 3. Disaggregated Storage Correctness Gaps

| Ticket | Status | Coverage | Issue |
|--------|--------|----------|-------|
| **WT-15357** | Open | UNCOVERED | Four skip guards prevent checkpoint-cursor consistency verification from ever running in disagg mode |
| **WT-15189** | Open | UNCOVERED | Active CI blocker (17+ failing variants); `test_layered_fast_truncate05.py` entirely skipped; random cursor ops suppressed in format stress |
| **WT-16467** | Blocked | PARTIAL | Followers can install checkpoints with `oldest_ts` newer than pinned; causes incorrect timestamped reads |
| **WT-15914** | Open | UNCOVERED | Magic constant `WT_DISAGG_START_LSN` (= `1<<32`) injected by MongoDB Server silently disables materialization frontier check during step-up |
| **WT-16789** | In Code Review | PARTIAL | Sweep server can evict layered dhandle while truncate-list entries still hold its URI — crash/assertion on re-acquire |
| **WT-15163** | Open | PARTIAL | Recovery restart requires `lose_all_my_data` flag; missed invocation leaves stale inconsistent local files |
| **WT-14902** | Backlog | UNCOVERED | Enabling precise checkpoint via reconfiguration without prior RTS can commit data beyond stable timestamp |

### 4. Entire Feature Disabled

| Ticket | Status | Coverage | Feature |
|--------|--------|----------|---------|
| **WT-15663** | Backlog | UNCOVERED | **Entire block cache is disabled** — data-aliasing bug in `__wt_buf_set` (`ip == buf` case); `test_layered43` permanently skipped |
| **WT-15768** | Open | UNCOVERED | Infinite retry loop in disagg block read path; blocks programmatic failure detection; fix pending cross-team (SERVER-113585) |
| **WT-15763** | Backlog | PARTIAL | Graceful step-down not implemented; every role change requires full connection restart; 3 test scenarios in `test_layered27.py` commented out |

### 5. Critical Test Coverage Gaps (HIGH importance, UNCOVERED)

Tests skipped or disabled that would catch real bugs:

| Ticket | Status | What's Not Tested |
|--------|--------|-------------------|
| **WT-15064** | Open | Corruption tests disabled for disagg in `test_verify.py`, `test_corrupt01.py`, `hook_disagg.py` — 3 separate skip sites |
| **WT-15040** | Open | Formal model tests exclude prepared transactions — entire prepare/commit/rollback flow unverified by model |
| **WT-15069** | Open | Model crash point `CKPT_CRASH_BEFORE_METADATA_SYNC` with logging enabled bypassed by workaround |
| **WT-16146** | Backlog | Crash during complex/tiered table creation leaves orphaned metadata; restart not covered |
| **WT-16215** | Backlog | `__wt_meta_track_off` called before meta-tracking initialized during recovery; flag-based guard is fragile |
| **WT-13232** | N/A | Model `kv_table.cpp` suppresses detection of truncate spurious `WT_PREPARE_CONFLICT` divergence |

---

## All 34 HIGH-Importance Tickets

| Ticket | Jira Status | Coverage | One-line summary |
|--------|-------------|----------|-----------------|
| WT-13232 | N/A | PARTIAL | Truncate returns spurious WT_PREPARE_CONFLICT; model test suppresses detection |
| WT-14545 | unavailable | PARTIAL | Double role-change undetected in layered cursor state machine |
| WT-14608 | unavailable | UNCOVERED | Delta pages from read_multiple silently ignored; assert or wrong data |
| WT-14713 | unavailable | PARTIAL | Disagg import read failure; root cause unknown; error silenced |
| WT-14730 | unavailable | UNCOVERED | Incomplete btree ID conflict check during checkpoint pickup |
| WT-14806 | Open | UNCOVERED | Tombstone marker collision causes live records to appear deleted |
| WT-14902 | Backlog | UNCOVERED | Precise checkpoint reconfiguration without RTS violates timestamp invariant |
| WT-15040 | Open | UNCOVERED | Formal model test excludes prepared transactions entirely |
| WT-15058 | Open | PARTIAL | Layered cursor ncursors management incorrect; 22% p99 latency regression |
| WT-15064 | Open | UNCOVERED | Corruption detection tests disabled for disagg (3 skip sites) |
| WT-15069 | Open | PARTIAL | Model crash point workaround bypasses logging-enabled crash scenario |
| WT-15163 | Open | PARTIAL | Restart recovery requires destructive lose_all_my_data flag |
| WT-15189 | Open | UNCOVERED | Active CI blocker; fast truncate random cursor test entirely skipped |
| WT-15357 | Open | UNCOVERED | Checkpoint-cursor consistency never verified in disagg mode |
| WT-15663 | Backlog | UNCOVERED | Entire block cache feature disabled due to data-aliasing bug |
| WT-15708 | Open | PARTIAL | TSAN data race on WAL log->sync_lsn in release hot path |
| WT-15709 | Backlog | UNCOVERED | Delta suppressed for split pages in disagg; write amplification |
| WT-15763 | Backlog | PARTIAL | Graceful step-down unimplemented; 3 test scenarios commented out |
| WT-15768 | Open | UNCOVERED | Infinite retry loop in disagg block read path; failure not detectable |
| WT-15914 | Open | UNCOVERED | Magic LSN sentinel silently disables materialization frontier check |
| WT-16067 | Open | PARTIAL | ASAN warnings hidden in parallel Python tests; root cause unknown |
| WT-16146 | Backlog | UNCOVERED | Crash during complex table creation leaves orphaned metadata |
| WT-16215 | Backlog | PARTIAL | Meta-tracking called before init during recovery; flag guard fragile |
| WT-16310 | Backlog | PARTIAL | TSAN race on oldest_timestamp/stable_timestamp; suppressed not fixed |
| WT-16319 | Backlog | PARTIAL | TSAN race on upd_start_ts/upd_durable_ts in hot read path |
| WT-16467 | Blocked | PARTIAL | Follower checkpoint install with newer oldest_ts causes wrong reads |
| WT-16660 | Open | UNCOVERED | Block write accounting bug; bytes_total leaks; size verify disabled |
| WT-16778 | Open | UNCOVERED | Non-atomic write to has_durable_timestamp during RTS |
| WT-16789 | In Code Review | PARTIAL | Sweep race: dhandle evicted while truncate-list entries hold URI |
| WT-16812 | Open | PARTIAL | Follower fast truncate write-conflict detection unvalidated |
| WT-16857 | Needs Scheduling | UNCOVERED | HS cursor from non-cached session sees no records |
| WT-16864 | Open | PARTIAL | Size accounting underflow assertion disabled to suppress CI crashes |
| WT-17272 | Open | UNCOVERED | Fast truncate misses write-conflict with uncommitted ingest updates |
| WT-17311 | Backlog | PARTIAL | modify returns WT_NOTFOUND instead of WT_ROLLBACK; silent data drop |

---

## All 43 UNCOVERED Tickets

Tickets where no test exercises the specific risky code path:

| Ticket | Importance | Summary |
|--------|------------|---------|
| WT-7247 | MEDIUM | (pre-2025; see per-ticket file) |
| WT-8681 | LOW | (pre-2025; see per-ticket file) |
| WT-12192 | LOW | (pre-2025; see per-ticket file) |
| WT-12905 | MEDIUM | (pre-2025; see per-ticket file) |
| WT-12983 | MEDIUM | (pre-2025; see per-ticket file) |
| WT-13706 | MEDIUM | (pre-2025; see per-ticket file) |
| WT-13897 | MEDIUM | (pre-2025; see per-ticket file) |
| WT-14047 | MEDIUM | (pre-2025; see per-ticket file) |
| WT-14223 | MEDIUM | (pre-2025; see per-ticket file) |
| WT-14608 | HIGH | Delta pages silently ignored in disagg read path |
| WT-14612 | MEDIUM | (see per-ticket file) |
| WT-14723 | MEDIUM | (see per-ticket file) |
| WT-14730 | HIGH | Incomplete btree ID conflict check during checkpoint pickup |
| WT-14739 | MEDIUM | Follower shutdown checkpoint not tested |
| WT-14740 | MEDIUM | Salvage operation for disagg storage untested |
| WT-14806 | HIGH | Tombstone marker collision = silent data loss |
| WT-14884 | LOW | (see per-ticket file) |
| WT-14887 | LOW | (see per-ticket file) |
| WT-14902 | HIGH | Precise checkpoint reconfiguration without RTS |
| WT-14939 | MEDIUM | (see per-ticket file) |
| WT-15040 | HIGH | Formal model excludes prepared transactions |
| WT-15064 | HIGH | Corruption tests disabled for disagg (3 sites) |
| WT-15189 | HIGH | CI blocker; fast truncate random cursor test skipped |
| WT-15357 | HIGH | Checkpoint-cursor consistency never verified in disagg |
| WT-15545 | LOW | (see per-ticket file) |
| WT-15565 | MEDIUM | Prepared truncate + precise checkpoint combination untested |
| WT-15663 | HIGH | Block cache disabled; data-aliasing bug untested |
| WT-15709 | HIGH | Delta suppressed for split pages in disagg |
| WT-15754 | MEDIUM | Stat macro atomicity untested |
| WT-15755 | LOW | Max/min stat lost-update race not caught |
| WT-15768 | HIGH | Infinite retry in disagg block read; failure undetectable |
| WT-15865 | LOW | Redundant URI strings in layered table manager |
| WT-15914 | HIGH | Magic LSN disables materialization frontier check |
| WT-15961 | MEDIUM | Statistics macros lack atomic variants; TSAN races suppressed |
| WT-16146 | HIGH | Crash during complex table creation; orphaned metadata |
| WT-16512 | MEDIUM | Reserved page header byte has no validation test |
| WT-16660 | HIGH | Block accounting bug; size verify disabled |
| WT-16778 | HIGH | Non-atomic write to has_durable_timestamp during RTS |
| WT-16787 | LOW | session->name race under concurrent access |
| WT-16857 | HIGH | HS cursor missing IGNORE_TOMBSTONE from non-cached session |
| WT-16918 | MEDIUM | tableExists() always returns False in disagg hook; 4 tests blocked |
| WT-16920 | LOW | URI tracking in disagg hook doesn't distinguish home directories |
| WT-17272 | HIGH | Fast truncate misses write-conflict with uncommitted ingest updates |

---

## By Subsystem

### Disaggregated / Layered Storage
Largest concentration of HIGH findings.  
HIGH: WT-14545, WT-14608, WT-14713, WT-14730, WT-14806, WT-14902, WT-15040, WT-15064, WT-15163, WT-15189, WT-15357, WT-15663, WT-15709, WT-15763, WT-15768, WT-15914, WT-16467, WT-16660, WT-16789, WT-16812, WT-16857, WT-16864, WT-17272

### Concurrency / TSAN-Suppressed Races
WT-15708, WT-15752, WT-15754, WT-15755, WT-15961, WT-16310, WT-16319, WT-16778

### Checkpoint / RTS
WT-14902, WT-15189, WT-15357, WT-16215, WT-16228, WT-16467

### Block Cache
WT-15663 (entire feature disabled), WT-14608 (delta read path)

### Model / Formal Verification
WT-13232, WT-15040, WT-15069, WT-14863

### Recovery / Crash Restart
WT-15163, WT-16146, WT-16215

### History Store
WT-16857, WT-17311, WT-16136

---

## Recommended Action Priority

1. **Fix or unblock before GA**: WT-14806, WT-17272, WT-16812, WT-17311, WT-15189, WT-16310, WT-16319, WT-16778 — all are data loss or confirmed data races with no mitigation beyond a TSAN suppressor.

2. **Schedule promptly**: WT-15663 (block cache), WT-15914 (magic LSN), WT-16660 (accounting bug), WT-15357 (no checkpoint verify in disagg), WT-15064 (no corruption tests for disagg).

3. **Track with tests**: WT-14902, WT-15040, WT-16146, WT-14730 — correctness gaps where a test would catch the defect but none exists today.
