# DisAgg Disabled Tests Analysis

**Generated:** 2026-05-06  
**Branch:** wt-17224-tests-analysis  
**Purpose:** Comprehensive inventory of all tests disabled for DisAgg, including root causes and Jira ticket status.

---

## How Tests Are Disabled for DisAgg

There are three distinct mechanisms:

### Mechanism 1: `@wttest.skip_for_hook("disagg", ...)` decorator
Applied at the class or method level in individual test files. The decorator is active whenever `--hook disagg` is passed. Defined in [test/suite/wttest.py:1111](test/suite/wttest.py#L1111).

### Mechanism 2: Category-based skips in `hook_disagg.py`
The `should_skip()` method in [test/suite/hook_disagg.py:369](test/suite/hook_disagg.py#L369) skips entire test files whose names match certain substrings. Applied at test registration time.

### Mechanism 3: `hook_disagg.fail` file
[test/suite/hook_disagg.fail](test/suite/hook_disagg.fail) — a plain list of test files that Evergreen excludes from disagg runs. These are known failures that haven't been fully triaged or fixed yet.

---

## Mechanism 2: Category-Based Skips in `hook_disagg.py`

These skip entire test files whose names contain the listed substring:

| Pattern | Reason | Notes |
|---------|--------|-------|
| `disagg` | Disagg tests already turn on the proper stuff | Prevents double-enabling |
| `inmem` | In-memory tests don't make sense with disagg storage | Fundamental incompatibility |
| `layered` | Layered tests already turn on the proper stuff | Prevents double-enabling |
| `live_restore` | Live restore is not supported with disagg storage | Architectural incompatibility |
| `lsm` | LSM is not supported with tiering | LSM/disagg incompatibility |
| `modify_smoke_recover` | Copying WT dir doesn't copy the PALite directory | Test relies on local file copy |
| `rollback_to_stable` | Rollback to stable is not needed at startup | RTS is a no-op in disagg |
| `test_backup` | Can't backup a disagg table | Backup not implemented for disagg |
| `test_compact` | Can't compact a disagg table | Compact not implemented for disagg |
| `test_config_json` | Disagg hook's create function can't handle JSON config string | Hook limitation |
| `test_cursor_big` | Cursor caching verified with stats | Stats behave differently in disagg |
| `test_cursor_bound` | Can't use cursor bounds with a disagg table | Not implemented |
| `test_import` | Can't import a disagg table | Not implemented |
| `test_salvage` | Salvage is not currently supported for disagg | FIXME-WT-14740 (Backlog, P4) |
| `tiered` | Tiered tests do not apply to disagg | Different storage backend |

Additionally, the `wiredtiger_open_replace` function in `hook_disagg.py` automatically skips at runtime when:
- `disaggregated=` already in conn_config (double-enable guard)
- `DisaggConfigMixin` already present (same)
- `in_memory=true` in conn_config
- `readonly=true` in conn_config (FIXME-WT-17177)
- `compatibility=` in conn_config
- `tiered_storage=` in conn_config

And `session_create_replace` skips when:
- Log tables are enabled at connection level
- `import=(enabled` in config

---

## Mechanism 1: `@wttest.skip_for_hook("disagg", ...)` Decorators

### Checkpoint Tests — Named Checkpoints Not Supported

All of the following are skipped because **layered trees do not support named checkpoints**.

| File | Line | Method/Class |
|------|------|-------------|
| [test_checkpoint01.py](test/suite/test_checkpoint01.py) | 39, 124, 156, 239, 265, 332 | Multiple classes |
| [test_checkpoint10.py](test/suite/test_checkpoint10.py) | 41 | Whole class |
| [test_checkpoint11.py](test/suite/test_checkpoint11.py) | 41 | Whole class |
| [test_checkpoint13.py](test/suite/test_checkpoint13.py) | 42 | Whole class |
| [test_checkpoint14.py](test/suite/test_checkpoint14.py) | 42 | Whole class |
| [test_checkpoint16.py](test/suite/test_checkpoint16.py) | 40 | Whole class |
| [test_checkpoint17.py](test/suite/test_checkpoint17.py) | 40 | Whole class |
| [test_checkpoint18.py](test/suite/test_checkpoint18.py) | 48 | Whole class |
| [test_checkpoint19.py](test/suite/test_checkpoint19.py) | 47 | Whole class |
| [test_checkpoint20.py](test/suite/test_checkpoint20.py) | 39 | Whole class |
| [test_checkpoint21.py](test/suite/test_checkpoint21.py) | 59 | Whole class |
| [test_checkpoint22.py](test/suite/test_checkpoint22.py) | 75 | Whole class |
| [test_checkpoint24.py](test/suite/test_checkpoint24.py) | 41 | Whole class |
| [test_checkpoint25.py](test/suite/test_checkpoint25.py) | 41 | Whole class |
| [test_checkpoint27.py](test/suite/test_checkpoint27.py) | 41 | Whole class |
| [test_checkpoint28.py](test/suite/test_checkpoint28.py) | 49 | Whole class |
| [test_checkpoint29.py](test/suite/test_checkpoint29.py) | 35 | Whole class |
| [test_checkpoint31.py](test/suite/test_checkpoint31.py) | 35 | Whole class |
| [test_cc05.py](test/suite/test_cc05.py) | 37 | Whole class |

**Root cause:** Named checkpoints use `name=` in checkpoint config. The disagg hook's `session_checkpoint_replace` explicitly detects this and calls `skip_test('named checkpoints do not work in disagg storage')`. This is a permanent limitation — disagg uses precise checkpoints, and naming them conflicts with the shared storage model.

---

### Checkpoint Cursor Tests

| File | Line | Reason |
|------|------|--------|
| [test_bug010.py](test/suite/test_bug010.py) | 46 | "layered trees do not support opening checkpoint cursors" |
| [test_checkpoint12.py](test/suite/test_checkpoint12.py) | 79 | "layered trees do not support opening checkpoint cursors" |
| [test_cursor13.py](test/suite/test_cursor13.py) | 141–177 | "disagg doesn't support opening checkpoint cursor" (7 methods) |

**Root cause:** `session_open_cursor_replace` in `hook_disagg.py` skips if `checkpoint=` is in cursor config. Checkpoint cursors on layered tables are not implemented.

Additionally `test_bug010.py` has a deeper issue:

> **WT-16532** (Open, P3, SE-Transactions) — "Investigate failure from bug010"  
> The test was disabled in WT-16458 after a hard-to-reproduce failure. The test also uses backup cursors (unsupported in disagg). Status: Open, unassigned, sprint 2026-06-19.

---

### Bulk Load Tests

| File | Line | Reason |
|------|------|--------|
| [test_checkpoint_snapshot05.py](test/suite/test_checkpoint_snapshot05.py) | 88 | "layered trees do not yet support bulk loading" (FIXME-WT-14563) |
| [test_timestamp05.py](test/suite/test_timestamp05.py) | 68 | "bulk load is not currently supported for layered cursors" |

**Root cause:** `session_open_cursor_replace` skips if `bulk` is in cursor config.

> **WT-14563** (Open, P3, SE-unassigned) — "Support bulk load for layered cursors"  
> Bulk load should work at least in leader mode. Proposed: open stable cursor with bulk=true in leader mode, ingest cursor with bulk=true in follower mode. Status: Open, in defined pipeline. Last comment (D. Anderson, Sep 2025) proposes simple minimal implementation.

---

### Salvage Tests

| File | Line | Reason |
|------|------|--------|
| [test_prepare_hs03.py](test/suite/test_prepare_hs03.py) | 44 | "Salvage on disagg tables not yet implemented" (FIXME-WT-14740) |

> **WT-14740** (Backlog, P4, SE-Foundations) — "Clarify how salvage works in disaggregated storage"  
> Original: block manager interface changed to take `WT_PAGE_BLOCK_META*`, passed as NULL in salvage code in `bt_slvg.c`. Need to define salvage semantics for disagg first. Status: Backlog, low priority.

---

### Rollback-to-Stable (RTS) Tests

Disagg does not use RTS at startup — this is a fundamental design difference.

| File | Line | Reason |
|------|------|--------|
| [test_checkpoint_snapshot03.py](test/suite/test_checkpoint_snapshot03.py) | 42 | "rollback to stable not expected to run on disagg; cache unable to evict pages due to invisible updates" |
| [test_checkpoint_snapshot02.py](test/suite/test_checkpoint_snapshot02.py) | 42 | "Disagg requires precise checkpoint which does not work well with small cache size" |
| [test_prepare29.py](test/suite/test_prepare29.py) | 36 | "This test relies on RTS, which is not used in disagg" |
| [test_prepare_hs04.py](test/suite/test_prepare_hs04.py) | 38 | "This test relies on RTS, which is not used in disagg" |
| [test_truncate09.py](test/suite/test_truncate09.py) | 37 | "Disagg does not support RTS" |

---

### Read-Only Connection Tests

| File | Line | Reason |
|------|------|--------|
| [test_truncate15.py](test/suite/test_truncate15.py) | 38 | "readonly connections not supported yet" (FIXME-WT-14582) |
| [test_util23.py](test/suite/test_util23.py) | 35 | "Disaggregated storage does not support read-only connections" (FIXME-WT-17177) |

> **WT-14582** (Backlog, P4, SE-Foundations) — "Add support for readonly connections for disagg"  
> Root cause: picking up a checkpoint during wiredtiger_open requires modifying the shared metadata file, which is not a read-only operation. Workaround needed: temporarily disable readonly flag for metadata updates. Status: Backlog. D. Anderson comment (Aug 2025): "low priority until we hear otherwise."

> **WT-17177** (Backlog, P3) — "Investigate whether support for read-only connections in disagg is needed"  
> Created after WT-17143 explicitly disabled read-only connections. Status: Backlog, unassigned. Note: `wt -r` is used in tutorials and internal tooling — if we decide not to support it, documentation must be updated.

---

### Verify/Corrupt Tests

| File | Line | Reason |
|------|------|--------|
| [test_corrupt01.py](test/suite/test_corrupt01.py) | 39 | "Verify is not supported with disaggregated storage (yet)" |
| [test_verify.py](test/suite/test_verify.py) | 84, 107 | "We cannot access shared tables data directly" |

> **WT-15064** (Open, P3, SE-Foundations+Persistence) — "Add table corruptions detection test cases for DisAgg tables verification"  
> Existing corruption tests write invalid data to local files, which doesn't work for disagg shared tables stored in PAL. Two approaches proposed: (1) PALM Python wrapper to overwrite pages, (2) custom C function in PALM. Status: Open, unassigned.

---

### Log / Logging Tests

| File | Line | Reason |
|------|------|--------|
| [test_schema09.py](test/suite/test_schema09.py) | 36 | "log tables is not supported on disagg" |
| [test_timestamp03.py](test/suite/test_timestamp03.py) | 41 | "log tables are disabled in disagg so this test will fail" |
| [test_txn15.py](test/suite/test_txn15.py) | 38 | "this test checks logging stats, which are not relevant for disagg tables" |
| [test_txn19.py](test/suite/test_txn19.py) | 60 | "corrupts log files, which are not relevant for disagg" |

**Root cause:** Log tables (`session.create` with `log=(enabled)` in conn config) are not supported in disagg. The hook detects this and skips.

---

### Sweep Tests

| File | Line | Reason |
|------|------|--------|
| [test_sweep01.py](test/suite/test_sweep01.py) | 39 | "Disagg doesn't sweep layered dhandles" |
| [test_sweep03.py](test/suite/test_sweep03.py) | 151 | "Fails with disagg" (FIXME-WT-16757) |
| [test_sweep06.py](test/suite/test_sweep06.py) | 38 | "This test uses multiple threads, which is incompatible with the disagg hook" |

> **WT-16757** (Open, P3, SE-Foundations+Persistence, assigned to Jie Chen) — "Investigate sweep03 error when queuing drops"  
> After WT-16565, metadata removal is queued to the upcoming checkpoint. The python sweep03 fails saying cache size increased after dropping. Only happens with release build and with table prefix. Root cause: metadata queued for removal inflates cache temporarily before being cleared at next checkpoint. Status: Open, story points: 2.

---

### Eviction Tests

| File | Line | Reason |
|------|------|--------|
| [test_eviction04.py](test/suite/test_eviction04.py) | 36 | "Fails due to evict a page" |
| [test_eviction05.py](test/suite/test_eviction05.py) | 35 | "Fails due to evict a page" |
| [test_rollback01.py](test/suite/test_rollback01.py) | 33 | "disagg requires an additional condition to evict pages" |

**Root cause:** Disagg eviction semantics differ — row-store tombstones remain in the update chain after eviction (WT-14937 pre-existing limitation). Tests that verify specific eviction behavior will see different counts.

---

### Cursor Behavior Tests

| File | Line | Reason |
|------|------|--------|
| [test_cursor13.py](test/suite/test_cursor13.py) | 325, 595 | "layered cursor don't support duplicate cursors" |
| [test_error_info02.py](test/suite/test_error_info02.py) | 38 | "Fails due to incorrect cursor logic" |

---

### Miscellaneous

| File | Line | Reason | Ticket |
|------|------|--------|--------|
| [test_bug024.py](test/suite/test_bug024.py) | 42 | "Moving the turtle file makes no sense with disaggregated storage" | — |
| [test_checkpoint_snapshot05.py](test/suite/test_checkpoint_snapshot05.py) | 88 | Bulk loading | WT-14563 |
| [test_config06.py](test/suite/test_config06.py) | 89 | "This case is DSC specified" | — |
| [test_metadata_cursor02.py](test/suite/test_metadata_cursor02.py) | 38 | "Test is specific to attached storage" | — |
| [test_truncate19.py](test/suite/test_truncate19.py) | 60 | "test depends on sizes of associated file objects" | — |
| [test_verbose05.py](test/suite/test_verbose05.py) | 41 | "Checkpoint progress output is different under disagg" | — |

---

## Mechanism 3: `hook_disagg.fail` File

These test files are completely excluded from Evergreen disagg runs. Status column reflects Jira ticket status as of 2026-05-06.

| Test File | FIXME Ticket | Ticket Summary | Ticket Status | Notes |
|-----------|-------------|----------------|---------------|-------|
| `test_autoclose.py` | — | Unknown failure | No ticket | Untriaged |
| `test_checkpoint06.py` | WT-15507 | test_checkpoint06 python file disagg segfault | **CLOSED/Fixed** | ⚠️ **Stale entry** — ticket fixed Apr 2026, should be removed |
| `test_config02.py` | — | Unknown failure | No ticket | Untriaged |
| `test_config09.py` | — | Unknown failure | No ticket | Untriaged |
| `test_cursor13.py` | WT-15369 | Fix cursor13/cursor21 stats check failure | **Open**, In Code Review (PR reverted Apr 2026) | Active work |
| `test_cursor21.py` | WT-15369 | (same as above) | **Open**, In Code Review | Cursor cache stat mismatch |
| `test_cursor_random.py` | WT-15189 | Disagg tests time out in `clayered_next_random` | **Open**, P3 | Root cause identified: random-pick loop spins on tombstoned rows. PR open. |
| `test_drop03.py` | — | Unknown failure | No ticket | Untriaged |
| `test_dump.py` | — | Unknown failure | No ticket | Untriaged |
| `test_dump01.py` | — | Unknown failure | No ticket | Untriaged |
| `test_dump02.py` | — | Unknown failure | No ticket | Untriaged |
| `test_dump03.py` | — | Unknown failure | No ticket | Untriaged |
| `test_dump04.py` | — | Unknown failure | No ticket | Untriaged |
| `test_dump05.py` | — | Unknown failure | No ticket | Untriaged |
| `test_dupc.py` | — | Unknown failure | No ticket | Untriaged |
| `test_durable_ts01.py` | WT-15370 | Disagg: fix test_durable_ts01.py | **CLOSED/Fixed** May 2026 | ⚠️ **Stale entry** — ticket fixed May 6 2026, should be removed |
| `test_durable_ts03.py` | — | Unknown failure | No ticket | Untriaged |
| `test_empty.py` | — | Unknown failure | No ticket | Untriaged |
| `test_encrypt06.py` | — | Unknown failure | No ticket | Untriaged |
| `test_env01.py` | — | Unknown failure | No ticket | Untriaged |
| `test_error_info01.py` | — | Unknown failure | No ticket | Untriaged |
| `test_error_info03.py` | WT-16872 | Tests fail in is_layered() check from spawned threads | **In Code Review** | 4 tests affected; fix: use `wtthread.Thread` |
| `test_hs01.py` | WT-15371 | Disagg: fix test_hs01.py | **CLOSED/Fixed** May 2026 | ⚠️ **Stale entry** — ticket fixed May 6 2026, should be removed |
| `test_hs24.py` | WT-16872 | (same as error_info03) | **In Code Review** | Spawned thread / is_layered() |
| `test_hs_evict_race01.py` | WT-16872 | (same as error_info03) | **In Code Review** | Spawned thread / is_layered() |
| `test_log03.py` | — | Unknown failure | No ticket | Untriaged |
| `test_metadata_cursor01.py` | — | Unknown failure | No ticket | Untriaged |
| `test_metadata_cursor04.py` | — | Unknown failure | No ticket | Untriaged |
| `test_prepare28.py` | WT-16872 | (same as error_info03) | **In Code Review** | Spawned thread / is_layered() |
| `test_readonly01.py` | — | Unknown failure | No ticket | Untriaged |
| `test_readonly03.py` | WT-14582 | Add support for readonly connections for disagg | **Backlog**, P4 | Root cause documented: metadata write needed during checkpoint pick-up |
| `test_shared_cache01.py` | — | Unknown failure | No ticket | Untriaged |
| `test_stat01.py` | — | Unknown failure | No ticket | Untriaged |
| `test_stat_log02.py` | — | Unknown failure | No ticket | Untriaged |
| `test_sweep05` | — | Unknown failure | No ticket | Untriaged |
| `test_timestamp26.py` | WT-16182 | test_timestamp26.py failed in disagg non-diagnostic build | **CLOSED/Fixed** Apr 2026 | ⚠️ **Stale entry** — ticket fixed Apr 24 2026, should be removed |
| `test_truncate01.py` | WT-15474 | truncate01 python failure: `__wt_cursor_kv_not_set` (follower mode) | **CLOSED/Won't Fix** | ⚠️ **Stale entry** — re-enabled via WT-17328 |
| `test_util01.py` | WT-16918 | Implement `tableExists()` for disagg python tests | **Open**, P3 | Blocked on `tableExists()` implementation |
| `test_util02.py` | — | Unknown failure | No ticket | Untriaged |
| `test_util04.py` | WT-16918 | (same as above) | **Open**, P3 | Blocked on `tableExists()` |
| `test_util07.py` | — | Unknown failure | No ticket | Untriaged |
| `test_util09.py` | — | Unknown failure | No ticket | Untriaged |
| `test_util11.py` | — | Unknown failure | No ticket | Untriaged |
| `test_util12.py` | — | Unknown failure | No ticket | Untriaged |
| `test_util13.py` | — | Unknown failure | No ticket | Untriaged |
| `test_util14.py` | WT-16918 | (same as above) | **Open**, P3 | Blocked on `tableExists()` |
| `test_util15.py` | WT-16918 | (same as above) | **Open**, P3 | Blocked on `tableExists()` |
| `test_util17.py` | WT-16918 | (same as above) | **Open**, P3 | Blocked on `tableExists()` |
| `test_verbose01.py` | WT-15372 | Disagg: Fix test_verbose01.py | **Open**, Backlog | Extra verbose message from disagg logic causes assertion failure |
| `test_verbose02.py` | — | Unknown failure | No ticket | Untriaged |
| `test_verbose04.py` | — | Unknown failure | No ticket | Untriaged |

---

## Open Tickets Summary (as of 2026-05-06)

| Ticket | Summary | Status | Priority | Assigned Team |
|--------|---------|--------|----------|---------------|
| WT-14563 | Support bulk load for layered cursors | Open | P3 | SE (unassigned) |
| WT-14582 | Add support for readonly connections for disagg | Backlog | P4 | SE-Foundations |
| WT-14740 | Clarify how salvage works in disaggregated storage | Backlog | P4 | SE-Foundations |
| WT-15064 | Add corruption detection test cases for DisAgg verification | Open | P3 | SE-Foundations+Persistence |
| WT-15189 | Disagg tests time out in `clayered_next_random` | Open | P3 | SE-Foundations (unassigned) |
| WT-15369 | Fix cursor13/cursor21 stats check failure | Open | P3 | SE-Foundations (A. Blekhman) |
| WT-15372 | Fix test_verbose01.py | Open | P3 | SE-Foundations (unassigned) |
| WT-16532 | Investigate failure from bug010 | Open | P3 | SE-Transactions |
| WT-16757 | Investigate sweep03 error when queuing drops | Open | P3 | SE-Foundations (Jie Chen) |
| WT-16872 | Fix tests that fail in is_layered() check | In Code Review | P3 | SE-Foundations (D. Anderson) |
| WT-16918 | Implement `tableExists()` for disagg python tests | Open | P3 | SE-Foundations |
| WT-16920 | Support per home directory URI tracking for disagg | Backlog | P4 | SE-Foundations |
| WT-17177 | Investigate whether read-only connections are needed | Backlog | P3 | SE-Foundations |

---

## Stale Entries in `hook_disagg.fail` (Should Be Removed)

The following entries in `hook_disagg.fail` have corresponding tickets that are now **Closed/Fixed** and should be removed from the fail list:

| Test File | Ticket | Closed Date | Notes |
|-----------|--------|-------------|-------|
| `test_checkpoint06.py` | WT-15507 | Apr 24, 2026 | Fixed — segfault in RTS call during checkpoint |
| `test_durable_ts01.py` | WT-15370 | May 6, 2026 | Fixed — `skip_for_hook` added to test itself |
| `test_hs01.py` | WT-15371 | May 6, 2026 | Fixed — `WT_IS_DISAGG_META` guard in `evict_file.c` |
| `test_timestamp26.py` | WT-16182 | Apr 24, 2026 | Fixed — `session.create` with alter failing in non-diagnostic builds |
| `test_truncate01.py` | WT-15474 | Apr 30, 2026 | Closed/Won't Fix — re-enabled via WT-17328 |

---

## FIXMEs in `hook_disagg.py` Itself

These are open infrastructure tickets for the hook, not specific test failures:

| FIXME | Ticket | Status | Description |
|-------|--------|--------|-------------|
| `FIXME-WT-14740` | WT-14740 | Backlog P4 | `session_salvage_replace` always skips; implement salvage |
| `FIXME-WT-17177` | WT-17177 | Backlog P3 | Read-only connections explicitly disabled; investigate if needed |
| `FIXME-WT-16920` | WT-16920 | Backlog P4 | URI tracking may break with multiple connections on multiple home dirs |
| `FIXME-WT-16918` | WT-16918 | Open P3 | `tableExists()` returns False unconditionally; needs PALite kv_home scan |
| `FIXME-WT-15064` | WT-15064 | Open P3 | `initialFileName()` returns None; no equivalent for PALite |

---

## Statistics

- **Category-based skips in hook_disagg.py:** 15 patterns (whole test files/suites)
- **`@skip_for_hook` decorator skips:** ~65 individual test methods/classes across 36 files
- **`hook_disagg.fail` entries:** 55 test files (of which ~5 are stale/should be removed)
- **Open tickets blocking re-enablement:** 13 tickets
- **Closed tickets with stale fail entries:** 5 entries

---

## How to Re-Produce This Analysis

To regenerate or extend this analysis, a future Claude instance should:

1. **Collect `@skip_for_hook` usages:**
   ```bash
   grep -r "skip_for_hook.*disagg" test/suite --include="*.py" -n | sort
   ```

2. **Read category skips:**  
   Read `test/suite/hook_disagg.py` — the `should_skip()` method lists all category patterns.

3. **Read fail file:**  
   Read `test/suite/hook_disagg.fail` directly.

4. **Find FIXME tickets:**
   ```bash
   grep -r "FIXME.*disagg\|FIXME-WT" test/suite/hook_disagg.py
   grep -r "FIXME-WT" test/suite --include="*.py" | grep -i disagg
   ```

5. **Check Jira ticket status** via MCP tool `jira_get_issue` for each WT-XXXXX ticket found.

6. **Check git history** for context on when tests were disabled:
   ```bash
   git log --oneline --all --grep="disagg" --since="2024-01-01"
   git show <commit-hash> --name-only
   ```

7. **Key files to read:**
   - `test/suite/hook_disagg.py` — all three skip mechanisms originate here
   - `test/suite/hook_disagg.fail` — Evergreen-level skip list
   - `test/suite/wttest.py:1111` — `skip_for_hook` definition
   - `test/suite/wthooks.py` — hook infrastructure
