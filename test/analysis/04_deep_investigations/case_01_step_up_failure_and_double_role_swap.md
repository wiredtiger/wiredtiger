# Case 01: Step-up Failure Path and Double Role Swap (WT_BTREE_READONLY)

**Priority:** CRITICAL  
**Source files:** `src/conn/conn_layered.c:1256–1313, 1316–1356, 1461–1479, 1586–1593`  
**Related FIXMEs:** FIXME-WT-14545 (test_layered26), FIXME-WT-14734 (drain lock race)  
**Status:** Two distinct sub-cases found; one partially confirmed as a real bug

---

## Sub-case A: Step-up failure → unconditional `__wt_panic`

### Code path

```
reconfigure('disaggregated=(role="leader")')
  → __wti_disagg_conn_config (conn_layered.c:1406)
    → line 1461: was_leader = conn->layered_table_manager.leader  (was false)
    → line 1464: WT_WITH_CHECKPOINT_LOCK(session, ret = __disagg_step_up(session))
      → line 1285: conn->layered_table_manager.leader = true   ← SET FIRST
      → line 1293: WT_ERR(__disagg_restart_checkpoint(session))    ← can fail
      → line 1302: WT_ERR_MSG_CHK(__layered_create_missing_stable_tables)  ← can fail
      → line 1306: WT_ERR_MSG_CHK(__wti_layered_drain_ingest_tables)       ← can fail
      → err: F_CLR(conn, WT_CONN_RECONFIGURING_STEP_UP)  ← clears reconfigure flag
             return ret                                   ← but leader=true NOT cleared
    → line 1466: WT_ERR_MSG_CHK(session, ret, "Failed to step up to the leader role")
      → falls to err: label
    → line 1591: if (ret != 0 && reconfig && !was_leader && leader)
    → line 1592:     return (__wt_panic(session, ret, "failed to step-up as primary"))
```

### What happens on failure

`__wt_panic` is called unconditionally for any step-up failure, regardless of whether the failure is transient (temporary I/O error, lock contention) or permanent (corrupt page log). The connection is **permanently destroyed** — it cannot be used again.

The `leader = true` flag that was set before the error is moot since panic destroys the connection, but the design choice to panic rather than allow retry is load-bearing.

### What is currently tested

The `reconfigure(role="leader")` call is exercised in 20+ tests. **None inject a fault** during the step-up operations. All tests assume step-up succeeds.

The three failable operations inside step-up:
- `__disagg_restart_checkpoint` — calls `__disagg_abandon_checkpoint` (FIXME-WT-16524: `pl_abandon_checkpoint` is a no-op for some backends) and `__disagg_begin_checkpoint`
- `__layered_create_missing_stable_tables` — acquires schema lock, creates missing stable btrees from metadata
- `__wti_layered_drain_ingest_tables` — drains ingest btrees into stable btrees

No test simulates any of these failing.

### What is missing

1. Verify the `__wt_panic` behavior is correct and observable (e.g., the connection becomes unusable, subsequent calls return `WT_PANIC`)
2. Verify that transient failures (e.g., page log temporarily unavailable during `__disagg_begin_checkpoint`) result in the expected panic behavior (or consider whether retry should be allowed)
3. Verify MongoDB error handling responds correctly to a panic from `reconfigure`

---

## Reviewer Discussion — Round 1

> **Reviewer:** Every layered cursor (`cur_layered.c`) should reopen the stable table constituent without `WT_BTREE_READONLY`, so given that could it still lead to any problems? And I don't see how `test_layered27.py` covers leader→follower→leader.

**On test_layered27:** Correct. `test_drain_insert_remove_within_same_transaction` creates the table as leader then immediately steps down at line 190 — it never performs any leader-mode writes before the step_down. So no meaningful leader state exists when the transition happens. More critically, after step_up at line 229 the test does not verify data correctness — it only calls `checkpoint()` and `close()`. This is not a true leader→follower→leader coverage test.

**On layered cursor reopen and READONLY:** `cur_layered.c`'s `__clayered_adjust_state` does detect role changes and triggers `__clayered_advance_stable → __clayered_open_stable(leader=true)`, which opens the live stable table URI without a checkpoint suffix and without `readonly=true` in config. However, **closing a cursor does not close the underlying dhandle** — it only decrements the refcount. The live `file:foo.wt_stable` dhandle that step_down marked READONLY remains in `conn->dhqh` as open. When `__clayered_open_stable(leader=true)` calls `__wt_open_cursor(session, stable_uri, ...)`, it goes through `__wt_session_get_dhandle` which at line 944-946 (session_dhandle.c) returns any already-OPEN dhandle immediately without re-opening: `if (F_ISSET(dhandle, WT_DHANDLE_OPEN) && !LF_ISSET(WT_BTREE_SPECIAL_FLAGS)) break;` — `WT_BTREE_SPECIAL_FLAGS = BULK | SALVAGE | VERIFY` (does NOT include READONLY), so the existing READONLY-flagged dhandle is returned as-is.

**Remaining problems despite reopen:**
1. The drain path in `conn_layered_ingest.c` opens the stable btree **directly** via `__layered_table_get_constituent_cursor` (not through the layered cursor mechanism), so `__clayered_open_stable`'s logic doesn't apply to drain at all.
2. Even user-facing layered cursors that go through `__clayered_open_stable` get back the same READONLY dhandle, so their writes also silently fail.
3. `__clayered_adjust_state` has **FIXME-WT-14545 on line 515**: *"What is not checked here is the possibility that a step down and step up have both occurred since the last check. We don't have a way to detect that (or its opposite) at the moment."* — double transitions are explicitly undetectable.

**Why `WT_BTREE_DISAGGREGATED` matters:** `bt_handle.c:626` sets this flag only on btrees whose name contains `.wt_stable`. Step_down's READONLY sweep (conn_layered.c:1330–1351) only marks btrees with `WT_BTREE_DISAGGREGATED` — confirming it exclusively targets live stable btrees. The ingest btrees are unaffected.

---

## Reviewer Discussion — Round 2

> **Reviewer:** The thing is that only stable btrees can be disaggregated btrees, so drain won't be affected. And I am doubting that by requesting a dhandle without READONLY we can get a dhandle with this flag. Can you recheck this?

Both observations point to the same underlying mechanism: **`__disagg_apply_checkpoint_meta` marks live stable btrees as `WT_DHANDLE_OUTDATED`**.

### The OUTDATED mechanism (conn_layered.c:377–385)

Inside `__disagg_apply_checkpoint_meta` (called from `__disagg_pick_up_checkpoint` — the follower checkpoint pickup path):

```c
/*
 * Mark all live btrees as outdated. Otherwise, we will not open a new dhandle for live
 * btrees after step-up.
 *
 * TODO: This is better done at step-up or step-down to force close all live btrees.
 */
WT_WITHOUT_DHANDLE(session, ret = __wti_conn_dhandle_outdated(session, metadata_key));
```

When a follower picks up a checkpoint from the page log, it marks the live stable btree's dhandle (`file:foo.wt_stable`) as `WT_DHANDLE_OUTDATED`. Once outdated:

1. `__wt_conn_dhandle_find` at line 295-308 **skips** the OUTDATED dhandle (returns `WT_NOTFOUND`)
2. A **new** dhandle is allocated and opened via `__wt_conn_dhandle_open` → `__wt_btree_open`
3. `__wt_btree_open` at line 192: `F_CLR(btree, ~WT_BTREE_SPECIAL_FLAGS)` — clears ALL btree flags except BULK/SALVAGE/VERIFY
4. **`WT_BTREE_READONLY` is cleared** on the fresh btree

So for the common case where a follower has operated for a checkpoint cycle before stepping up again, the READONLY dhandle has already been replaced by the OUTDATED mechanism, and drain / layered cursors both get a fresh non-READONLY dhandle.

### When the bug still applies

The OUTDATED mechanism is triggered only by `__disagg_pick_up_checkpoint` — i.e., when the follower actually receives and applies a checkpoint from the page log. The bug remains in the case of **a fast step_down → step_up without any intervening checkpoint pickup**:

- step_down: `WT_BTREE_READONLY` set on stable btrees, `leader=false`
- step_up (immediately, before any page-log checkpoint is processed): `leader=true`, drain called
- Stable btrees are OPEN (not OUTDATED), READONLY still set
- `__wt_conn_dhandle_find` returns the READONLY dhandle (OUTDATED check at line 295 does not apply)
- `__wt_session_get_dhandle` line 944-946: dhandle is OPEN, no SPECIAL_FLAGS → returns READONLY dhandle immediately
- Drain writes silently no-op; checkpoint skips READONLY btrees; **data lost**

The TODO comment at line 381 explicitly acknowledges this gap: *"This is better done at step-up or step-down to force close all live btrees."*

### Corrected scope of the bug

| Scenario | READONLY issue? |
|---|---|
| leader → step_down → (follower picks up ≥1 checkpoint) → step_up | **No** — OUTDATED mechanism clears the flag |
| leader → step_down → step_up immediately (no checkpoint pickup) | **Yes** — READONLY dhandle returned, drain writes lost |

The test gap is therefore the **fast handoff** path: a node steps down and steps back up on the same connection without any follower checkpoint activity. This is exactly what would happen in a rolling-restart test or in a test that immediately calls step_up after step_down (as test_layered27 does, without data verification).

---

## Sub-case B: WT_BTREE_READONLY not cleared on second step-up (same connection)

### Code path: Step-down sets READONLY

`__disagg_step_down` → `__disagg_mark_btrees_readonly_then_step_down` (conn_layered.c:1316–1356):

```c
for each open dhandle in conn->dhqh:
    if btree has WT_BTREE_DISAGGREGATED and NOT WT_BTREE_READONLY:
        F_SET(btree, WT_BTREE_READONLY)   // ← set on all open stable btrees
conn->layered_table_manager.leader = false
```

### Code path: Step-up does NOT clear READONLY

`__disagg_step_up` (conn_layered.c:1256–1313):
```c
conn->layered_table_manager.leader = true
__disagg_restart_checkpoint(session)
__layered_create_missing_stable_tables(internal_session)
__wti_layered_drain_ingest_tables(internal_session)   // writes to stable btree
// ← NO code clears WT_BTREE_READONLY on any btree
```

### What READONLY prevents (btree_inline.h:930–940, 1044–1052)

```c
// In __wt_page_modify_set / dirty marking functions:
if (F_ISSET(btree, WT_BTREE_READONLY))
    return;    // ← SILENT early return, no error, page NOT marked dirty
// ... (code to mark page dirty never reached)
```

Also in `checkpoint_txn.c:403`:
```c
if (F_ISSET(btree, WT_BTREE_NO_CHECKPOINT | WT_BTREE_IN_MEMORY | WT_BTREE_READONLY) || ...)
    continue;  // ← READONLY btrees are skipped entirely by checkpoint
```

And in `bt_page.c:1099` (debug builds only):
```c
WT_ASSERT(session, !F_ISSET(btree, WT_BTREE_READONLY));  // ← fires in debug mode
```

### The bug sequence (fast handoff — no intermediate checkpoint pickup)

On a single connection doing `leader → follower → leader` **without** the follower picking up any page-log checkpoint between the two role changes:

1. First step-up: stable btrees opened fresh, `READONLY` not set
2. Step-down: stable btrees get `READONLY=true` (correct — prevents dirty pages during follower read)
3. Node operates as follower but does **not** receive/apply any checkpoint from the page log
   - If it had, `__disagg_apply_checkpoint_meta` → `__wti_conn_dhandle_outdated` would mark the stable btree dhandle `WT_DHANDLE_OUTDATED`, forcing a fresh open on next access that would clear `READONLY`
4. Second step-up: `leader=true` set, drain called
5. Drain calls `__layered_table_get_constituent_cursor` → `__wt_open_cursor(stable_uri)` → `__wt_session_get_dhandle`
   - Dhandle is `WT_DHANDLE_OPEN` (not OUTDATED) → returned immediately at session_dhandle.c:944-946 with `READONLY` still set
6. Drain calls `__layered_copy_ingest_table` → `__layered_move_updates` → `__wt_row_modify` on stable btrees
7. `__wt_row_modify` internally calls dirty-marking, which hits the `if (READONLY) return` guard
   - In **debug builds**: `WT_ASSERT(!READONLY)` at `bt_page.c:1099` fires first → immediate crash
   - In **release builds**: dirty marking is silently skipped → pages appear modified in memory but are not
8. Drain reports success (no error returned)
9. Checkpoint runs, but skips the READONLY stable btrees → drain writes are NOT persisted
10. On close or crash, all data that was drained in step 6 is **silently lost**

Note: `__disagg_step_up` itself never calls `__wti_conn_dhandle_outdated` on the stable btrees before running drain. The TODO comment at `conn_layered.c:381` explicitly acknowledges this: *"This is better done at step-up or step-down to force close all live btrees."*

### What the current tests do

**Tests with multiple role swaps on the same connection:**

| Test | Sequence | Notes |
|---|---|---|
| test_layered15.py | follower→leader→follower→**restart**→leader→follower→**restart**→leader | Connection restarted between every swap — fresh dhandles each time, READONLY not an issue |
| test_layered27.py (`test_drain_insert_remove_within_same_transaction`) | "leader"→step_down→step_up | Connection opens as leader but immediately steps down before any writes; no data verification after step_up |
| test_layered26.py | follower→leader→follower | Part 6 (step_down after step_up) is disabled via `if False:` at line 265 with comment "FIXME-WT-14545" |

**`test_layered27` clarification (per reviewer):** The `test_drain_insert_remove_within_same_transaction` method creates the table as leader then immediately steps down — never doing any leader-mode writes before step_down. The stable btree may technically be open (having been created) but has no meaningful leader data. After step_up at line 229, the test does not read back any data — only checkpoint + close. So this test does NOT catch the READONLY issue even if it occurs.

**The disabled Part 6 in `test_layered26`** (guarded by `if False:` at line 265): *"FIXME-WT-14545: enable this test when stepping down is debugged."* This is the same FIXME referenced inside `__clayered_adjust_state` at line 515.

### Why the double-swap-on-same-connection pattern matters in practice

Most follower→leader promotion in the test suite uses **two separate connections** (old leader closes, new leader opens fresh). The single-connection cycle represents the case where a node temporarily steps down for maintenance (e.g., leader handoff during rolling restart) and then steps back up on the same database home directory.

### Key evidence that the bug exists

1. `WT_BTREE_READONLY` is **never cleared** in any path inside `__disagg_step_up` — confirmed by exhaustive grep of `F_CLR.*READONLY` across all `src/` files (returns no matches in `conn_layered.c` or any step-up path)
2. `__disagg_step_up` does not call `__wti_conn_dhandle_outdated` on stable btrees before running drain — the TODO at `conn_layered.c:381` calls this out explicitly
3. `btree_inline.h:936` silently returns on READONLY btrees without marking pages dirty
4. `checkpoint_txn.c:403` skips READONLY btrees in checkpoint
5. Debug assertion at `bt_page.c:1099` would catch this immediately if a debug-build test ran the fast-handoff sequence without any intermediate checkpoint pickup
6. `conn_layered.c:295–308` (`__wt_conn_dhandle_find`): a non-OUTDATED READONLY dhandle IS returned to non-readonly cursor opens (OUTDATED check at line 295 only applies to `WT_DHANDLE_OUTDATED` dhandles — step_down never sets OUTDATED)

---

## Proposed test: `test_layered_double_role_swap`

**What it should do:**

```python
# 1. Start as leader, create table, insert data, checkpoint
conn = wiredtiger_open(home, 'disaggregated=(role="leader"), ...')
session = conn.open_session()
session.create('layered:test', 'key_format=S,value_format=S')
cursor = session.open_cursor('layered:test')
cursor['key1'] = 'value1'
session.checkpoint()

# 2. Step down (sets WT_BTREE_READONLY on stable btrees)
conn.reconfigure('disaggregated=(role="follower")')

# 3. Step up again on THE SAME connection (bug: READONLY not cleared)
conn.reconfigure('disaggregated=(role="leader")')

# 4. Write new data — this is the critical path
cursor = session.open_cursor('layered:test')
cursor['key2'] = 'value2'
session.checkpoint()

# 5. Close and reopen — data from step 4 must survive
conn.close()
conn = wiredtiger_open(home, 'disaggregated=(role="follower"), ...')
conn.reconfigure(f'disaggregated=(checkpoint_meta="{checkpoint_meta}")')
conn.reconfigure('disaggregated=(role="leader")')
session = conn.open_session()
cursor = session.open_cursor('layered:test')
# ASSERT: both key1 and key2 are readable
assert cursor['key1'] == 'value1'
assert cursor['key2'] == 'value2'  # ← this would FAIL without the fix
```

**What it validates:**
- `WT_BTREE_READONLY` is properly cleared during the second step-up
- Drain writes to the stable btrees are persisted through checkpoint
- The data is recoverable after a restart following the second step-up

**Where it belongs:** `test/suite/test_layered_role_swap_recovery.py` or as a new test case in `test_layered15.py`

---

## Proposed test: `test_layered_stepup_fault`

**What it should do:**
- Use a mock/fault-injectable page log that fails during `__disagg_begin_checkpoint`
- Call `reconfigure(role="leader")` and verify it returns `WT_PANIC` (or that the connection is in panic state)
- Verify the error message is "failed to step-up as primary"
- Verify subsequent operations on the panicked connection return `WT_PANIC`

**Where it belongs:** Could be a C-level csuite test using the existing `timestamp_abort` crash injection infrastructure, or a Python test with a fault-injectable palite extension.

---

## Summary of Findings

| Sub-case | Severity | Bug confirmed? | Tests covering it | Action |
|---|---|---|---|---|
| Step-up failure → `__wt_panic` | HIGH | Design is intentional but untested | None | Add fault-injection test |
| `WT_BTREE_READONLY` not cleared on 2nd step-up | CRITICAL | **Yes** — no `F_CLR(READONLY)` exists in step-up path | test_layered27 hits the pattern but doesn't verify | Fix the bug + add test |

The `WT_BTREE_READONLY` issue is a **real bug**, not just a testing gap. It exists in both debug and release builds (crashes on assert in debug, silent data loss in release). The proposed test would expose it.

FIXME-WT-14545 in `test_layered26.py` is directly related — Part 6 of that test was disabled because "stepping down is debugged." This investigation explains precisely why: the READONLY flag is not cleared on step-up.
