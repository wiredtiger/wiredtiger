# EVICTION Blockers

This document consolidates eviction/reconciliation blocker conditions found in the current codebase and prior analysis. It focuses on reasons a page is skipped, deferred, or fails eviction/reconciliation.

Duration class legend used below:
- `Transient`: expected to clear quickly (lock/cas/hazard races, queue timing).
- `Medium`: can persist across multiple passes but usually clears as txn/checkpoint state advances.
- `Potentially long-lived`: can last for checkpoint windows, long-running transactions, or materialization/frontier waits.
- `Mixed`: depends on workload/state; can be short or long.

## Queueing / Candidate Selection

| Reason | duration class | source references | Notes/Description |
|---|---|---|---|
| Tree eviction disabled (`btree->evict_disabled > 0`) | Potentially long-lived | `src/evict/evict_lru.c:1822` | Entire tree is skipped for eviction candidate walks. |
| Read-only tree skipped when not seeking clean pages | Potentially long-lived | `src/evict/evict_lru.c:1829` | Read-only trees are skipped unless clean-page eviction is enabled. |
| Checkpointing tree skipped for dirty-only selection | Potentially long-lived | `src/evict/evict_lru.c:1836` | During syncing, dirty-only walks skip this tree. |
| Stick-in-cache priority skip (unless aggressive/dominating cache) | Mixed | `src/evict/evict_lru.c:1849` | Defers low-priority trees until pressure/aggressive mode changes. |
| Too many active walks/hazards for walk session | Mixed | `src/evict/evict_lru.c:1865` | Tree skipped when walk-session active references exceed threshold. |
| Tree previously "not useful" in recent walk periods | Medium | `src/evict/evict_lru.c:1888` | Walk-period heuristic temporarily skips this tree. |
| In-memory tree skipped unless dirty eviction mode is active | Medium | `src/evict/evict_lru.c:1895` | In-memory btrees are skipped unless current mode includes dirty targets. |
| In disaggregated mode, ingest/stable tree skip accounting | Mixed | `src/evict/evict_lru.c:1712` | Skip counters distinguish ingest vs stable trees in disagg. |
| Tree has no bytes relevant to current eviction mode | Medium | `src/evict/evict_lru.c:2068` | If target pages are zero and no desired bytes exist, tree is skipped. |
| Dirty page skipped while tree is checkpointing | Potentially long-lived | `src/evict/evict_lru.c:2486` | Candidate is dropped during checkpoint sync window. |
| Clean HS pages skipped for updates target during precise checkpoint | Potentially long-lived | `src/evict/evict_lru.c:2503` | Avoids evicting HS clean pages likely needed soon. |
| Page class not wanted by current eviction mode | Medium | `src/evict/evict_lru.c:2541` | Clean/dirty/updates target filter excludes the page. |
| Clean metadata leaf with historical content needed by readers | Potentially long-lived | `src/evict/evict_lru.c:2556` | Metadata has no history store fallback, so page is preserved. |
| Internal page skipped because worker observed active child | Long-lived | `src/evict/evict_lru.c:2573` | **Under the new Internal page policy**: internal pages with active children are never in LRU lists, so this check becomes a guard against races only. Internal pages enter LRU lists only when they lose all active children. |
| Internal page skipped until aggressive mode/tree idle | Long-lived | `src/evict/evict_lru.c:2577` | **Removed under the new Internal page policy**: aggressive-mode/tree-idle gating is no longer needed. Internal pages in LRU lists are always childless and eligible for eviction. |
| Dirty page retry heuristic says "too soon" | Medium | `src/evict/evict_lru.c:2111` | `__wt_page_evict_retry` gate avoids repeated failed retries. |
| Page has updates at/after `last_running` txn | Mixed | `src/evict/evict_lru.c:2124` | Skip recent updates unless hard pressure path applies. |
| GC tree: newest commit timestamp newer than prune timestamp | Potentially long-lived | `src/evict/evict_lru.c:2133` | Skip until prune timestamp advances. |
| Precise checkpoint: newest commit timestamp newer than pinned stable ts | Potentially long-lived | `src/evict/evict_lru.c:2141` | Skip until stable timestamp advances. |
| Candidate already urgent-queued or tree eviction-disabled on urgent enqueue | Mixed | `src/evict/evict_lru.c:3295` | Urgent queue add is rejected if already urgent/ineligible. |
| Urgent queue has no free slot or urgent enqueue push fails | Transient | `src/evict/evict_lru.c:3331` | Urgent enqueue succeeds only when capacity remains and candidate push/flagging succeeds. |
| Candidate flagging race (`WT_PAGE_EVICT_LRU` CAS fails/already set) | Transient | `src/evict/evict_lru.c:1980` | Concurrent queueing race prevents re-adding page. |
| Core evictability predicate returns false during queueing | Mixed | `src/evict/evict_lru.c:2590` | `__wt_page_can_evict` blocks candidate before enqueue. |

## Claim / Lock / Dispatch

| Reason | duration class | source references | Notes/Description |
|---|---|---|---|
| No work in normal/urgent queues (`WT_NOTFOUND`) | Transient | `src/evict/evict_lru.c:2873` | Dispatcher finds all usable queues empty. |
| Server declines to continue from partially useful queues (`WT_NOTFOUND`) | Transient | `src/evict/evict_lru.c:2888` | Server-side policy returns early to repopulate/fill heuristically. |
| Queue becomes empty at claim time (`WT_NOTFOUND`) | Transient | `src/evict/evict_lru.c:2933` | Candidate disappeared/consumed before claim. |
| Candidate ref no longer `WT_REF_MEM` or CAS lock to `WT_REF_LOCKED` fails | Transient | `src/evict/evict_lru.c:2985` | Race with other activity/eviction invalidates candidate. |
| Server/app avoids dirty ordinary-queue candidate under policy | Medium | `src/evict/evict_lru.c:2974` | Dirty entries can be deferred unless urgent or hard pressure path. |

## Eviction / Reconciliation Blockers

| Reason | duration class | source references | Notes/Description |
|---|---|---|---|
| Hazard pointer on page | Transient | `src/evict/evict_page.c:41` | Another thread still uses page; eviction returns `EBUSY`. |
| Internal page has prefetch child or non-evictable child state | Mixed | `src/evict/evict_page.c:567` | Parent eviction blocked when child refs are active/non-disk. |
| Child delete ref cannot be locked for visibility check | Transient | `src/evict/evict_page.c:629` | CAS to `WT_REF_LOCKED` fails; parent eviction aborts. |
| Child truncate/delete not visible/committed | Potentially long-lived | `src/evict/evict_page.c:664` | Parent eviction blocked until delete becomes visible. |
| In-memory mode: clean page eviction disallowed | Potentially long-lived | `src/evict/evict_page.c:855` | Clean pages are not evicted in in-memory configurations. |
| Checkpoint running on HS + dirty HS dominates cache | Potentially long-lived | `src/evict/evict_page.c:888` | Non-HS dirty eviction blocked to avoid cache blow-up. |
| Precise checkpoint: page already reconciled for checkpoint timestamp | Potentially long-lived | `src/evict/evict_page.c:900` | Reconcile would be redundant; page is skipped (`EBUSY`). |
| Session has `WT_SESSION_NO_RECONCILE` | Potentially long-lived | `src/evict/evict_page.c:910` | Thread-level reconcile prohibition blocks eviction. |
| Reconcile recheck after page lock says not evictable | Mixed | `src/reconcile/rec_write.c:119` | State can change while waiting for page lock; rechecked and blocked. |
| Reconcile made no effective progress | Medium | `src/reconcile/rec_write.c:331` | Sets snapshot refresh hint and returns `EBUSY` to avoid spin. |
| Multi-block reconciliation while another session checkpoints | Potentially long-lived | `src/reconcile/rec_write.c:2362` | Big-page reconciliation avoided during checkpoint window. |
| Update-restore eviction creates unsupported empty non-row-leaf chunk | Mixed | `src/reconcile/rec_write.c:2461` | Reconciliation returns `EBUSY` when eviction+saved-updates path would write empty non-row-leaf content. |
| Need to remove HS update while checkpoint is running | Potentially long-lived | `src/reconcile/rec_visibility.c:545` | Cannot safely delete from HS mid-checkpoint; returns `EBUSY`. |
| No-timestamp checkpoint race #1 (on-disk TW mismatch) | Potentially long-lived | `src/reconcile/rec_visibility.c:663` | Timestamp consistency check blocks eviction under checkpoint-running HS path. |
| No-timestamp checkpoint race #2 (selected TW stop/start mismatch) | Potentially long-lived | `src/reconcile/rec_visibility.c:555` | Tombstone/selected window inconsistency causes `EBUSY`. |
| No-timestamp checkpoint race #3 (timestamp fix needed during checkpoint-running HS) | Potentially long-lived | `src/reconcile/rec_visibility.c:1550` | Fixup is disallowed in this state; eviction aborts. |
| No-timestamp checkpoint race #4 (update-chain ordering issue) | Potentially long-lived | `src/reconcile/rec_visibility.c:601` | Out-of-order no-timestamp chain detected; eviction aborts. |
| Metadata/disagg-meta update from same transaction | Mixed | `src/reconcile/rec_visibility.c:781` | Eviction gives up when selecting own transaction's update in metadata paths. |
| Metadata chain anomaly: committed then uncommitted pattern | Mixed | `src/reconcile/rec_visibility.c:822` | Cannot discard uncommitted updates; reconciliation returns `EBUSY`. |
| Invisible newer updates remain while page expected clean-after-rec | Potentially long-lived | `src/reconcile/rec_visibility.c:1496` | Eviction must abort if invisibles remain under clean-after-rec expectation. |
| History-store timestamp ordering fix forbidden by policy | Potentially long-lived | `src/reconcile/rec_hs.c:101` | `error_on_ts_ordering` forces eviction failure to protect HS/checkpoint consistency. |
| Test failpoint: split-write bulk-load reconciliation forced busy | Transient | `src/reconcile/rec_write.c:2354` | Diagnostic failpoint path returns `EBUSY` (stress/testing only). |
| Test failpoint: post-image, pre-wrapup reconciliation forced busy | Transient | `src/reconcile/rec_write.c:361` | Diagnostic failpoint path sets `EBUSY` before wrapup (stress/testing only). |
| Test failpoint: history-store key delete-from-ts forced busy | Transient | `src/reconcile/rec_write.c:3369` | Diagnostic failpoint path returns `EBUSY` during HS delete-key flow (stress/testing only). |

## Core `__wt_page_can_evict` Predicate Blockers

| Reason | duration class | source references | Notes/Description |
|---|---|---|---|
| Ref is on prefetch queue (`WT_REF_FLAG_PREFETCH`) | Medium | `src/include/btree_inline.h:2249` | Eviction forbidden to avoid prefetch thread seeing freed ref. |
| Materialization frontier not reached for disaggregated page | Potentially long-lived | `src/include/btree_inline.h:2269` | Clean/discard path blocked before materialization boundary. |
| Uncommitted fast-truncate updates (`mod->inst_updates != NULL`) | Potentially long-lived | `src/include/btree_inline.h:2291` | Page cannot evict until truncate txn resolves. |
| Overflow-key safety during checkpoint sync | Potentially long-lived | `src/include/btree_inline.h:2303` | Multi-block row-store overflow key case blocks eviction. |
| Dirty page while btree syncing in another session | Potentially long-lived | `src/include/btree_inline.h:2337` | Dirty page eviction blocked during checkpoint sync. |
| Dirty internal page in disaggregated storage | Potentially long-lived | `src/include/btree_inline.h:2346` | Dirty disagg internal pages are kept in cache. |
| Disagg page reserved for next checkpoint while checkpoint running | Potentially long-lived | `src/include/btree_inline.h:2357` | Page blocked to preserve checkpoint sequencing semantics. |
| Internal page has active split generation | Medium | `src/include/btree_inline.h:2380` | Avoids freeing ref arrays while older split-gen readers may exist. |
| Clean metadata page appears too new to evict | Medium | `src/include/btree_inline.h:2387` | Global visibility gate blocks eviction of recently modified metadata page. |

## Forced Immediate Eviction Path

| Reason | duration class | source references | Notes/Description |
|---|---|---|---|
| Page release path cannot lock ref / hazard clear sequencing yields busy | Transient | `src/btree/bt_read.c:114` | `__wt_page_release_evict` returns `EBUSY` if lock handoff fails. |
| Forced-eviction precheck rejects page (internal/clean/small/hazard/retry/core predicate) | Mixed | `src/btree/bt_read.c:31` | `__evict_force_check` gates forced eviction with several early-return blockers. |
