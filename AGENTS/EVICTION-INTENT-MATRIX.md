# Eviction Intent Call-Site Matrix

This document maps **where pages enter eviction** and the **likely intent** behind those calls. It separates **direct calls** into `__wt_evict` from **queue/flag-based** signals that are processed later by eviction workers. The intent categories are:

- **Remove from memory** (clean eviction / discard).
- **Write to disk** (reconcile dirty pages and persist).
- **Transform in memory** (in-memory split, rewrite, update-restore).

**Intent mask legend**:
- `R` = remove from memory
- `W` = write to disk
- `T` = transform in memory
- Combine with `|` (e.g., `R|W|T`).

## How eviction actually behaves today

The worker path always funnels through `__wt_evict`, which can do **three different things** depending on page state, flags, and reconciliation results:

- **Remove from memory** when clean or when reconciliation allows discard.
- **Write to disk** when dirty pages are reconciled.
- **Transform in memory** (in-memory split or rewrite) while keeping the page resident.

This is why “eviction” currently mixes intents: the same call can result in remove, write, or transform.

## Call sites: direct `__wt_evict` (bypass queue)

| Call site (function) | Location | How it reaches eviction | Flags/markers | Intent mask | Intent notes |
|---|---|---|---|---|---|
| `__evict_page` (worker/server/app-assist) | `src/evict/evict_lru.c:3023` | **Worker queue consumer** → direct `__wt_evict` | Typically no call flags | `R|W|T` | Remove if clean; if dirty → reconcile/write; may transform via in-memory split |
| `__wt_page_release_evict` | `src/btree/bt_read.c:98` → `src/btree/bt_read.c:141` | **Direct** eviction on release | `WT_EVICT_CALL_URGENT`; `WT_EVICT_CALL_NO_SPLIT` if `WT_READ_NO_SPLIT` | `R|W|T` | Remove if possible; otherwise reconcile/write or transform |
| Forced eviction of too-big pages | `src/btree/bt_read.c:648` | **Direct** via `__wt_page_release_evict` | Same as above | `R|W|T` | Remove if possible; otherwise reconcile/write or transform |
| Checkpoint timing stress eviction | `src/btree/bt_sync.c:351` | **Direct** via `__wt_page_release_evict` | Same as above | `R|W|T` | Remove if possible; otherwise reconcile/write (transform possible via in-memory split) |
| Cursor debug reset eviction | `src/include/cursor_inline.h:307` | **Direct** via `__wt_page_release_evict` | Same as above | `R|W|T` | Remove if possible; otherwise reconcile/write or transform |
| Page release path (evict soon) | `src/include/btree_inline.h:2427` and `src/include/btree_inline.h:2441` | **Direct** via `__wt_page_release_evict` | Same as above | `R|W|T` | Remove if possible; otherwise reconcile/write or transform |
| Delete path (clean only) | `src/btree/bt_delete.c:112` | **Direct** `__wt_evict` | No special flags | `R` | Clean eviction only |
| Salvage/close | `src/btree/bt_slvg.c:202`, `src/btree/bt_slvg.c:1286`, `src/btree/bt_slvg.c:1941` | **Direct** `__wt_evict` | `WT_EVICT_CALL_CLOSING` | `R|W` | Reconcile/write if dirty, then remove |
| File close eviction | `src/evict/evict_file.c:108` | **Direct** `__wt_evict` inside file walk | `WT_EVICT_CALL_CLOSING` | `R|W` | Reconcile/write then remove |

## Call sites: queue/flag-based (processed later)

| Call site (function) | Location | How it reaches eviction | Flags/markers | Intent mask | Intent notes |
|---|---|---|---|---|---|
| Mark evict soon (general) | `src/btree/bt_read.c:87`, `src/btree/bt_random.c:91`, `src/btree/row_modify.c:436`, `src/btree/bt_sync_obsolete.c:167`, `src/btree/bt_walk.c:243`, `src/btree/bt_split.c:744` | **Mark for urgent handling** | `WT_READGEN_EVICT_SOON` via `__wt_evict_page_soon` | `W|T` | Under the new design, urgent handling inserts into the **urgent queue region of List 2** with `urgent` flag set (transform/write, not clean removal) |
| Mark dirty + evict soon | `src/include/cursor_inline.h:295`, `src/btree/bt_curnext.c:771`, `src/btree/bt_curprev.c:682` | **Mark for urgent eviction** | `__wt_page_dirty_and_evict_soon` → `WT_READGEN_EVICT_SOON` | `W|T` | Forces reconciliation to clean obsolete content |
| Urgent insertion | `src/include/btree_inline.h:2439` | **Insert into urgent queue region of List 2** with `urgent` flag | `WT_PAGE_EVICT_LRU_URGENT` via `__wt_evict_page_urgent` | `W|T` | Urgent insertion is **transform/write only** (no clean eviction). Non-clean workers check urgent queue first. |
| “Wont need” read hint | `src/btree/bt_curnext.c:777`, `src/btree/bt_curprev.c:688`, `src/btree/row_srch.c:575`, `src/btree/col_srch.c:196`, `src/btree/bt_walk.c:481`, `src/prepared_discover/prepared_discover_walk.c:385`, `src/rollback_to_stable/rts_btree_walk.c:128` | **Priority hint** for eviction order | `WT_READ_WONT_NEED` → `WT_READGEN_WONT_NEED` in `__wt_evict_touch_page` | `R|W|T` | Remove if clean; reconcile/write or transform if dirty |

## Notes that tie the paths together

- `__wt_page_release` checks `__wt_evict_page_soon_check` and decides between **direct eviction** (`__wt_page_release_evict`) or **urgent insertion into List 2's urgent queue region** (`__wt_evict_page_urgent`). See `src/include/btree_inline.h:2400`.
- "Direct call flags" are `WT_EVICT_CALL_URGENT`, `WT_EVICT_CALL_NO_SPLIT`, and `WT_EVICT_CALL_CLOSING` in `src/evict/evict.h:156-158`.
- "Queue/flag markers" are read-generation flags `WT_READGEN_EVICT_SOON` and `WT_READGEN_WONT_NEED` (set in `src/evict/evict_inline.h`).
- Under the new design:
  - **`WT_READGEN_WONT_NEED`** → insert at the **LRU end** (LRU_HEAD sentinel) of the **List 1 LRU region** for prompt eviction (memory removal).
  - **`WT_READGEN_EVICT_SOON`** → insert into the **urgent queue region** in List 2 with the `urgent` flag set. Non-clean workers check the urgent queue first and process regardless of dirty/updates pressure. Call sites that only need memory removal (not transform/write) should use the "wont_need" path instead.
- Under the new design, workers determine work type from **page state** and **`all_pages_region`** (which provides implicit REMOVE intent when the page is not in List 1). No explicit intent flags are used.
- Lists are **LTAILQ** structures (not circular buffers), so pages can be removed from any position in O(1). No Destroy-ref flag is needed.
- There is no dedicated Eviction Server thread. Workers scan LRU regions directly.

## Where intent flips inside `__wt_evict`

- **In-memory split**: `__evict_review` can set `inmem_split` and call `__wt_split_insert` (transform, keep in memory).
- **Reconciliation**: dirty pages call `__evict_reconcile` → `__wt_reconcile` (write).
- **Dirty update results**: `__evict_page_dirty_update` can split, rewrite, replace, or delete based on `mod->rec_result`.
- **Obsolete time window cleanup**: `__evict_review_obsolete_time_window` can dirty clean pages to force reconciliation (transform/write intent).
