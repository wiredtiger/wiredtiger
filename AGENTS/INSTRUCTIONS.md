## Goal

Replace the existing random / pseudo-LRU eviction mechanism with a **true LRU** eviction system.

"True LRU" means: **all in-memory pages participate in a doubly-linked LRU structure**, and eviction candidates are chosen from the **least-recently used end** when eviction is needed.

Do **not** implement any "btree/dhandle fairness" or similar balancing policies. Use **pure LRU only**.

---

## High-level expectations

- This is a **redesign** of the eviction subsystem, not a minimal patch.
- It is acceptable (and expected) to **delete large portions** of the current eviction code (e.g., most of `evict_lru.c`).
- **Remove all obsolete eviction-related code** from the codebase entirely (including dead paths and unused helpers) **except configuration options** (for compatibility).
- **Remove old eviction statistics**, and replace them with **new statistics** appropriate for the new design. The new statistics should provide insight into cache state and eviction behavior: queue sizes and fill rates, worker utilization per pool, eviction/transform/write throughput, LRU list sizes, cooldown region occupancy and flow rates, promotion throttle hit rates, and any other counters relevant to the actual implementation. The exact set of counters depends on the implementation; add them as the code takes shape.
- If you are unsure whether to keep any behavior/feature from the old eviction implementation, **stop and ask me first**.

---

## Architecture overview

The new eviction system uses **two LTAILQ lists**, each containing multiple **regions** separated by **sentinel pairs**. There are no separate work queues — workers scan LRU regions directly.

### List 1 — All pages list

Tracks all eligible in-memory pages, except internal pages with active children (see [Internal page policy](EVICTION-POLICIES.md#internal-page-policy)) and pages belonging to in-memory tables (see [In-memory table policy](EVICTION-POLICIES.md#in-memory-table-policy)). Clean eviction is processed from this list.

```
HEAD - [CD-long] - [CD3] - [CD2] - [CD1] - [CD-transient] - [All pages LRU (LRU→MRU)] - TAIL
```

### List 2 — Dirty/updated pages list

Tracks dirty/updated pages (subset of in-memory pages). Non-clean eviction, transform/write, and urgent work is processed from this list.

```
HEAD - [CD-long] - [CD3] - [CD2] - [CD1] - [CD-transient] - [Urgent queue] - [Dirty/updated LRU (LRU→MRU)] - TAIL
```

### Processing model

- **2 worker pools** scan LRU regions directly and process pages inline — there is no separate Eviction Server thread:
  - **Clean workers** — scan the LRU region in List 1. Handle eviction of clean pages (cheap, no I/O).
  - **Non-clean workers** — scan the LRU region in List 2. Handle dirty eviction, transform/write, and urgent work.
- Each pool has a configurable number of workers (default 4).
- Workers also scan **cooldown regions** of their own list opportunistically using **per-list atomic timers** — there is no dedicated cooldown worker thread.
- **App-assist threads** participate in eviction when cache pressure exceeds `eviction_trigger`.

### Urgent work

Urgent work (checkpoint, oversized pages) is handled via a dedicated **urgent queue region** in List 2, positioned between the cooldown regions and the LRU region. Pages requiring immediate processing are inserted into this region. Non-clean workers check the urgent queue first (before the LRU region) and process from it even when dirty/updates thresholds are not exceeded. A per-page `urgent` flag in `WT_REF_EVICT` prevents duplicate insertion into the urgent queue.

### Implicit intent from list position

The type of work to perform is determined by **page state** and **list position**, not by explicit intent flags:
- **Remove from memory**: implicit when the page is not in the LRU region of List 1 (tracked via a region-tracking field in `WT_REF_EVICT`). **Exception**: pages of in-memory tables (`WT_BTREE_IN_MEMORY`) are never in List 1 and always have `all_pages_region == NONE`, but must never be removed from memory. Workers must check the in-memory table flag to distinguish this case (see [In-memory table policy](EVICTION-POLICIES.md#in-memory-table-policy)).
- **Write to disk / transform in memory**: determined by page state (dirty flag, update bytes, etc.) at processing time, as in the existing `__wt_evict()` function.

### Cooldown regions

**Cooldown regions** (CD-transient, CD1, CD2, CD3, CD-long) hold pages that workers attempted but could not process. Pages escalate through cooldown tiers based on failure duration:
- **CD-transient**: transient failures (lock contention, hazard pointer). Retry timeout: 1ms.
- **CD1–CD3**: escalating tiers for persistent failures. Retry timeouts: 5ms, 10ms, 100ms.
- **CD-long**: long-lived conditions (checkpoint sync, materialization). Retry timeout: 2s.

Workers scan cooldown regions of **their own list** using **per-list atomic timers** (one `uint64_t` per cooldown tier per list; 5 tiers × 2 lists = 10 timers total). Each timer's lowest bit is a "scan in progress" flag; the remaining bits are the nanosecond timestamp of the last scan completion. Workers check timers in shortest-to-longest order; if any timer shows a scan already in progress, the worker stops cooldown scanning for that cycle (longer-delay tiers may be skipped, which is acceptable). When a cooldown scan finds eligible entries (timeout elapsed since `last_retry_ts`), it moves them to the **LRU end** of the scanning worker's list — unless the page's `urgent` flag is set, in which case it goes to the **urgent queue region** of List 2 instead. This ensures pages marked urgent before entering cooldown resume urgent processing after cooldown.

### Cross-list cooldown coordination

When pages are moved out of cooldown regions, the scanner checks the **other list's region indicator** and moves the page there too if it is also in cooldown:
1. Pages moved out of **List 1 cooldown**: check `dirty_pages_region`. If `COOLDOWN`, also move out of **List 2 cooldown** (to the LRU end of List 2, or to the urgent queue if the `urgent` flag is set). If `LRU` or `URGENT`, do not change the page's position in List 2.
2. Pages moved out of **List 2 cooldown**: check `all_pages_region`. If `COOLDOWN`, also move out of **List 1 cooldown** (to the LRU end of List 1). If `LRU`, their position in List 1 is unchanged. If `NONE`, do not insert into List 1 (REMOVE intent is still active).

Cross-list operations are done **sequentially** (not simultaneously) — see [Inter-list lock ordering](LRU-LISTS.md#inter-list-lock-ordering).

**Region-tracking fields** in `WT_REF_EVICT` (`evict_flags` byte):
- `all_pages_region` (bits 1-2): which region of List 1 the page is in (`NONE` / `LRU` / `COOLDOWN`).
- `dirty_pages_region` (bits 3-4): which region of List 2 the page is in (`NONE` / `LRU` / `COOLDOWN` / `URGENT`).
- `urgent` flag (bit 0): prevents duplicate insertion into the urgent queue.
- `sentinel` flag (bit 5): set on sentinel elements. Sentinel elements are special list boundaries and cannot be removed as eviction candidates. Sentinel identity is determined by comparing element address with predefined sentinel pointers stored in the connection's eviction state.

### Tracked object

Track **WT_REF**-backed page entries (not WT_PAGE) in the eviction lists. Region-boundary sentinels are also represented as `WT_REF_EVICT` entries with the sentinel flag set. Eviction metadata is stored in a separately allocated **WT_REF_EVICT** struct to minimize memory overhead for non-resident pages.

---

## Implementation approach (required steps)

1. Analyze interdependencies
   - Identify how other subsystems depend on eviction internals (variables, state, assumptions).
   - Document these dependencies and adjust callers as needed.

2. Lifecycle accuracy
   - Precisely define when a `REF` and/or `PAGE` is created, becomes visible, becomes ineligible, and is destroyed.
   - Ensure list insertion/removal happens at the correct points, with no stale pointers and no missing transitions.

3. REF <-> PAGE mapping correctness
   - Be explicit and careful: the mapping between `REF` and `PAGE` can change over time.
   - Any eviction/LRU bookkeeping must remain correct across mapping changes.

4. Feature selection policy
   - Confirm with me before keeping any legacy behaviors, especially:
     - multiple eviction modes/queues beyond what's specified here,
     - "clean-only" or "dirty-only" scanning policies,
     - special-case heuristics.

---

## Build and test

- Follow **AGENTS.md** for build and test instructions.
- Add/adjust tests as needed to validate:
  - LRU ordering correctness,
  - correct list membership and region placement under concurrency,
  - urgent queue region and processing behavior,
  - urgent queue and urgent flag behavior,
  - cooldown region mechanics and retry behavior,
  - cooldown tier escalation and timeout correctness,
  - correctness when pages are pinned/busy/not evictable.

---

## Collaboration rules

- Ask clarification questions whenever needed.
- Discuss any new design decisions and options with me before starting implementation.
- If you are uncertain whether to keep any old behavior, ask me first.

---

## Potential future improvements (out of scope)

If later we decide pure LRU needs targeted protections, consider these **explicit policies** rather than scoring biases:

- **Metadata protection**: add a dedicated policy (e.g., explicit non-evictable rules or urgent-intent routing) instead of `btree->evict_priority` skew.

- **Event-driven cooldown promotion**: on checkpoint completion or stable timestamp advance, bulk-promote matching entries from CD-long back to LRU regions. Requires per-entry reason codes.

- **Reason-specific cooldown sub-regions**: if CD-long scanning on events becomes expensive, split into sub-regions by blocker category (checkpoint, visibility, materialization).

---

## Detailed design documents

| Document | Content |
|---|---|
| [EVICTION-METADATA.md](EVICTION-METADATA.md) | WT_REF tracking, WT_REF_EVICT struct layout, allocation/deallocation lifecycle, access discipline |
| [LRU-LISTS.md](LRU-LISTS.md) | List layout and regions, LTAILQ data structure, sentinel pair boundaries, per-element locking, membership tracking, time-gated promotion, page replacement rules |
| [WORK-QUEUES-AND-WORKERS.md](WORK-QUEUES-AND-WORKERS.md) | Worker pools (clean/non-clean), direct LRU scanning, worker behavior, urgent queue, cooldown mechanics, corner cases |
| [EVICTION-SCANNING.md](EVICTION-SCANNING.md) | Worker scanning strategy, threshold triggers, cooldown scanning with atomic timers, state transitions, invariants |
| [EVICTION-POLICIES.md](EVICTION-POLICIES.md) | Internal pages, non-evictable pages, eviction hints, checkpoint handling, app-assist, file close, intent separation |
| [EVICTION-INTENT-MATRIX.md](EVICTION-INTENT-MATRIX.md) | Call-site mapping and intent analysis |
| [EVICTION-DEPENDENCIES.md](EVICTION-DEPENDENCIES.md) | Cross-module eviction dependencies |
| [EVICTION-FEATURES-LRU.md](EVICTION-FEATURES-LRU.md) | Eviction features/biases mapping to pure LRU |
| [EVICTION-BLOCKERS.md](EVICTION-BLOCKERS.md) | Eviction/reconciliation blocker conditions and duration classes |
| [EVICTION-BLOCKERS-MITIGATION.md](EVICTION-BLOCKERS-MITIGATION.md) | Mitigation techniques for repeated non-evictable page selection |
| [LTAILQ-EXAMPLES.md](LTAILQ-EXAMPLES.md) | LTAILQ per-element locking algorithm examples: insert, remove, promotion, work item dequeue, worker flow, page deallocation |
