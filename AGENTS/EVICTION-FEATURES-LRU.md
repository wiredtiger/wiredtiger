# Eviction Feature Review for Pure LRU with Intent-Separated Queues

This report evaluates **existing eviction features/biases** under the new design that uses **two LTAILQ lists** and **two worker pools** (clean and non-clean) that scan LRU regions directly.

## Design context

- **Pure LRU**: list order is the primary eviction policy. Walk-based scoring and skew should not drive candidate choice.
- **Two lists**: List 1 (All pages) and List 2 (Dirty/updated pages), each containing LRU regions and cooldown regions — all as sub-regions of a single LTAILQ separated by sentinel pairs.
- **No dedicated Eviction Server**: workers scan LRU regions directly and process pages inline.
- **Implicit intent from list position**: `all_pages_region` in `WT_REF_EVICT` tracks whether the page is in List 1's LRU, cooldown, or removed. When `NONE`, the page should be removed from memory. Transform/write decisions are based on page state.
- **Clean workers**: only accept **clean pages**; dirty pages are inserted into the **urgent queue region of List 2** with the urgent flag set by the clean worker.
- **Urgent work**: urgent requests insert into the **urgent queue region** in List 2. Non-clean workers check the urgent queue first and process regardless of dirty/updates pressure.
- **Two worker pools**: clean workers (List 1) and non-clean workers (List 2). No shared pool.
- **Cooldown regions**: pages that fail processing are moved to cooldown regions (CD-transient, CD1–CD3, CD-long) within the same list, with escalating retry timeouts. Workers scan cooldown regions using atomic timers.
- **No stale entries or Destroy-ref**: LTAILQ supports O(1) removal from any position. No circular buffers, no stale entries, no Destroy-ref flag.

## Feature-by-feature assessment

**Read-generation scoring (`read_gen`)**
Current: eviction order is approximated by read-gen; "soon/wont-need" get forced priority.
Change: replace with explicit **LRU list membership** and **urgent insertion**:
- `WT_READGEN_WONT_NEED` → insert at the **LRU end** (head) of **All pages LRU** for prompt eviction.
- `WT_READGEN_EVICT_SOON` → insert into the **urgent queue region** of List 2 with the `urgent` flag set. Non-clean workers check the urgent queue first and process regardless of dirty pressure. Call sites that only need memory removal should use "wont_need" instead.
Recommendation: **Replace**.
Trade-offs: Pure LRU removes smoothing; must rely on list promotion throttling to avoid hot-page lock churn.

**Per-btree priority skew (`btree->evict_priority`, metadata skew)**  
Current: biases read-gen to keep metadata in cache and prefer leaf vs internal.  
Change: remove scoring. If metadata protection is still needed, route it via explicit **policy** (non-evictable or urgent transform/write), not skew.  
Recommendation: **Replace** (do not keep scoring).  
Trade-offs: Eliminates hidden bias; may increase metadata churn unless explicit protection is added.

**Internal page skew (`WT_EVICT_INTL_SKEW`)**
Current: penalizes internal pages to prefer leaf eviction.
Change: **Remove entirely**. Internal pages with active children are never placed in LRU lists. When an internal page loses all its children, it is inserted at the least-recently-used end of the relevant LRU lists for prompt eviction. This policy replaces both the scoring skew and any aggressive-pressure/idle-tree heuristics.
Recommendation: **Replace** (with Internal page policy as defined in INSTRUCTIONS.md).
Trade-offs: Simpler, deterministic behavior; no scoring or heuristic tuning needed. Internal pages are evicted promptly when childless and kept in memory when they have active children.

**Dirty vs clean scoring difference**  
Current: when evicting dirty pages, base score uses `page->modify->update_txn` instead of `page->read_gen`.  
Change: with pure LRU, use **list position** for ordering and use **list selection** (Dirty/Updates lists) rather than alternate scoring bases.  
Recommendation: **Replace**.  
Trade-offs: Simplifies policy; risk of increased reconcile churn if list admission is too eager.

**Size-based scoring adjustments**  
Current: large pages are fast-tracked (evict-soon) rather than using a size-weighted score; there is no general size bonus in the core score.  
Change: keep large-page fast-track via **urgent queue insertion with urgent flag** (when transform/write is required), but avoid size-weighted ordering in main LRU lists.
Recommendation: **Keep** fast-track; **avoid** size-based ordering.  
Trade-offs: Large pages are still handled quickly without distorting global LRU order.

**Tree walk strategy (NPOS, random/linear walk, walk period)**  
Current: eviction server walks trees and uses NPOS to resume; alternates strategies to balance fairness.  
Change: obsolete under global LRU lists.  
Recommendation: **Remove**.  
Trade-offs: Simplifies selection; removes walk-specific tuning knobs.

**“Tree usefulness” skip (`evict_walk_period`, `evict_walk_skips`)**  
Current: skips trees that yielded few candidates; dynamic doubling/halving of walk period.  
Change: replace with **LRU list admission** and queue-fill heuristics, not per-tree walk penalties.  
Recommendation: **Replace**.  
Trade-offs: Reduces wasted scanning but requires list-level metrics (e.g., list emptiness or eligibility filters).

**Dominating cache detection**  
Current: a tree that dominates cache can override stickiness and be prioritized for eviction.  
Change: in pure LRU, dominance should already be reflected by list order. If stickiness is removed, this heuristic becomes redundant.  
Recommendation: **Reconsider** (likely remove unless a new “stickiness” policy exists).  
Trade-offs: Removing simplifies policy; if some stickiness remains, dominance checks may still be needed.

**Skip trees by state (checkpointing, read-only, in-memory)**  
Current: walk skips trees based on eviction mode and flags.  
Change: keep as **eligibility filters** when selecting from lists.  
Recommendation: **Keep**.  
Trade-offs: Prevents invalid candidates without reintroducing tree walks.

**Urgent queue (`__wt_evict_page_urgent`, `__wt_evict_page_soon`)**
Current: fast-tracks candidates into urgent queue.
Change: keep API, but route to the **urgent queue region** in List 2 with the `urgent` flag set. Non-clean workers check the urgent queue first and process regardless of dirty/updates pressure.
Recommendation: **Replace** (implementation).
Trade-offs: Preserves callers and semantics; urgent requests are processed promptly via the urgent queue region.

**Fast-track dead/empty/large pages**  
Current: dead trees, empty pages, large pages become priority candidates.  
Change: keep via **intent override**; map to clean/dirty/transform queues based on page state.  
Recommendation: **Keep**.  
Trade-offs: Preserves important safety/perf guardrails.

**Dirty/updates triggers**
Current: flags determine whether dirty/updates eviction is enabled and "hard."
Change: keep thresholds, but use them to trigger **non-clean worker scanning** of List 2 (transform/write and dirty eviction).
Recommendation: **Replace**.
Trade-offs: Preserves cache pressure behavior while separating intent.

**Dirty candidate skip heuristics (`__evict_skip_dirty_candidate`)**  
Current: avoids thrash by skipping recently updated pages unless under pressure; uses transaction visibility, checkpoint timestamp checks, and a modification-count threshold (15) under low pressure (disaggregated).  
Change: keep **correctness checks** (visibility, checkpoint) and apply the skip only when draining the **transform/write** queue. Consider removing or simplifying the modification-count threshold if it conflicts with pure LRU intent.  
Recommendation: **Keep correctness checks**, **simplify** mod-count threshold.  
Trade-offs: Removing mod-count threshold may increase reconcile frequency; keeping it improves I/O efficiency but departs from pure LRU philosophy.

**Metadata history skip**  
Current: avoids evicting metadata pages with older visibility requirements.  
Change: keep as eligibility filter regardless of queue.  
Recommendation: **Keep**.  
Trade-offs: Correctness constraint.

**Scrub vs no-keep (`WT_EVICT_CACHE_SCRUB`, `WT_EVICT_CACHE_NOKEEP`)**
Current: reconcile dirty pages and keep in cache under lower pressure; evict under high pressure.
Change: **scrub** maps to non-clean worker processing from List 2 where `all_pages_region == LRU` (page stays in memory after reconciliation); **no-keep** maps to processing where `all_pages_region == NONE` (page is removed from memory after reconciliation).
Recommendation: **Replace** (implicit intent from list position).
Trade-offs: Keeps hot-page benefit; intent is determined by list membership rather than explicit flags.

**Update-restore eviction (`WT_REC_SCRUB`, `WT_REC_IN_MEMORY`)**  
Current: reconcile but restore updates, leaving page in memory.  
Change: keep as **transform/write intent** with explicit queue routing.  
Recommendation: **Keep**.  
Trade-offs: Correctness-critical; intent separation clarifies behavior.

**History store dirty dominance (`__wti_evict_hs_dirty`)**
Current: prioritizes HS pages when HS dirty dominates cache.
Change: keep, but insert HS pages into the **urgent queue region of List 2** with urgent flag for prompt transform/write processing.
Recommendation: **Replace** (routing).
Trade-offs: Prevents HS feedback loops; may temporarily deprioritize clean evictions.

**Skip clean HS pages during precise checkpoint**  
Current: avoids evicting HS pages needed soon.  
Change: keep as eligibility filter.  
Recommendation: **Keep**.  
Trade-offs: Preserves checkpoint correctness.

**Aggressive eviction (`evict_aggressive_score`)**
Current: escalates selection and allows more forceful candidates.
Change: keep, but compute "progress" based on **worker processing throughput** and **LRU region drain rate**; aggressive mode may relax eligibility filters.
Recommendation: **Replace** (new signals).
Trade-offs: Prevents stuck cache; must avoid over-eviction.

**Cache stuck (`WT_EVICT_SCORE_MAX`)**
Current: signals severe eviction failure and can trigger rollback.
Change: keep, but base on **worker processing progress** (pages processed per cycle, cooldown region sizes) not walk progress.
Recommendation: **Replace** (new signals).
Trade-offs: Maintains safety; requires new progress metrics.

**Empty-queue score (`evict_empty_score`)**
Current: if queues are empty, increase aggressiveness.
Change: replace with "no eligible candidates in LRU regions" or "all candidates in cooldown" metrics.
Recommendation: **Replace**.
Trade-offs: Still needed, but the signal changes.

**Worker auto-tuning (`__evict_tune_workers`)**
Current: adjusts worker count based on eviction progress.
Change: keep, but track progress per **worker pool** (pages processed, cooldown rates) and tune each pool independently.
Recommendation: **Replace** (new metrics).
Trade-offs: Maintains throughput while preventing transform/write saturation.

**App-assist eviction**
Current: app threads help eviction under pressure.
Change: keep; app-assist threads select a list based on a pressure metric (`(1 - current_ratio) / (1 - threshold)`) and scan the LRU end of the list most in need. This serves the dual purpose of helping eviction and throttling application threads.
Recommendation: **Replace** (routing and selection heuristic).
Trade-offs: Keeps latency under pressure; intent routing reduces surprises. Selection heuristic can be refined later.

**Candidate selection fractions**  
Current: eviction considers a subset of queue entries rather than always the strict oldest (to maintain throughput).  
Change: there's no eviction server or batches. the pages are processed one by one by workers.
Recommendation: **Remove**.  
Trade-offs: Simpler ordering; may reduce flexibility in highly contended cases.

**Give-up / desert detection in walks**  
Current: walk gives up in sparse areas with few candidates.  
Change: not applicable with LRU lists.  
Recommendation: **Remove**.  
Trade-offs: Simplifies selection logic.

**Debug aggressive mode**  
Current: debug flag forces aggressive behavior.  
Change: keep as a debugging tool; apply to all intent queues.  
Recommendation: **Keep**.  
Trade-offs: None for production; useful for testing.

**Internal page eviction restrictions**
Current: do not evict internal pages with active children; also skip internal pages unless aggressive or tree idle.
Change: **Replaced by the Internal page policy** (see INSTRUCTIONS.md). Internal pages with active children are never in LRU lists at all, so they cannot be selected. Internal pages enter LRU lists only when they lose all active children, at which point they are inserted at the least-recently-used end for prompt eviction. The old aggressive-mode/tree-idle heuristics are removed.
Recommendation: **Replace** (with Internal page policy).
Trade-offs: Eliminates the need for aggressive-mode or tree-idle gating for internal pages. Simplifies eligibility checks since internal pages in LRU lists are always childless and eligible.

**Checkpoint interaction**  
Current: skips dirty pages during checkpoint, saves walk state, and may force evict pages in timing stress.  
Change: keep correctness checks, remove walk-state dependencies, and route forced evictions to the appropriate intent queue.  
Recommendation: **Replace**.  
Trade-offs: Keeps checkpoint correctness; removes walk-specific coupling.

## Worker routing guidance

- **Clean workers** (List 1): scan List 1 LRU region. Process clean pages for removal from memory. Dead tree pages and empty pages are inserted at the LRU end for prompt processing.
- **Non-clean workers** (List 2): scan List 2 LRU region. Process dirty pages requiring reconcile (with or without removal from memory based on `all_pages_region`), non-evicting reconcile/scrub/update-restore, history-store cleanup, and other transformations.
- **Urgent work** (List 2): pages inserted into the urgent queue region of List 2 with the `urgent` flag set. Non-clean workers check the urgent queue first and process regardless of dirty/updates pressure.

## Key trade-offs introduced by the two-pool model

- **Pros**: clean eviction latency improves (clean workers never blocked by reconciliation); simpler architecture (2 pools, no separate eviction server, no circular buffers, no Destroy-ref, no explicit intent flags); cooldown regions provide structured retry for blocked pages; workers scan directly without producer-consumer coordination.
- **Cons**: routing decisions remain correctness-sensitive; non-clean pool handles all heavy work (urgent + dirty eviction + transform/write), so it must be sized appropriately; during checkpoint floods, non-clean eviction may temporarily stall while urgent work is prioritized; workers contend at the LRU end when scanning, though this is comparable to contention at a shared work queue head.
