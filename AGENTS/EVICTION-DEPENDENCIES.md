
**Btree‑level eviction walk state (likely removed or replaced)**  
These fields exist to support the current *walk‑based* eviction selection and are tightly coupled to the old algorithm.  
- `WT_BTREE.evict_ref`, `evict_pos`, `evict_saved_ref_check`, `evict_walk_*`, `last_evict_walk_flags` in `src/include/btree.h`  
  Explanation: these fields track where the eviction server last walked within a tree and the mode of that walk.  
  Likely outcome: **obsolete** with true LRU lists. LRU list membership replaces “walk position” and NPOS state.  
- Checkpoint code that saves/restores walk period: `src/checkpoint/checkpoint_txn.c`  
  Explanation: checkpoint stores the current walk skip period to avoid biasing eviction while it runs.  
  Likely outcome: remove or replace with LRU‑specific throttling knobs.  
- NPOS stats and walk helpers: `src/btree/bt_npos.c`, `src/include/stat.h`  
  Explanation: NPOS records a numerical walk position used to resume or randomize tree walks.  
  Likely outcome: **removed**, since NPOS is tied to walk‑based eviction.

**Checkpoint‑specific eviction coordination (needs re‑mapping)**  
- Checkpoint temporarily lowers dirty cache targets by writing `evict->eviction_scrub_target` and waits against `evict->eviction_checkpoint_target` in `src/checkpoint/checkpoint_txn.c`.  
  Explanation: `__checkpoint_set_scrub_target` updates the target, and `__checkpoint_wait_reduce_dirty_cache` spins until dirty cache drops.  
  Likely outcome: keep the *behavioral intent* (reduce dirty cache during checkpoint), but rewire to LRU‑based mechanisms.  
- Checkpoint resets per‑checkpoint eviction counters (e.g., `evict_max_*`, `reentry_hs_eviction_ms`) in `src/checkpoint/checkpoint_txn.c`.  
  Explanation: counters are cleared at checkpoint start to track per‑checkpoint maxima and timings.  
  Likely outcome: replace with LRU‑specific stats and reset points.  
- Checkpoint wakes eviction via `__wt_evict_server_wake()` and can evict file contents via `__wt_evict_file()` in `src/checkpoint/checkpoint_txn.c`.  
  Explanation: checkpoint explicitly prods eviction to resume and may evict pages as part of its cleanup path.  
  Likely outcome: keep API calls but ensure the new eviction pipeline honors them.

**Eviction priority / fairness knobs (conflict with “pure LRU”)**  
- `__wt_evict_priority_set/clear` and `btree->evict_priority` (metadata skew) in `src/meta/meta_table.c`, `src/evict/evict_lru.c`  
  Explanation: metadata tables set a high priority so eviction avoids them under the current read‑generation scheme.  
  Likely outcome: **remove or refactor**. "Pure LRU only" suggests no per‑tree skew; if metadata still needs protection, it should become an explicit **urgent insertion** rule (urgent queue region of List 2 with urgent flag) or **non‑evictable** policy, not a priority skew.

**Read‑generation heuristics (needs mapping to LRU lists)**  
- `page->read_gen`, `WT_READGEN_EVICT_SOON`, `WT_READGEN_WONT_NEED`, and related APIs in `src/evict/evict_inline.h`, `src/include/btree_inline.h`  
  Used by a lot of call sites to influence eviction order.  
  Likely outcome: **map to LRU list membership** (explicit promotions) or **urgent insertion into the urgent queue region of List 2** with urgent flag when transform/write is required. The semantic intent should remain, but the storage of "recency" will shift to list position.

**Eviction queue structures (old rotating queues → intent queues)**  
- `WTI_EVICT_QUEUE`, `evict_current_queue`, `evict_other_queue`, `evict_urgent_queue`, queue locks, and walk locks in `src/evict/evict_conn.c`, `src/evict/evict_lru.c`  
  Explanation: the eviction server walks trees into one queue while workers drain another, with a separate urgent queue.  
  Likely outcome: **replace** with two LTAILQ lists — List 1 (All pages) and List 2 (Dirty/updated pages) — each containing LRU and cooldown regions. List 2 also has a dedicated urgent queue region. No separate work queues. Workers scan LRU regions directly. Two worker pools (clean, non-clean). Workers determine intent from `all_pages_region` (REMOVE if `NONE`) and page state. Non-clean workers check the urgent queue first. No circular buffers, no Destroy-ref, no explicit intent flags — LTAILQ supports O(1) removal from any position.
- `__wt_evict_page_urgent` / `__wt_evict_page_soon` call sites (many)  
  Explanation: callers mark pages as urgent or set read‑generation to force prioritization in eviction.  
  Likely outcome: **retain APIs**, but rewire them to insert pages into the urgent queue region of List 2 with the `urgent` flag, instead of current queue mechanics.

**Eviction pass generation & retry throttling (likely replaced)**  
- `evict_pass_gen`, `page->cache_create_gen`, `mod->last_evict_pass_gen`, and `page->evict_queue_attempts` in `src/include/btmem.h`, `src/reconcile/rec_write.c`, `src/btree/bt_page.c`  
  Explanation: pass generations and attempt counters throttle repeated eviction and report walk progress.  
  Likely outcome: **replaced by per‑list timestamps** (already in the new doc). Eviction retry logic in `__wt_page_evict_retry` will need to use the new LRU throttling timestamps.

**Eviction config options tied to old walk / queue model**  
- `evict_use_npos`, `evict_legacy_page_visit_strategy`, `evict_sample_inmem` in `src/evict/evict_conn.c`, `src/include/connection.h`  
  Explanation: these options choose between walk strategies (NPOS, random/linear) and sampling behavior.  
  Likely outcome: **remove or repurpose**. These options exist to control eviction walk behavior and are incompatible with list‑based LRU.

**Btree handle sizing tied to eviction thresholds**  
- `btree->maxmempage` is clamped using `evict->eviction_dirty_trigger` in `src/btree/bt_handle.c`.  
  Explanation: ensures at least ~10 pages fit in cache at the dirty trigger to avoid non‑evictable oversized pages.  
  Likely outcome: keep the **cache‑pressure coupling**, but confirm if the threshold should still be tied to dirty trigger or replaced with LRU‑specific sizing policy.

**Eviction pressure APIs used broadly (should remain, but semantics may shift)**  
Callers use these to decide *when* to evict or throttle work.  
- `__wt_evict_clean_pressure`, `__wt_evict_dirty_needed`, `__wt_evict_needed` used in `src/conn/conn_prefetch.c`, `src/conn/conn_compact.c`, `src/include/misc_inline.h`  
  Explanation: these functions gate prefetch/compact/sleep decisions based on cache pressure.  
  Likely outcome: **retain API**, but adjust to LRU metrics and new queue behavior.

**Eviction disable / exclusive state (should remain, but LRU lists must respect it)**  
- `btree->evict_disabled`, `evict_disabled_open`, `evict_busy` used in `src/evict/evict_lru.c`, `src/conn/conn_dhandle.c`, `src/btree/bt_handle.c`, `src/conn/conn_prefetch.c`  
  Explanation: disable counters prevent eviction during handle close/critical sections; busy counters prevent closure mid‑eviction.  
  Likely outcome: **keep semantics**, but ensure list‑based LRU ignores or excludes pages when eviction is disabled.

**Eviction server coordination / interruption**  
- `evict->pass_intr` used in `src/conn/conn_dhandle.c` and `src/evict/evict_lru.c`  
  Explanation: handle list operations raise `pass_intr` to interrupt eviction while locks are held.  
  Likely outcome: **still needed**, but may shift from "interrupt walk" to "interrupt worker LRU scan".

**Handle lifecycle coordination with eviction**  
- `conn->evict->walk_tree` is asserted against handle removal in `src/conn/conn_dhandle.c`.  
  Explanation: ensures eviction isn’t walking a handle being removed.  
  Likely outcome: remove or replace with list‑based walk protection.  
- Session/handle close paths use `__wt_evict_file_exclusive_on/off` and `__wt_evict_file()` in `src/conn/conn_dhandle.c` and `src/session/session_dhandle.c`.  
  Explanation: these APIs block eviction and then evict/discard pages during handle close.  
  Likely outcome: keep API but adjust internal semantics to new queue/LRU model.

**Statistics and diagnostics (planned removal / replacement)**  
- Many eviction stats are updated inside evict paths (`src/evict/evict_page.c`, `src/evict/evict_lru.c`, `src/include/stat.h`).  
  Explanation: counters and maxima are keyed to walk/queue behavior and current eviction stats.  
  Likely outcome: **remove old stats**, replace with new LRU‑appropriate stats as per INSTRUCTIONS.

**Eviction stats update hooks**  
- `__wt_evict_stats_update()` is called by connection stats reporting in `src/conn/conn_stat.c`.  
  Explanation: stats init calls into eviction to refresh derived counters.  
  Likely outcome: keep the hook but adjust what it reports to match new LRU and worker statistics.

**Cache pool decisions**  
- Cache pool sizing reads `evict->eviction_trigger` in `src/cache/cache_pool.c`.  
  Explanation: used to avoid shrinking cache sizes so far that eviction becomes aggressively triggered.  
  Likely outcome: keep cache‑pressure coupling; update inputs if eviction triggers are redefined.

**Reconciliation‑eviction coupling**  
- `rec_write.c` checks `btree->evict_disabled` during reconciliation and updates `conn->evict->reentry_hs_eviction_ms`.  
  Explanation: reconciliation avoids reporting failure if eviction is disabled, and it feeds eviction timing telemetry.  
- `rec_hs.c` calls `__wt_evict_clean_needed`/`__wt_evict_dirty_needed` for verbose logging.  
  Explanation: history store logging reports cache pressure during HS reconciliation.  
  Likely outcome: keep checks and telemetry, but align thresholds and timing to the new LRU model.

**Transaction and cursor assist hooks**  
- `__wt_evict_app_assist_worker_check()` is called from transaction and cursor inline paths (`src/include/txn_inline.h`, `src/include/cursor_inline.h`).  
  Explanation: app threads help eviction when cache pressure is high.  
  Likely outcome: retain API, but ensure assist logic maps to direct LRU scanning rather than walk‑based selection.

**Split‑path dependency on scrub flags**  
- Split code checks `WT_EVICT_CACHE_SCRUB` in `src/btree/bt_split.c` when setting page progress flags.  
  Explanation: scrub flag determines whether in‑memory rewrite counts as eviction progress.  
  Likely outcome: keep semantics but confirm where the scrub flag is set in the new model.

**Checkpoint integration (depends on eviction behavior)**  
- Checkpoint uses eviction timing, evict walk period, and optional forced eviction in stress paths: `src/checkpoint/checkpoint_txn.c`, `src/btree/bt_sync.c`  
  Explanation: checkpoint may force eviction on pages during timing stress and depends on eviction timing counters.  
  Likely outcome: **rewrite interactions** so checkpoint doesn't depend on walk state; checkpoint‑driven work should insert pages into the urgent queue region of List 2 with the urgent flag.
