## Internal page policy

Only **leaf pages** participate in the LRU eviction process. Internal pages follow a separate lifecycle:

Rationale:
- Internal pages exist to provide access to leaf pages below them. Promoting internal pages on every leaf access would cause severe lock contention on high-level pages.
- LRU reinsertion throttling would violate strict LRU order for internal pages anyway.
- An internal page with active children must remain in memory to provide access to those children; evicting it is wasteful and immediately forces re-reads.
- Once an internal page has no live children, it serves no purpose and should be evicted promptly.

Rules:
- **Internal pages with at least one active (in-memory) child are never placed in any list.**
- **When an internal page loses its last active child** (the last child is evicted or deleted), the internal page is inserted into the relevant lists at the **least-recently-used end** (LRU_HEAD sentinel in the LRU region) so that it becomes the next candidate for eviction.
- An internal page that enters lists in this way is treated as a normal eviction candidate from that point forward (subject to the same eligibility checks as any other page when a worker scans the LRU end).
- When a child page is instantiated under an internal page that is currently in a list (because it previously had no children), the internal page must be **removed from all lists** immediately, since it now has an active child again. Set `all_pages_region = NONE` and `dirty_pages_region = NONE`.

Implementation:
- Add an **atomic counter** (`uint32_t inmem_children`) to the **WT_REF_EVICT** struct, tracking the number of active (in-memory) children. This counter is only meaningful for internal pages; for leaf pages it remains 0. The counter is accessible whenever `ref->evict` is allocated (i.e., whenever the internal page is in memory), which is guaranteed for all operations that modify child state.
- On child instantiation: atomically increment `ref->evict->inmem_children`. If the internal page is currently in any list, remove it immediately (it now has an active child and must stay in memory).
- On child eviction: atomically decrement `ref->evict->inmem_children`. If it reaches zero, insert the internal page's **WT_REF** into **List 1 LRU region** (and **List 2 LRU region** if dirty) at the **least-recently-used end** (LRU_HEAD sentinel). Set `all_pages_region = LRU` (and `dirty_pages_region = LRU` if inserted into List 2).
- Workers must still verify that an internal page has no active children before attempting eviction (as a guard against races).

---

## In-memory table policy

Pages belonging to in-memory tables (`WT_BTREE_IN_MEMORY`) must **never** appear in List 1 (the All pages list). Their `all_pages_region` must always remain `NONE`.

Rationale:
- In-memory table pages must never be removed from memory — there is no on-disk representation to fall back to.
- Presence in List 1 can lead to `all_pages_region` transitioning away from `NONE` and eventually back to `NONE` via worker processing, which signals removal-from-memory intent. This intent is invalid for in-memory table pages and must be structurally prevented by keeping them out of List 1 entirely.

Rules:
- **Never insert** in-memory table pages into List 1 (not at page-in time, not via promotion, not via any other path).
- In-memory table pages **can** appear in **List 2** (dirty/updated pages list) and in the **urgent queue region** of List 2. When processed from List 2, their `all_pages_region == NONE` does **not** signal removal intent — instead, it means the page was never in List 1. Workers must check `F_ISSET(btree, WT_BTREE_IN_MEMORY)` to distinguish this case from a true removal intent.
- Workers processing a page from List 2 with `all_pages_region == NONE` must check whether the page belongs to an in-memory table. If so, the worker performs reconciliation/transform only (using `WT_REC_IN_MEMORY | WT_REC_SCRUB` flags) and the page stays in memory. The page is **not** evicted.
- In-memory table pages follow the same cooldown mechanics as other pages when processed from List 2: on failure, they move to the appropriate cooldown region within List 2.

---

## Non-evictable pages

Non-evictable pages (pinned, busy, locked, or otherwise ineligible) are handled through the standard worker scan and cooldown cycle — no special region or proactive removal is needed.

Rationale:
- Non-evictable state is usually temporary and maps naturally to cooldown tiers based on the blocking condition.
- Eligibility checks are applied after the worker pops and locks the page, using the same validation logic as for any other page.
- The cooldown system provides structured retry with appropriate timeouts, preventing repeated wasted attempts on the same page.

Implementation:
- Workers pop the next page from the LRU end, lock it, and check eligibility. If the page is not eligible: release the lock, move the page to the appropriate cooldown region based on failure type and duration (see [Cooldown placement on failure](WORK-QUEUES-AND-WORKERS.md#cooldown-placement-on-failure)), and proceed to the next page.
- Transient conditions (lock contention, hazard pointer) → CD-transient (1ms retry). Long-lived conditions (checkpoint sync, visibility constraints) → CD-long (2s retry). Persistent failures escalate through CD1→CD2→CD3→CD-long based on elapsed time since first failure.
- For a comprehensive list of blocker conditions and their duration classes, see [EVICTION-BLOCKERS.md](EVICTION-BLOCKERS.md).

---

## Eviction hints mapping (wont_need / evict_soon)

The current codebase uses read-generation hints to influence eviction priority. Under the new LRU design, these map as follows:

**`WT_READGEN_WONT_NEED` / `WT_READ_WONT_NEED`** ("won't need"):
- Meaning: the page will not be accessed again soon and should be removed from cache promptly.
- New mapping: insert the page at the **LRU end** (LRU_HEAD sentinel) of the **List 1 LRU region** so it becomes an early eviction candidate.
- Used by: RTS (rollback-to-stable), compaction walks, column search, tree walks with statistics, history/metadata verification.

**`WT_READGEN_EVICT_SOON`** ("evict soon"):
- Meaning: the page requires urgent processing — typically reconciliation, in-memory splitting, or write to disk.
- New mapping: insert into the **urgent queue region** in List 2, set the `urgent` flag, and set `dirty_pages_region = URGENT`. Non-clean workers check the urgent queue first and will process it regardless of dirty/updates pressure.
- Used by: oversized pages needing split, pages with long update chains, obsolete content needing reconciliation, `__wt_page_dirty_and_evict_soon` callers.
- Note: callers that only need memory removal (not transform/write) should use the "wont_need" path instead (LRU-end insertion in List 1). During implementation, review each evict_soon call site and confirm the correct mapping.

---

## Checkpoint eviction handling

Reuse the **urgent queue region** in List 2 for checkpoint dirty page transform/write (no separate checkpoint mechanism).

Rationale:
- Checkpoint dirty pages are **urgent** (need immediate reconcile/write).
- Non-clean workers check the urgent queue first and process urgent pages before scanning the LRU region.
- No additional per-ref data structure overhead.

Implementation:
- During checkpoint: mark dirty pages as urgent (set `urgent` flag), insert into the **urgent queue region** in List 2, and set `dirty_pages_region = URGENT`.
- After checkpoint: pages are processed by non-clean workers draining the urgent queue.

---

## App-assist eviction

When cache fill ratio exceeds `eviction_trigger`, application threads participate in eviction via `__wt_evict_app_assist_worker_check` / `__wti_evict_app_assist_worker`.

The dual purpose of app-assist is:
1. **Help eviction** by scanning LRU regions and processing pages.
2. **Throttle application threads** to slow down operations that generate cache pressure.

List selection heuristic for app-assist threads:
- Compute a pressure metric per threshold: `pressure = (1 - current_ratio) / (1 - threshold)` for each of the eviction and dirty/updates thresholds.
- Scan the list whose corresponding pressure metric is **lowest** (i.e., closest to the threshold, most in need of work): either **List 1** (clean eviction) or **List 2** (non-clean eviction/transform/write).
- This is a reasonable initial heuristic. Leave a comment in the code suggesting that the selection metric can be refined (e.g., weighting by LRU region size, or using a more sophisticated cost model).

App-assist threads act as **temporary workers** for the selected list: they scan the LRU end and process the next page using the same logic as dedicated workers (see [Worker behavior](WORK-QUEUES-AND-WORKERS.md#worker-behavior)).

---

## File close eviction path

File close (`__wt_evict_file`) must be a **direct synchronous operation** that completes before the file handle is released. Urgent queue insertion is not sufficient because:
- Workers process pages asynchronously — completion timing is not guaranteed.
- LRU-end insertion does not guarantee timely eviction.

Implementation under the new design:
1. Acquire exclusive eviction access for the file (blocks new eviction work on this file).
2. Walk the tree (same as current approach).
3. For each page's **WT_REF**:
   - Remove from all lists (both List 1 and List 2, from any region — LRU or cooldown). This is O(1) per list since LTAILQ supports removal from any position.
   - If the page has the `urgent` flag set: clear it.
   - Reconcile the page if dirty (synchronous).
   - Evict the page directly (free page, set `ref->page = NULL`).
   - Free `ref->evict` and set `ref->evict = NULL`, reset `ref->last_promotion_timestamp = 0`.
4. Release exclusive eviction access.
5. This bypasses the normal worker path entirely, which is correct for a synchronous close operation.

Note: No "Destroy-ref" flag is needed. LTAILQ supports removal from any position. If a worker has already popped the page from an LRU region, it will find the page evicted (`ref->page == NULL`) when it attempts to lock the ref, and skip via the normal validation path.

---

## Eviction vs reconciliation and in-memory transformations

There is confusion today between **eviction** (removing a page from memory), **reconciliation** (writing dirty content to disk), and other **in-memory transformations** that can occur as part of eviction flow.

Intent going forward:
- **Identify and separate** these use cases where possible, so callers can express the real intent.
- Preserve **fallback behavior**: when eviction is invoked with the intent to remove a page from memory, the system must still perform any required in-memory transformations, write the page out as needed, and then **attempt to remove it from memory**.

Do **not** document the exact per-callsite use cases yet; only capture this separation intent and keep the design consistent with it.

Threading policy:
- Use **two worker pools**:
  - **Clean workers** — scan List 1 LRU region, evict clean pages from memory.
  - **Non-clean workers** — scan List 2 LRU region, handle dirty eviction, transform/write, and urgent work.
- The **number of workers** in each pool is **configurable**, default **4** per pool.
- Each worker scans the **LRU end** of its list's LRU region and processes the next eligible page.
- If the worker accepts a page for work, it performs **all required actions** (in-memory transformations, write to disk, and/or removal from memory as dictated by page state and `all_pages_region`).
- If a worker cannot complete its action, it moves the page to the appropriate **cooldown region** and proceeds to the next cycle.
- **Clean workers** only accept clean pages. If a clean worker finds a dirty page, it inserts the page into the **urgent queue region** in List 2, sets the urgent flag and `dirty_pages_region = URGENT`, and skips.

For the current call-site mapping and intent analysis, see [EVICTION-INTENT-MATRIX.md](EVICTION-INTENT-MATRIX.md).
For cross-module eviction dependencies and likely impacts under the new LRU design, see [EVICTION-DEPENDENCIES.md](EVICTION-DEPENDENCIES.md).
For a deep review of eviction features/biases and how they map to pure LRU, see [EVICTION-FEATURES-LRU.md](EVICTION-FEATURES-LRU.md).
