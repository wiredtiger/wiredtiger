## Worker pools

There are **two worker pools**, each with a **configurable number of workers** (default **4** per pool):

1. **Clean workers** — scan the LRU region of List 1 (All pages).
   - Handle eviction of clean pages from memory.
   - Cheap operations (no I/O, no reconciliation).

2. **Non-clean workers** — scan the LRU region of List 2 (Dirty/updated pages).
   - Handle dirty eviction (reconciliation + removal from memory).
   - Handle transform/write (reconciliation, scrub, in-memory rewrite).
   - Handle urgent work (checkpoint-driven reconciliation, oversized pages).
   - Check the **urgent queue region** to determine whether to process even without dirty/updates pressure.

Rationale for two pools:
- Clean eviction is extremely cheap and should not be blocked by heavy reconciliation or disk I/O.
- All non-clean operations involve similar heavy work (reconciliation, disk writes), so there is no benefit to separating them into multiple pools. A single non-clean pool provides better utilization: workers are never idle while one sub-type has pending work but its dedicated pool is full.

---

## Worker behavior

### Worker cycle

Each worker repeats the following cycle:

1. **Check cooldown regions** (see [Cooldown scanning by workers](EVICTION-SCANNING.md#cooldown-scanning-by-workers)). If any cooldown tier timer for this worker's list shows a scan already in progress, skip all cooldown scanning for this list.
2. **Check urgent queue** (non-clean workers only): if the urgent queue region in List 2 is non-empty → pop and process a page from the urgent queue.
3. **Check thresholds** to decide whether to scan:
   - **Non-clean workers**: dirty cache > `eviction_dirty_target`, or updates > `eviction_updates_target` → scan List 2 LRU region.
   - **Clean workers**: cache fill > `eviction_target` → scan List 1 LRU region.
4. **Scan LRU region** from the LRU end (see [LRU scanning](#lru-scanning)).
5. **No work available**: sleep / wait for wakeup signal.

### LRU scanning

When a worker decides to scan its list's LRU region:

1. **Remove the first page element** from the LRU end of the LRU region (or urgent queue) using the [Atomically getting a work item from HEAD](LTAILQ-EXAMPLES.md#atomically-getting-a-work-item-from-head-and-dequeueing-it) algorithm. This involves locking 3 per-element locks at the boundary (predecessor sentinel, the element, and its successor), removing the element, and unlocking. Sentinel entries are `WT_REF_EVICT` sentinel elements (`WT_REF_EVICT_SENTINEL` set) and must never be removed as eviction candidates.
2. **Lock the page for exclusive access** (CAS `ref->state` from `WT_REF_MEM` to `WT_REF_LOCKED`):
   - If the lock fails (another thread holds the ref): treat as a transient failure → move the page to the **CD-transient** cooldown region. See [Cooldown placement on failure](#cooldown-placement-on-failure).
   - If the lock succeeds: proceed to validation and processing.
3. **Validate eligibility**: check that the page is still in memory, not pinned, not otherwise blocked. If not eligible, release the lock and move the page to the appropriate cooldown region.

### Processing

If the worker accepts the page, it determines the work to perform based on **page state** and **list position**:

- **Clean workers** (scanning List 1):
  - The page was removed from List 1 LRU. `all_pages_region` was set to `NONE`.
  - Verify the page is clean. If the page is now dirty:
    - Insert the page into the **urgent queue region** in List 2, set the `urgent` flag, and set `dirty_pages_region = URGENT` (the page needs processing with REMOVE intent and cannot wait for normal dirty pressure).
    - The page's `all_pages_region` stays `NONE` (REMOVE intent from List 1 persists).
    - Release the ref lock and skip to the next cycle.
  - If the page is clean: evict it from memory.

- **Non-clean workers** (scanning List 2):
  - The page was removed from List 2's urgent queue or LRU region. `dirty_pages_region` was set to `NONE`.
  - Check `all_pages_region`:
    - `NONE` → the page was removed from List 1 for eviction → intent is **remove from memory**. Perform all required work (reconcile if dirty, write to disk, then evict).
    - `LRU` or `COOLDOWN` → the page is still tracked in List 1 → intent is **transform/write only** (reduce dirty cache, page stays in memory).
  - If the page has the `urgent` flag set: clear it.
  - Perform **all required actions** based on page state:
    - If the page requires **in-memory transformations**, perform them.
    - If the page needs to be **written to disk**, write it.
    - If the intent is **remove from memory**, remove it.

Page eviction, reconciliation, and in-memory transformations are always performed **under the appropriate page/eviction lock** (consistent with existing WiredTiger locking discipline).

### Failure handling

If the operation fails (lock contention, page busy, etc.), the worker moves the page to the appropriate **cooldown region** (see [Cooldown mechanics](#cooldown-mechanics)) and proceeds to the next cycle. Workers must not spin or retry on the same page.

---

## Worker completion

After processing a page, the worker:

1. **If the page was evicted (removed from memory):**
   - Deallocate `ref->evict` using the lock-based deallocation protocol: remove from all lists with double-check, then free the struct (see [Deallocation and cleanup](EVICTION-METADATA.md#deallocation-and-cleanup-page-out) and [Removing page from memory](LTAILQ-EXAMPLES.md#removing-page-from-memory-or-destroying-wt_ref)).
   - Reset `ref->last_promotion_timestamp = 0`.

2. **If the page stays in memory after successful processing (transform/write intent, selected from List 2):**
   - The page's position in **List 1** (All pages LRU) is **kept intact** — it was never removed from List 1.
   - Ensure correct list membership:
     - If the page is now clean, remove from List 2 (dirty/updated list) if present. Set `dirty_pages_region = NONE`.
     - If the page is still dirty, reinsert into **List 2 LRU region** at the MRU end. Set `dirty_pages_region = LRU`.
   - Clear cooldown state (reset `cooldown_enter_ts`, `last_retry_ts`, `cooldown_retry_count`, and `cooldown_reason` to 0).

3. **If the operation failed (selected from List 1, processing failed):**
   - Move the page to the appropriate **cooldown region in List 1** (see [Cooldown mechanics](#cooldown-mechanics)). Set `all_pages_region = COOLDOWN`.
   - If the page is in List 2 (`dirty_pages_region != NONE`): also move it to the corresponding **cooldown region in List 2**. Set `dirty_pages_region = COOLDOWN`. This prevents non-clean workers from wasting time on a page that is in cooldown from a List 1 failure.
   - When the page later exits cooldown in List 1, it returns to List 1's LRU. A clean worker will pick it up again, re-establishing REMOVE intent at that point.

4. **If the operation failed (selected from List 2, processing failed):**
   - The page's position in **List 1** remains intact.
   - Move the page to the appropriate **cooldown region in List 2** (see [Cooldown mechanics](#cooldown-mechanics)). Set `dirty_pages_region = COOLDOWN`.

5. **Release the ref lock** (`ref->state` transitions back to `WT_REF_MEM` or to `WT_REF_DISK` on eviction).

---

## Urgent work

Urgent work (checkpoint-driven reconciliation, oversized pages, dirty pages redirected by clean workers, etc.) bypasses threshold checks.

### Mechanism

- **Urgent queue region**: a dedicated region in List 2, positioned between the CD-transient cooldown region and the LRU region. Pages requiring immediate processing are placed here.
- **Per-page urgent flag**: a single bit (`urgent`) in `WT_REF_EVICT` to prevent duplicate insertion into the urgent queue.

### Insertion

When a caller needs urgent processing for a page:
1. Check the page's `urgent` flag. If already set → skip (page is already in the urgent queue or in cooldown with urgent pending).
2. Set the `urgent` flag on the page.
3. Insert the page into the **urgent queue region** in List 2. Set `dirty_pages_region = URGENT`.

Callers: `__wt_evict_page_urgent`, `__wt_evict_page_soon` (checkpoint, oversized pages, long update chains), and clean workers redirecting dirty pages.

### Processing

Non-clean workers check the urgent queue region first:
- If the urgent queue is **non-empty** → pop the first page from the urgent queue and process it. This happens regardless of dirty/updates thresholds.
- If the urgent queue is **empty** → check dirty/updates thresholds and scan the LRU region normally.
- When a worker processes a page with the `urgent` flag set: clear the flag.

### Semantics

- Urgent pages are processed with whatever intent their state indicates (REMOVE if `all_pages_region == NONE`, transform/write otherwise).
- The urgent queue provides a clean separation: cooldown-return pages go to the LRU region (or back to the urgent queue if the `urgent` flag is still set), explicit urgent callers insert into the urgent queue. No interleaving ambiguity.
- Emptiness is checked via the region's sentinels (`URGENT_HEAD->next != URGENT_TAIL`). No separate counter is needed.

---

## Cooldown mechanics

When a worker attempts to process a page and fails, the page is moved to a **cooldown region** within the appropriate list. The cooldown tier is determined by the failure type and duration.

### Cooldown state fields

Each `WT_REF_EVICT` struct contains:
- `cooldown_enter_ts` — timestamp when the ref first entered cooldown (0 when not in cooldown). Set on first failure, cleared on successful processing.
- `last_retry_ts` — timestamp of the most recent processing attempt. Updated each time the page is attempted.
- `cooldown_retry_count` — number of processing attempts since entering cooldown (statistics/diagnostics). Incremented on each failure, cleared on successful processing.
- `cooldown_reason` — reason code for the current cooldown (statistics, future event-driven promotion). Set on each failure, cleared on successful processing.

### Cooldown placement on failure

When a worker fails to process a page:

1. Update `last_retry_ts = now`.
2. If `cooldown_enter_ts == 0`, set `cooldown_enter_ts = now` (first failure).
3. Increment `cooldown_retry_count`.
4. Set `cooldown_reason` to a reason code classifying the failure (e.g., lock contention, hazard pointer, checkpoint sync, visibility constraint).
5. Determine the cooldown tier:
   - If the failure is **transient** (e.g., failed to lock the page, hazard pointer, CAS race) → **CD-transient**.
   - If the failure is due to a **long-running condition** (e.g., tree is being checkpointed, materialization frontier not reached, visibility constraint) → **CD-long**.
   - Otherwise, based on elapsed time since `cooldown_enter_ts`:
     - `elapsed < T1` → **CD1**
     - `elapsed < T2` → **CD2**
     - `elapsed < T3` → **CD3**
     - `elapsed >= T3` → **CD-long**
   - Values: T1 = 50ms, T2 = 500ms, T3 = 5s (compile-time constants).
6. Insert the page at the **tail** of the determined cooldown region in the **same list** the worker was scanning.
7. Update the region indicator for that list to `COOLDOWN` (via CAS on `evict_flags`).
8. If the worker was scanning **List 1** and the page is in List 2 (`dirty_pages_region != NONE`): also insert into the corresponding cooldown region in List 2 and set `dirty_pages_region = COOLDOWN`.

### Cooldown retry timeouts

Pages in cooldown regions become eligible for retry after a per-tier timeout elapses since `last_retry_ts`:

| Cooldown tier | Retry timeout |
|---|---|
| CD-transient | 1ms |
| CD1 | 5ms |
| CD2 | 10ms |
| CD3 | 100ms |
| CD-long | 2s |

### Cooldown scanning

Workers scan cooldown regions of **their own list** using per-list atomic timers (see [Cooldown scanning by workers](EVICTION-SCANNING.md#cooldown-scanning-by-workers)). When a page is moved out of cooldown:
- If the page's `urgent` flag is set (List 2 only): place the page in the **urgent queue region** of List 2. Set `dirty_pages_region = URGENT`. This ensures pages that were marked urgent before entering cooldown resume urgent processing.
- Otherwise: place the page at the **LRU end** (head of LRU region) of the scanning worker's list. Update the region indicator to `LRU`.
- Cross-list cooldown coordination rules are applied (see [Cross-list cooldown coordination](EVICTION-SCANNING.md#cross-list-cooldown-coordination)). Cross-list operations are done **sequentially** after releasing the current list's locks.
- The page is **not** processed directly by the scanning worker — it is made available for other workers to pick up during normal LRU or urgent queue scanning.

### Cooldown clearing

When a page is successfully processed (evicted or transformed/written):
- Clear `cooldown_enter_ts = 0`, `last_retry_ts = 0`, `cooldown_retry_count = 0`, and `cooldown_reason = 0`.
- The page is either removed from all lists (eviction) or stays in / is reinserted into the appropriate LRU regions (successful transform/write processing; see [Worker completion](#worker-completion)).

### Cooldown statistics

The following statistics should be tracked for the cooldown subsystem:

| Statistic | Type | Description |
|---|---|---|
| Entries per cooldown region | Gauge | Current number of pages in each cooldown tier (CD-transient, CD1, CD2, CD3, CD-long), per list. |
| Cooldown entry rate per tier | Counter | Number of pages entering each cooldown tier per unit time. |
| Cooldown promotion rate per tier | Counter | Number of pages moving from cooldown back to an LRU region per unit time. |
| Bypass eviction rate | Counter | Number of pages that were evicted directly from cooldown (e.g., file close) without going through the LRU region. |
| Reason distribution in CD-long | Histogram | Breakdown of `cooldown_reason` codes among pages currently in CD-long. |
| Max time in cooldown | High-water mark | Maximum observed `now - cooldown_enter_ts` across all pages in cooldown, per list. |

---

## Corner cases and expected handling

1. **Cross-list race: both workers pick the same page from different lists**
   - Worker A removes page from List 1, locks the ref (CAS succeeds). Worker B encounters the same page in List 2 and removes it. Worker B tries to lock the ref (CAS fails). Worker B moves the page to CD-transient in List 2 (sets `dirty_pages_region = COOLDOWN`).
   - Worker A, upon successful eviction, removes the page from all lists. If Worker B already moved the page to List 2 cooldown, Worker A removes it from there (O(1) LTAILQ removal via link pointers). Sets both region indicators to `NONE`.
   - If Worker A fails, the page goes to List 1 cooldown (sets `all_pages_region = COOLDOWN`) and also to List 2 cooldown (sets `dirty_pages_region = COOLDOWN`). Worker B may have also placed it in CD-transient. The second placement is a no-op if the page is already in a cooldown region (check `dirty_pages_region` before inserting).

2. **Clean worker dequeues a page that became dirty**
   - Insert the page into the urgent queue region in List 2 with the urgent flag set. Set `dirty_pages_region = URGENT`.
   - The page's `all_pages_region` stays `NONE` (REMOVE intent persists).
   - Release ref lock and skip.

3. **Non-clean worker pops a page with urgent flag but no urgent work needed**
   - Check page state. If no work is needed, clear the urgent flag, release lock, and reinsert into List 2 LRU region at MRU end (if still dirty) or remove from List 2 (if clean).

4. **Page in cooldown region needs to be evicted (file close)**
   - File close removes the page from all lists (including cooldown regions) directly. LTAILQ supports O(1) removal from any position. No special handling needed.

5. **Page visibility after processing failure**
   - The page goes to a cooldown region, not back to the LRU region. It will be retried after the cooldown timeout.

6. **Multiple failures escalate cooldown tier**
   - The tier is determined by elapsed time since `cooldown_enter_ts`, so repeated failures naturally escalate the page to longer cooldown tiers as time passes.

7. **Page in cooldown that becomes clean (e.g., modifications rolled back)**
   - When the cooldown scanner moves the page to the LRU end, other workers will assess its state when they pick it up. A clean page in List 2 would be removed from List 2 by the processing worker (no dirty work needed, set `dirty_pages_region = NONE`). If the page is also not in List 1 (`all_pages_region == NONE`), a clean worker scanning List 1 won't find it, but the non-clean worker already handles the REMOVE intent.

8. **Urgent request for a page already in cooldown**
   - Move the page from cooldown to the urgent queue region in List 2, set urgent flag, set `dirty_pages_region = URGENT`. The page leaves cooldown for immediate processing.

9. **Page removed from List 1 by clean worker, moved to List 2 urgent queue, but dirty pressure drops**
   - The page sits in the urgent queue region of List 2. Even without dirty pressure, non-clean workers check the urgent queue first and will process it. The `all_pages_region == NONE` ensures the REMOVE intent is honored.
