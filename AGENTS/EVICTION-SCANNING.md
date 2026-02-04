## Worker scanning strategy

There is **no dedicated Eviction Server thread**. Workers scan LRU regions directly and process pages inline. There is also **no dedicated background cooldown worker** — workers scan cooldown regions opportunistically using atomic timers.

Each worker follows this cycle:

1. **Check cooldown regions** (atomic timer gate, see [Cooldown scanning by workers](#cooldown-scanning-by-workers)).
2. **Check urgent queue** (non-clean workers only): if the **urgent queue region** in List 2 is non-empty → pop and process the first page from the urgent queue.
3. **Check thresholds** to decide whether and what to scan:
   - **Non-clean workers**: if dirty cache above `eviction_dirty_target`, or updates above `eviction_updates_target` → scan List 2 LRU region.
   - **Clean workers**: if cache fill above `eviction_target` → scan List 1 LRU region.
4. **Scan LRU region**: pop the first eligible page from the LRU end, lock the page, and process it (see [Worker behavior](WORK-QUEUES-AND-WORKERS.md#worker-behavior)).
5. **No work available**: if thresholds are not exceeded and the urgent queue is empty, the worker sleeps until a wakeup signal or a timeout interval.

Notes:
- Workers only run when eviction is needed (any configured threshold is hit) **or** the urgent queue is non-empty.
- The **urgent queue** is populated by direct callers (checkpoint, oversized pages, clean-worker dirty redirects) and drained by non-clean workers (see [Urgent work](WORK-QUEUES-AND-WORKERS.md#urgent-work)).

---

## Threshold definitions

### `eviction_dirty_target` vs `eviction_updates_target`

These are separate thresholds monitoring different metrics:
- **`eviction_dirty_target`** (default 5%): percentage of cache containing dirty page bytes (`bytes_dirty_leaf`).
- **`eviction_updates_target`** (default: auto-calculated as `eviction_dirty_target / 2`): percentage of cache containing unresolved update bytes (`bytes_updates`).

Both thresholds trigger the same action: non-clean workers scan the **Dirty/updated LRU region** in List 2. They are kept separate because they measure different aspects of cache pressure (a page can have many unresolved updates without high dirty byte counts, and vice versa). Either threshold being exceeded is sufficient to trigger non-clean worker scanning.

---

## Cooldown scanning by workers

Workers scan cooldown regions **opportunistically** using **atomic timers**. Each list has its own set of timers — one `uint64_t` per cooldown tier per list. Workers only scan cooldown regions of **their own list** (clean workers scan List 1 cooldown; non-clean workers scan List 2 cooldown).

Total timers: 5 tiers × 2 lists = **10 timers**.

### Timer format

Each timer is a `uint64_t`:
- **Lowest bit (bit 0)**: "scan in progress" flag. Set while a worker is actively scanning this tier.
- **Remaining bits (bits 1–63)**: nanosecond timestamp of the last scan completion. The lowest bit is zeroed out (masked) when reading the timestamp, which loses 1 ns of precision — negligible given nanosecond granularity.

### Timer check protocol

When a worker's cycle begins, it checks cooldown timers for **its own list only**:

1. Check timers in **shortest-to-longest** order: CD-transient, CD1, CD2, CD3, CD-long.
2. For each timer:
   a. Atomically load the timer value.
   b. If the **flag bit is set** (another worker is already scanning a cooldown tier on this list) → **stop all cooldown scanning** for this cycle and proceed to threshold-based LRU scanning. It is acceptable for longer-delay cooldown tiers to not be scanned while shorter-delay tiers are being processed.
   c. Compute `elapsed = now - (timer_value & ~1)` (mask out the flag bit to get the timestamp).
   d. If `elapsed < scan_interval_for_this_tier` → skip this tier, check the next one.
   e. If `elapsed >= scan_interval_for_this_tier` → attempt CAS: `old_value → (old_value | 1)` (set the flag bit).
      - CAS fails → another worker won the race → **stop all cooldown scanning** for this cycle.
      - CAS succeeds → this worker now **owns** this tier's cooldown region. Scan it.
3. After scanning a tier, store `(now & ~1)` (current timestamp with flag bit cleared) to the timer. This atomically updates the timestamp and clears the "in progress" flag, allowing other workers to scan this tier in a future cycle.
4. **Continue** checking the next tier (step 2) — a worker can scan multiple tiers in a single cycle if their intervals have elapsed, as long as no "scan in progress" flag is encountered.

Note: because workers check timers in shortest-to-longest order, if a longer-delay tier is being scanned by another worker, the current worker can still scan shorter-delay tiers first. The worker stops only when it encounters a "scanning" flag, which means a shorter-or-equal tier is already being handled.

### Scan intervals per tier

| Cooldown tier | Scan interval |
|---|---|
| CD-transient | 1ms |
| CD1 | 5ms |
| CD2 | 10ms |
| CD3 | 100ms |
| CD-long | 2s |

### Cooldown scan behavior

When scanning a cooldown region:
- Walk entries from the **head** (oldest entries first).
- Stop if the next element is the region tail sentinel (`WT_REF_EVICT_SENTINEL` set); sentinel entries are boundaries, not page entries.
- For each entry, check if `now - last_retry_ts >= retry_timeout_for_this_tier`.
- If eligible (timeout elapsed):
  - If the page's `urgent` flag is set and this is **List 2**: move the page to the **urgent queue region** of List 2. Set `dirty_pages_region = URGENT`. This ensures pages that were marked urgent before entering cooldown resume urgent processing.
  - Otherwise: move the page to the **LRU end** (head of LRU region) of this worker's list. Update the region indicator to `LRU`.
  - The page is **not** processed directly by the scanning worker — it is made available for other workers to pick up during normal LRU or urgent queue scanning.
  - Apply cross-list cooldown coordination rules (see [Cross-list cooldown coordination](#cross-list-cooldown-coordination)). Cross-list operations are performed **sequentially** after releasing the current list's locks (see [Inter-list lock ordering](LRU-LISTS.md#inter-list-lock-ordering)).
- If not eligible (timeout has not elapsed) → **stop scanning** this region. Entries are ordered by insertion time; since new entries are appended at the tail, later entries have more recent `last_retry_ts` values and will also not be eligible. This bounds the scan duration: the worker only examines the prefix of entries that have been in cooldown long enough.

### Cooldown scan concurrency

At most **one** worker scans a given cooldown tier of a given list at any time (enforced by the CAS on the timer flag bit). Other workers that need to **insert** into the same cooldown region operate at the **tail** (new entries are always appended at the tail), while the scanning worker operates at the **head**. Since the scanning worker stops as soon as it encounters a non-eligible entry, the time spent scanning is short and bounded, minimizing contention between the scanning worker and workers inserting at the tail.

One opportunity for contention is when another thread wants to **remove** a page from a cooldown region (e.g., file close, page touch) while the scanning worker is near that page. However, the scanning worker's per-entry check (load `last_retry_ts`, compare to threshold) is fast, and the window of contention is small.

### Cross-list cooldown coordination

When moving pages out of cooldown, the scanner checks the **other list's region indicator** and moves the page there too if it is also in cooldown. All cross-list operations are done **sequentially** — first complete the move in the scanner's own list, then operate on the other list (see [Inter-list lock ordering](LRU-LISTS.md#inter-list-lock-ordering)). Re-check the region indicator after acquiring the other list's locks.

1. **Page moved out of List 1 cooldown** → check `dirty_pages_region`:
   - If `COOLDOWN`: also move out of List 2 cooldown. If the page's `urgent` flag is set, move to the **urgent queue region** of List 2 and set `dirty_pages_region = URGENT`. Otherwise, move to the LRU end of List 2's LRU region and set `dirty_pages_region = LRU`.
   - If `LRU` or `URGENT`: do not change the page's position in List 2.
   - If `NONE`: do not insert into List 2.
   - Set `all_pages_region = LRU`.

2. **Page moved out of List 2 cooldown** → check `all_pages_region`:
   - If `COOLDOWN`: also move out of List 1 cooldown → to the LRU end of List 1's LRU region. Set `all_pages_region = LRU`.
   - If `LRU`: do not change the page's position in List 1.
   - If `NONE`: do not insert into List 1 (the page was removed from List 1 for eviction and should remain absent — the REMOVE intent is still active).
   - Set `dirty_pages_region` to `URGENT` if the page's `urgent` flag is set (moved to urgent queue), or `LRU` otherwise (moved to LRU region).

---

## Invariant: pages must be in lists consistently

For consistency and debuggability:

- Any in-memory page that is potentially eligible for eviction must always be present in **one or more** lists, in some region (LRU, cooldown, or urgent queue).
- You must **plan and document** page state transitions from the eviction subsystem's perspective, and map each transition to movement between regions or lists.

---

## State transitions

1. **Leaf page loaded into memory** → allocate `ref->evict` (see [Allocation and initialization](EVICTION-METADATA.md#allocation-and-initialization-page-in)), then insert into **List 1 LRU region** at MRU end. Set `all_pages_region = LRU`. (Internal pages also get `ref->evict` allocated but are **not** inserted into any list; see [Internal page policy](EVICTION-POLICIES.md#internal-page-policy).)

2. **Page becomes dirty/updated** → if the page is already anywhere in List 2 (`dirty_pages_region != NONE`), its position in List 2 is **unchanged**. Otherwise, insert into **List 2 LRU region** at MRU end and set `dirty_pages_region = LRU`. (Leaf pages only; internal pages follow the [Internal page policy](EVICTION-POLICIES.md#internal-page-policy).)

3. **Page requires urgent transform/write** → insert into the **urgent queue region** in List 2. Set the page's `urgent` flag to prevent duplicate insertion. Set `dirty_pages_region = URGENT`. The page's position in List 1 is unchanged.

4. **Clean worker selects page from List 1 LRU**:
   - Remove page from List 1 LRU region. Set `all_pages_region = NONE`.
   - Lock the page for exclusive access.
   - If the page is clean: process it (evict from memory).
   - If the page is dirty: insert into the **urgent queue region** in List 2, set `urgent` flag, set `dirty_pages_region = URGENT`. The page's `all_pages_region` remains `NONE` (REMOVE intent persists). The non-clean worker will handle it.

5. **Non-clean worker selects page from List 2 (urgent queue or LRU region)**:
   - Remove page from the urgent queue or LRU region in List 2. Set `dirty_pages_region = NONE`.
   - Lock the page for exclusive access.
   - Check `all_pages_region`:
     - If `NONE`: intent is **remove from memory**. Reconcile/write if dirty, then evict.
     - If `LRU` or `COOLDOWN`: intent is **transform/write only** (reduce dirty cache). Reconcile/write, page stays in memory.
   - If the page has the `urgent` flag set: clear it.

6. **Worker cannot complete (failure)** → page is moved to the appropriate **cooldown region** in the **same list** the worker was scanning. Cooldown tier is determined by failure type and duration (see [Cooldown mechanics](WORK-QUEUES-AND-WORKERS.md#cooldown-mechanics)). Update the region indicator for that list to `COOLDOWN`. Additionally, if the worker was scanning **List 1**, and the page is in List 2 (`dirty_pages_region != NONE`), also move the page to the corresponding cooldown region in List 2 and set `dirty_pages_region = COOLDOWN`. This prevents non-clean workers from wasting time on a page that is in cooldown from a List 1 failure. See [Worker completion](WORK-QUEUES-AND-WORKERS.md#worker-completion) for the full rules.

7. **Page remains in memory after successful processing (transform/write intent, selected from List 2):**
   - The page's position in **List 1** is kept intact (it was never removed from List 1).
   - Reinsert into **List 2 LRU region** at the MRU end if still dirty (set `dirty_pages_region = LRU`); remove from List 2 if now clean (set `dirty_pages_region = NONE`).
   - Clear cooldown state (reset `cooldown_enter_ts`, `last_retry_ts`, `cooldown_retry_count`, and `cooldown_reason` to 0).

8. **Page is evicted (removed from memory)** → deallocate `ref->evict` using the lock-based deallocation protocol: remove from all lists with double-check, then free the struct (see [Deallocation and cleanup](EVICTION-METADATA.md#deallocation-and-cleanup-page-out) and [Removing page from memory](LTAILQ-EXAMPLES.md#removing-page-from-memory-or-destroying-wt_ref)). Reset `ref->last_promotion_timestamp = 0`.

9. **Internal page loses its last active child** → insert into **List 1 LRU region** (and **List 2 LRU region** if dirty) at the **least-recently-used end** (LRU_HEAD sentinel) so it is evicted promptly. Set `all_pages_region = LRU` (and `dirty_pages_region = LRU` if inserted into List 2).

10. **Child page instantiated under an internal page that is in lists** → remove the internal page from all lists (it now has an active child and must stay in memory). Set `all_pages_region = NONE` and `dirty_pages_region = NONE`.

11. **Cooldown timeout elapsed** → page is moved from cooldown region by the worker that won the cooldown scan timer. If the page's `urgent` flag is set (List 2 only), move to the **urgent queue region** and set the region indicator to `URGENT`. Otherwise, move to the LRU end of the LRU region and set the region indicator to `LRU`. Cross-list coordination rules apply (see [Cross-list cooldown coordination](#cross-list-cooldown-coordination)).

12. **Failed processing (selected from List 1)**:
    - Move the page to the appropriate **cooldown region in List 1**. Set `all_pages_region = COOLDOWN`.
    - If the page is in List 2 (`dirty_pages_region != NONE`), also move it to the corresponding **cooldown region in List 2**. Set `dirty_pages_region = COOLDOWN`.
    - When the page later exits cooldown in List 1 (via transition 11), it returns to List 1's LRU region. A clean worker will pick it up again, re-establishing REMOVE intent (`all_pages_region = NONE`) at that point.

13. **Page touched (accessed by user code)** → promote to MRU end of all lists where the page is present, **except**: if `dirty_pages_region == URGENT` (page is in the urgent queue of List 2), its position in List 2 is unchanged. See [Page touch algorithm](LRU-LISTS.md#page-touch-algorithm).
