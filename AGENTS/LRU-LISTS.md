## LRU data structure

- Use the **TAILQ** library as the basis for the doubly-linked lists.
- Fork the existing TAILQ implementation into a new file **`queue_locked.h`** with an alternative set of macros prefixed by **`LTAILQ_*`** (locked TAILQ). This provides a clean namespace for the locking extensions without modifying or colliding with the standard TAILQ macros.

List orientation:
- **Head** = **cold end** (longest-deferred cooldown pages, then least-recently used pages).
- **Tail** = **hot end** (most-recently used). New pages and promoted pages are inserted at the MRU end of the LRU region.
- Workers scan the LRU region from **LRU end to MRU end**.

---

## List layout and regions

Each list is a single LTAILQ containing multiple **regions** separated by **sentinel pairs**. A page occupies exactly one region within each list at any given time.

### List 1 — All pages list

Tracks all eligible in-memory pages, except internal pages with active children (see [Internal page policy](EVICTION-POLICIES.md#internal-page-policy)) and pages belonging to in-memory tables (see [In-memory table policy](EVICTION-POLICIES.md#in-memory-table-policy)). Clean eviction is done from this list.

```
HEAD - [CD-long] - [CD3] - [CD2] - [CD1] - [CD-transient] - [All pages LRU (LRU→MRU)] - TAIL
```

### List 2 — Dirty/updated pages list

Tracks dirty/updated pages (subset of in-memory pages). Non-clean eviction, transform/write, and urgent work is done from this list.

```
HEAD - [CD-long] - [CD3] - [CD2] - [CD1] - [CD-transient] - [Urgent queue] - [Dirty/updated LRU (LRU→MRU)] - TAIL
```

### Region descriptions

| Region | Purpose |
|---|---|
| **LRU** | Active pages ordered by recency. Workers scan from the LRU end toward the MRU end (tail). |
| **Urgent queue** | (List 2 only) Dedicated region for pages requiring immediate processing regardless of dirty/updates pressure. Non-clean workers check this region first, before the LRU region. |
| **CD-transient** | Pages that failed processing due to transient conditions (lock contention, hazard pointer). Retry timeout: 1ms. Checked by workers via atomic timers. |
| **CD1, CD2, CD3** | Escalating cooldown tiers for pages with persistent failures. Retry timeouts: 5ms, 10ms, 100ms. Checked by workers via atomic timers. |
| **CD-long** | Pages blocked by long-lived conditions (checkpoint sync, materialization frontier, visibility constraints). Retry timeout: 2s. Checked by workers via atomic timers. |

---

## Urgent queue region (List 2 only)

The **urgent queue** is a dedicated region in List 2, positioned between the CD-transient cooldown region and the LRU region. Pages requiring immediate processing (checkpoint, oversized pages, dirty pages redirected by clean workers) are inserted into this region.

- **Insertion**: callers (`__wt_evict_page_urgent`, `__wt_evict_page_soon`, checkpoint, clean-worker dirty redirect) check the per-page `urgent` flag in `WT_REF_EVICT`. If not already set: set the flag, insert into the urgent queue region, and set `dirty_pages_region = URGENT`.
- **Processing**: non-clean workers check the urgent queue first — if it is non-empty, they pop a page from it before checking thresholds for the LRU region. When a worker processes a page with the `urgent` flag set, it clears the flag. On pop, set `dirty_pages_region = NONE`.
- **Wakeup**: non-clean workers wake up and process from the urgent queue even when dirty/updates thresholds are not exceeded.
- **Emptiness check**: workers check whether the urgent queue is non-empty by comparing the region's sentinels (`URGENT_HEAD->next != URGENT_TAIL`). No separate counter is needed.
- **Cooldown return**: pages moving from cooldown regions go to the **LRU region** unless the `urgent` flag is set, in which case the page goes to the **urgent queue region** instead. Set `dirty_pages_region` to `LRU` or `URGENT` accordingly. This ensures pages that were marked urgent before entering cooldown resume urgent processing after cooldown.

---

## Region tracking (`all_pages_region` and `dirty_pages_region`)

Two bit fields in the `evict_flags` byte of `WT_REF_EVICT` track which region of each list the page currently occupies. Both use the same set of values:

| Value | Meaning |
|---|---|
| `WT_REF_REGION_NONE` (0) | Not in this list. Either removed for eviction processing, page not in memory, or internal page with active children. |
| `WT_REF_REGION_LRU` (1) | In the LRU region of this list. |
| `WT_REF_REGION_COOLDOWN` (2) | In a cooldown region of this list. |
| `WT_REF_REGION_URGENT` (3) | In the urgent queue region (`dirty_pages_region` only; `all_pages_region` never uses this value). |

**`all_pages_region`** (bits 1-2 of `evict_flags`) — tracks List 1 region. Serves two purposes:
1. **Implicit REMOVE intent**: when `all_pages_region == NONE` and the page is being processed from List 2, the worker knows the page should be removed from memory (not just written/transformed).
2. **Cross-list cooldown coordination**: when moving a page out of List 2 cooldown, the scanner checks `all_pages_region` to decide whether to also move the page in List 1 (see [Cross-list cooldown coordination](EVICTION-SCANNING.md#cross-list-cooldown-coordination)).

**`dirty_pages_region`** (bits 3-4 of `evict_flags`) — tracks List 2 region. Serves two purposes:
1. **Fast region lookup for List 2**: determines whether the page is in List 2's LRU, cooldown, urgent queue, or not in List 2 at all, without scanning sentinels.
2. **Cross-list cooldown coordination**: when moving a page out of List 1 cooldown, the scanner checks `dirty_pages_region` to decide whether to also move the page in List 2.

The urgent flag (bit 0 of `evict_flags`) is also stored in the same byte. The sentinel flag (bit 5 of `evict_flags`) is set on sentinel elements, indicating a special region-boundary sentinel marker that cannot be removed as an eviction candidate. Sentinel identity is determined by comparing the element address with predefined sentinel pointers stored in the connection's eviction state. See [Eviction metadata memory layout](EVICTION-METADATA.md#eviction-metadata-memory-layout-wt_ref_evict) for the full bit layout and access discipline.

---

## Sentinel pair boundaries

Each boundary between adjacent regions consists of **two sentinel elements**: one serves as the **tail** of the preceding region, the other serves as the **head** of the following region. These sentinel elements are permanently allocated and never removed.

```
... [region A entries] ↔ REGION_A_TAIL ↔ REGION_B_HEAD ↔ [region B entries] ...
```

Rationale:
- Each region has its own head and tail sentinels, providing clear ownership for per-element locking.
- Inserting at the tail of region A requires locking REGION_A_TAIL and its predecessor — no need to touch region B's sentinels.
- Popping from the head of region B requires locking REGION_B_HEAD and its successor — no need to touch region A's sentinels.
- This eliminates locking conflicts between adjacent regions.

Sentinel elements are represented as normal `WT_REF_EVICT` structs with `WT_REF_EVICT_SENTINEL` set in `evict_flags`.
- They participate in the per-element locking protocol as regular elements.
- They carry their own lock state in `tqe_prev` bit 0 (same as any LTAILQ element).
- They are permanent region-boundary sentinels and must never be removed as eviction candidates.
- Their specific role (for example `CD2_TAIL` vs `CD1_HEAD`) is identified by comparing their address with predefined sentinel pointers in the connection's eviction-related struct.

Sentinel counts:
- **List 1**: 6 regions × 2 sentinels = **12** sentinel elements.
- **List 2**: 7 regions × 2 sentinels = **14** sentinel elements (includes urgent queue region).

Sentinels are distinguished from regular page entries via `WT_REF_EVICT_SENTINEL` in `evict_flags` (no separate sentinel struct).

---

## Membership tracking

Membership is determined by **LTAILQ link pointers** (not flags).

Rationale:
- Better consistency with **TAILQ** semantics.
- Avoid flag/pointer state divergence.
- TAILQ links are **NULL** when not in list (if properly initialized).

Implementation:
- All `LTAILQ_ENTRY` fields in `WT_REF_EVICT` are zero-initialized when the evict struct is allocated (see [Eviction metadata memory layout](EVICTION-METADATA.md#eviction-metadata-memory-layout-wt_ref_evict)); NULL links mean "not in any list".
- Check `ref->evict->all_lru_link.tqe_prev != NULL` to determine if in **List 1** (All pages).
- Check `ref->evict->dirty_lru_link.tqe_prev != NULL` to determine if in **List 2** (Dirty/updated pages).
- Macro helper: `WT_REF_IN_ANY_LIST(ref)` checks `ref->evict` is non-NULL and then checks both link fields.

Note: A page is in exactly **one region** of each list it belongs to. The specific region is determined by the page's position relative to the sentinels. The `all_pages_region` and `dirty_pages_region` bit fields in `evict_flags` provide fast region lookup for List 1 and List 2 respectively, without scanning sentinels.

---

## Cross-list coordination

A page can be in both List 1 and List 2 simultaneously, occupying different regions of each list independently.

The intent of work depends on the page's **`all_pages_region`** value, which reflects whether the page was removed from List 1:

- **`all_pages_region == NONE`** → the page was removed from List 1 for eviction → intent is **removal from memory**. The worker reconciles/writes if dirty, then evicts. **Exception**: pages belonging to in-memory tables (`WT_BTREE_IN_MEMORY`) always have `all_pages_region == NONE` because they are never placed in List 1 (see [In-memory table policy](EVICTION-POLICIES.md#in-memory-table-policy)). Workers must check `F_ISSET(btree, WT_BTREE_IN_MEMORY)` to distinguish this case — in-memory table pages are reconciled/transformed in place and are **never** removed from memory.
- **`all_pages_region == LRU` or `COOLDOWN`** → the page is still in List 1 → intent is **transform and/or write to disk** (page stays in memory). The page's position in List 1 is kept intact.

Detailed rules:

- When a **clean worker selects a page from List 1 LRU**:
  - Remove from List 1 LRU. Set `all_pages_region = NONE`.
  - If clean: evict from memory.
  - If dirty: insert into the **urgent queue region** in List 2, set the `urgent` flag, and set `dirty_pages_region = URGENT`. The `all_pages_region` remains `NONE` (REMOVE intent persists). A non-clean worker will handle reconciliation + removal.

- When a **non-clean worker selects a page from List 2** (urgent queue or LRU region):
  - Remove from the urgent queue or LRU region in List 2.
  - Check `all_pages_region`:
    - `NONE` and **not** an in-memory table → reconcile/write if dirty, then evict. Remove from all lists.
    - `NONE` and in-memory table (`WT_BTREE_IN_MEMORY`) → reconcile/transform only (`WT_REC_IN_MEMORY | WT_REC_SCRUB`), page stays in memory. Not evicted.
    - `LRU` or `COOLDOWN` → reconcile/write, page stays in memory. Position in List 1 unchanged.

Post-processing behavior follows from the intent:
- **REMOVE intent** (`all_pages_region == NONE`): on success the page is evicted and removed from all lists. On failure the page goes to the **same list's** cooldown region. If the worker was scanning List 1, the page is also placed into List 2's cooldown (if the page is in List 2) so it is not in List 2's LRU area. See [Worker completion](WORK-QUEUES-AND-WORKERS.md#worker-completion) for details.
- **Transform/write intent** (`all_pages_region == LRU` or `COOLDOWN`): on success the page remains in memory and stays in List 1 (position intact). On failure the page goes to List 2 cooldown.

### Dirty page insertion into List 2

When a page becomes dirty or is updated: if the page is already anywhere in List 2 (`dirty_pages_region != NONE`, including `LRU`, `COOLDOWN`, or `URGENT`), its position in List 2 is **unchanged**. Otherwise, the page is inserted at the **MRU end** of List 2's LRU region. Set `dirty_pages_region = LRU`.

---

## Per-element locking strategy

Use **per-element locking** for all list structures (not a single lock per list).

Rationale:
- A single list lock becomes a contention hotspot when many threads promote, insert, or remove pages concurrently.
- Per-element locking allows concurrent operations on different parts of the list.

### Lock mechanism

The lock is embedded in the **`tqe_prev`** field of each `LTAILQ_ENTRY`:

- **Lock bit**: The lowest bit (bit 0) of the `tqe_prev` pointer serves as a spinlock. When bit 0 is set, the element is locked.
- **Lock acquisition**: CAS on `tqe_prev` to atomically set bit 0 (from 0 → 1). See [Lock acquisition protocol](#lock-acquisition-protocol) for retry behavior.
- **Lock release**: Atomic store to `tqe_prev` with bit 0 cleared (store the final clean pointer value).
- **Address extraction**: To read the actual `tqe_prev` pointer value, mask out bit 0.

This works because:
- `tqe_prev` is a `struct type **` pointer that always points to naturally aligned memory (a `tqe_next` field within another element, or the `tqh_first` field of the list head). Since these pointer-sized fields are at least pointer-aligned on all target platforms, bit 0 is always 0 in a valid address.
- All useful list regions are bounded by **sentinel elements**. The `tqe_prev` of every element within a region points to a valid predecessor's `tqe_next` field — it is never NULL. This eliminates the need for NULL-pointer special cases in locking logic.

No separate lock field or external lock structure is needed. The lock state is carried within the existing `tqe_prev` pointer at zero additional memory cost.

### Lock acquisition protocol

Two lock operations are provided: a **full lock** (blocking, with retries) and a **try lock** (non-blocking, single attempt).

#### Full lock (`LTAILQ_LOCK`)

1. Use a CAS operation to set the "locked" bit (bit 0 of `tqe_prev`). Upon failure, retry up to **N1** times (default: 50).
2. Yield and retry step 1 up to **N2** times (default: 20).
3. Increment an FTDC counter for "long running lock". Retry all above up to **N3** times (default: 20).
4. Increment an FTDC counter for "possible deadlock avoidance". Return lock failure to the caller.

After receiving a lock failure, the caller must unlock all entries that it currently holds and retry the entire operation.

#### Try lock (`LTAILQ_TRY_LOCK`)

A single CAS attempt to set the "locked" bit. Returns success or failure immediately — no retry loops.

### Accessor functions

All access to the embedded lock and the underlying pointer must go through dedicated **functions**. Inline bit calculations (manual masking, shifting, or casting of `tqe_prev`) must **never** appear directly in code outside of these functions.

Required functions (defined in `queue_locked.h`):

| Function | Purpose |
|---|---|
| `LTAILQ_PREV_ADDR(raw)` | Extract the clean pointer from a `tqe_prev` value (masks out bit 0). |
| `LTAILQ_IS_LOCKED(elm, field)` | Return whether the element's `tqe_prev` has bit 0 set. |
| `LTAILQ_LOCK(elm, field)` | Acquire the spinlock using the [full lock protocol](#full-lock-ltailq_lock). Returns success or lock failure. |
| `LTAILQ_TRY_LOCK(elm, field)` | Single CAS attempt to set bit 0 of `elm->field.tqe_prev`. Returns success or failure immediately. |
| `LTAILQ_UNLOCK(elm, field, clean_prev)` | Release the spinlock: atomic store of the clean `tqe_prev` value (bit 0 cleared). |
| `LTAILQ_SET_PREV(elm, field, addr)` | Store a new `tqe_prev` address while preserving the current lock bit. |

Note: `LTAILQ_UNLOCK` takes the clean pointer value explicitly (not just a flag-clear) because the `tqe_prev` pointer may have been updated during the locked operation (e.g., after an insert or remove that changes the element's predecessor). The caller stores the final correct `tqe_prev` value with bit 0 cleared in a single atomic write.

### Alignment requirement

Since the lowest bit of `tqe_prev` is used as a lock, all addresses stored in `tqe_prev` must be at least **2-byte aligned**. This means:

- All `WT_REF_EVICT` structs (page entries and sentinel entries) must be allocated at 2-byte-aligned addresses.
- The `LTAILQ_HEAD` struct itself must be 2-byte-aligned (its `tqh_first` field address is stored in the first element's `tqe_prev`).
- A `WT_ASSERT` checking `((uintptr_t)ptr & 1) == 0` must be placed at:
  - **Every site where `LTAILQ_ENTRY`-containing structs are allocated** (both page entry allocation and sentinel allocation).
  - **Every `LTAILQ_HEAD` initialization site**: assert `((uintptr_t)&head->tqh_first & 1) == 0` before the head is used. The first element's `tqe_prev` will store the address of `tqh_first`, so this address must be even.
- In practice, `__wt_calloc_one` and standard allocators return memory aligned to at least `sizeof(void *)`, which exceeds this requirement. Similarly, struct members are at least pointer-aligned. The assertions guard against future changes to allocation or layout strategy.

### Lock ordering and protocol

- Scanning always proceeds **head-to-tail**, ensuring a consistent lock ordering that prevents deadlocks within a single list.
- When locking an element for mutation (insert, remove, or move), acquire locks on **prev → current → next** in list order.
- When scanning from any position, lock the next element and then release the previous element (hand-over-hand / crabbing).
- The element lock protects only the **list entry** (prev/next pointers), not the page itself. It is safe to hold a reference to the page without holding its list-entry lock.

See [LTAILQ-EXAMPLES.md](LTAILQ-EXAMPLES.md) for detailed step-by-step locking algorithms for insert, remove, promotion, and work item dequeue operations.

### Inter-list lock ordering

There is **no need to lock elements in multiple lists at the same time**. Whenever a page needs to change its position in multiple lists (e.g., cross-list cooldown coordination, removal during eviction, page touch), the operations are done **sequentially** — one list at a time:

1. Acquire per-element lock(s) in **List A**. Perform the operation (move, remove, or insert). Release lock(s) in List A.
2. Acquire per-element lock(s) in **List B**. **Re-check** whether the operation is still needed (conditions may have changed between releasing List A locks and acquiring List B locks). Perform or skip accordingly. Release lock(s) in List B.

This eliminates inter-list deadlocks entirely — no thread ever holds locks from both lists. The re-check after acquiring the second lock handles races where another thread modified the page's state between the two operations.

List-entry pointers are per-list and independent (`all_lru_link` vs `dirty_lru_link`), so operations on one list's entry do not interfere with the other list's entry.

---

## Time-gated LRU promotion

Objective: reduce lock contention caused by frequently-accessed ("hot") pages by limiting how often a page can be promoted in the LRU region.

Use a **single 64-bit timestamp** shared across both lists:
- Add `wt_shared uint64_t last_promotion_timestamp` directly to **WT_REF** (not in the `WT_REF_EVICT` struct). This field is checked on every page access as a lock-free atomic read; keeping it in WT_REF avoids dereferencing `ref->evict` on the hottest path (see [Eviction metadata rationale](EVICTION-METADATA.md#rationale-1)).
- Initialize to 0 when **WT_REF** is allocated.
- Reset to 0 when the page is evicted (`ref->page = NULL`), as part of the evict-struct deallocation cleanup (see [Deallocation and cleanup](EVICTION-METADATA.md#deallocation-and-cleanup-page-out)).

Use a **single throttle interval** for all LRU regions:
- Add configuration constant `WT_EVICT_PROMOTION_THROTTLE_US = 1000000`.
- Apply throttling to **LRU regions** of both lists.
- **Cooldown** regions do not use promotion throttling (pages are placed there by eviction machinery, not by access pattern).

Unify insertion/promotion logic:
- Implement a single shared helper for insertion/promotion operations that:
  - accepts the target list identifier,
  - uses the configured throttle interval,
  - reads timestamps using the same timer API,
  - performs insertion/promotion consistently.

### Concurrency and locking (double-checking pattern)

1. First check (lock-free / atomic read):
   - Atomically load the page's last-promotion timestamp.
   - If now - last < interval, return without acquiring any element lock.
2. Second check (under the element lock):
   - Acquire the per-element lock(s) needed for the operation (see locking strategy above).
   - Re-read (or re-validate) the last-promotion timestamp.
   - If throttling still applies, release lock(s) and do nothing.
   - If promotion is allowed:
     - perform the list promotion/reinsertion,
     - record the new timestamp while still holding the lock(s),
     - then release the lock(s).

### Throttle semantics for promotions vs insertions

Case A: Page already in LRU region (promotion)
```
if (page_in_lru_region) {
    now = __wt_clock(session);
    if (now - ref->last_promotion_timestamp < throttle_interval) {
        return; // Throttled, skip promotion
    }
    // Promote: move to MRU end of LRU region (just before LRU_TAIL sentinel)
    remove from current position;
    insert before LRU_TAIL sentinel;
    ref->last_promotion_timestamp = now;
}
```

Case B: Page not in list (insertion)
```
else {
    // Always insert at MRU end of LRU region (no throttling check)
    insert before LRU_TAIL sentinel;

    // Update timestamp ONLY if expired
    now = __wt_clock(session);
    if (now - ref->last_promotion_timestamp >= throttle_interval) {
        ref->last_promotion_timestamp = now;
    }
}
```

Rationale:
- Insertions always succeed (page must enter list).
- Only update timestamp if expired to reduce cache line bouncing when pages rapidly move between regions.
- Promotions within the LRU region are throttled to reduce lock contention on hot pages.

---

## Page touch algorithm

Whenever a page is **touched** (accessed by user code), it is promoted to the MRU end of all LRU lists where it is already present. This maintains true LRU ordering based on access recency.

**List 1** — the algorithm is:

1. Atomically load `evict_flags` and extract `all_pages_region`.
2. If the region is `NONE` (page is not in List 1) → **do nothing**.
3. If the region is `LRU` or `COOLDOWN`:
   - Apply the [time-gated promotion throttle](#time-gated-lru-promotion). If throttled → skip.
   - Remove the page from its current position in List 1.
   - Insert the page at the **MRU end** of the LRU region (just before LRU_TAIL sentinel).
   - Clear cooldown state: reset `cooldown_enter_ts`, `last_retry_ts`, `cooldown_retry_count`, `cooldown_reason` to 0.
   - Set `all_pages_region = LRU` (via CAS on `evict_flags`).
   - Update `ref->last_promotion_timestamp`.

**List 2** — the algorithm is:

1. Atomically load `evict_flags`. Extract `dirty_pages_region`. If the region is `URGENT` → **do nothing** for List 2. The page is awaiting immediate processing and must not be moved out of the urgent queue.
2. If the region is `NONE` → **do nothing**.
3. If the region is `LRU` or `COOLDOWN`:
   - Apply the [time-gated promotion throttle](#time-gated-lru-promotion). If throttled → skip.
   - Remove the page from its current position in List 2.
   - Insert the page at the **MRU end** of the LRU region (just before LRU_TAIL sentinel).
   - Clear cooldown state: reset `cooldown_enter_ts`, `last_retry_ts`, `cooldown_retry_count`, `cooldown_reason` to 0.
   - Set `dirty_pages_region = LRU` (via CAS on `evict_flags`).
   - Update `ref->last_promotion_timestamp`.

The lists are processed **sequentially** (one at a time) per the [inter-list lock ordering](#inter-list-lock-ordering) rules. After operating on one list, re-check `evict_flags` for the next list before operating (it may have changed concurrently).

Note: the page touch does **not** insert a page into a list where it is not already present. Insertion into List 1 happens at page-in time; insertion into List 2 happens when the page becomes dirty/updated.

---

## In-memory transform: page replacement rules

When a page is transformed in memory and **one ref is replaced by multiple new refs** (e.g., multi-reconciliation):
- **New refs must not be in any cooldown region**.
- For each **list** where the original ref is currently present:
  - Insert the new refs **immediately before the original ref** (as a contiguous block; order does **not** matter).
  - Remove the original ref from the list.
- If the original ref is **not** present in a given list, insert the new refs at the **LRU end** of the LRU region (just after the LRU_HEAD sentinel).
- Set `all_pages_region` and `dirty_pages_region` for each new ref to match the region they are placed in for each list (e.g., `LRU` if inserted into the LRU region of that list).

Rationale: the new refs inherit the original's approximate LRU position. Since LTAILQ supports O(1) insertion at any position, no special placeholder mechanism is needed. If the transform is aborted and the original ref stays, no cleanup is required — the original ref was not removed until the new refs were successfully inserted.
