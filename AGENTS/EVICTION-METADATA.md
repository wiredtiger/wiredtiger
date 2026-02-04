## Tracked object and membership model

Track **WT_REF**-backed page entries (not **WT_PAGE**) in the eviction lists. Region-boundary sentinels are separate sentinel entries represented by `WT_REF_EVICT` with `WT_REF_EVICT_SENTINEL` set.

Rationale:
- **WT_REF** is stable and persists even when a page is evicted/reloaded.
- **WT_PAGE** comes and goes (eviction sets `ref->page = NULL`).
- Tracking **WT_REF** provides stable identity across eviction/reload cycles.

Lifecycle rules:
- Insert **WT_REF** into LRU regions when the page is instantiated (`ref->page` is set), **only if it is a leaf page** (see [Internal page policy](EVICTION-POLICIES.md#internal-page-policy)).
- Remove **WT_REF** from all lists when the page is evicted (`ref->page = NULL`).
- **WT_REF** stays allocated but is **not** in any list when `ref->page == NULL`.
- **Internal pages** with active children are **never** placed in any list (see [Internal page policy](EVICTION-POLICIES.md#internal-page-policy)).

Pages can be in **both lists simultaneously** (List 1 — All pages, and List 2 — Dirty/updated pages), but occupy exactly **one region** within each list at any given time.

Rationale:
- Always evict least recently used pages regardless of status.
- Prioritize eviction of pages with certain properties (dirty) while preserving LRU order.

Implementation:
- Each list has its own `LTAILQ_ENTRY` field in the **WT_REF_EVICT** struct: `all_lru_link` (List 1), `dirty_lru_link` (List 2). To find the corresponding `WT_REF`, follow the `ref` back-pointer from the `WT_REF_EVICT` struct.
- A page can have non-NULL links in **both lists** at once, but in different regions (e.g., LRU region of List 1, cooldown region of List 2). The `all_pages_region` and `dirty_pages_region` bit fields in `evict_flags` track which region of each list the page is in.
- Sentinel elements are also represented as `WT_REF_EVICT` structs. They are identified by `WT_REF_EVICT_SENTINEL` in `evict_flags` and by address comparison against predefined sentinel pointers stored in the connection's eviction-related struct.

---

## Eviction metadata memory layout (`WT_REF_EVICT`)

Store eviction-specific metadata in a **separately allocated struct** (`WT_REF_EVICT`) rather than embedding it directly in `WT_REF`. The `WT_REF` holds only a pointer to this struct and the `last_promotion_timestamp` field.

### Rationale

- **`WT_REF` persists across eviction/reload cycles**, but eviction metadata for page entries (TAILQ links, per-element locks, eviction flags) is only meaningful when the page is in memory (`ref->page != NULL`).
- Sentinel entries are the exception: they are a small fixed set of always-allocated `WT_REF_EVICT` entries used as permanent list boundaries.
- In a typical deployment with a large dataset and a small cache, the vast majority of `WT_REF` instances have no page in memory. Embedding eviction metadata in every `WT_REF` wastes memory proportional to the total number of refs, not the number of in-memory pages.
- Example: with 100M leaf pages on disk and 1M pages in cache, embedding eviction fields (~72 bytes each) in all refs wastes ~6.7 GB on the 99M non-resident refs. With a separate allocation, those refs carry only an 8-byte NULL pointer.
- **Exception: `last_promotion_timestamp`** remains directly in `WT_REF` (not in the evict struct). This field is checked on **every page access** as the fast-path throttle test (a single atomic load, no lock required). Keeping it in `WT_REF` avoids a pointer dereference and a potential cache miss on the hottest code path in the eviction subsystem. The 8-byte cost per `WT_REF` is justified by the frequency of access.

### Struct layout

```c
struct __wt_ref_evict {
    WT_REF  *ref;                             /* Back-reference to owning WT_REF for page entries.
                                                 For sentinel entries (WT_REF_EVICT_SENTINEL set), this is NULL. */
    LTAILQ_ENTRY(__wt_ref_evict) all_lru_link;   /* List 1: All pages list.
                                                    Per-element lock is embedded in tqe_prev (bit 0).
                                                    See LRU-LISTS.md, "Per-element locking strategy". */
    LTAILQ_ENTRY(__wt_ref_evict) dirty_lru_link; /* List 2: Dirty/updated pages list.
                                                    Per-element lock is embedded in tqe_prev (bit 0).
                                                    See LRU-LISTS.md, "Per-element locking strategy". */
    uint8_t  evict_flags;                     /* Atomic flags field — see bit layout below */
    uint32_t inmem_children;                  /* Active in-memory children (internal pages only) */
    uint64_t cooldown_enter_ts;               /* When this ref first entered cooldown (0 = not in cooldown) */
    uint64_t last_retry_ts;                   /* When this ref was last attempted for processing */
    uint32_t cooldown_retry_count;            /* Number of processing attempts (stats) */
    uint8_t  cooldown_reason;                 /* Reason code for current cooldown (stats, future event-driven promotion) */
};

/*
 * evict_flags bit layout (single atomic uint8_t):
 *
 *   Bit 0:   WT_REF_EVICT_URGENT       — urgent flag (prevents duplicate insertion into urgent queue)
 *   Bits 1-2: all_pages_region          — region tracking for List 1 (NONE / LRU / COOLDOWN)
 *   Bits 3-4: dirty_pages_region        — region tracking for List 2 (NONE / LRU / COOLDOWN)
 *   Bit 5:   WT_REF_EVICT_SENTINEL     — this entry is a sentinel marker (cannot be removed as a page candidate)
 *   Bits 6-7: reserved
 *
 * The urgent flag is set/cleared using atomic OR / AND on the whole byte.
 * Region indicators are changed using CAS on the whole byte (read old value, compute
 * new value with the target bits changed, CAS).
 */
#define WT_REF_EVICT_URGENT        0x01  /* Bit 0 */
#define WT_REF_EVICT_ALL_SHIFT     1
#define WT_REF_EVICT_ALL_MASK      0x06  /* Bits 1-2 */
#define WT_REF_EVICT_DIRTY_SHIFT   3
#define WT_REF_EVICT_DIRTY_MASK    0x18  /* Bits 3-4 */
#define WT_REF_EVICT_SENTINEL      0x20  /* Bit 5 */

/* Region values — shared by both all_pages_region and dirty_pages_region */
#define WT_REF_REGION_NONE     0  /* Not in this list */
#define WT_REF_REGION_LRU      1  /* In LRU region of this list */
#define WT_REF_REGION_COOLDOWN 2  /* In a cooldown region of this list */
#define WT_REF_REGION_URGENT   3  /* In the urgent queue region (dirty_pages_region only) */

/* Accessor helpers (operate on a loaded evict_flags byte) */
#define WT_REF_ALL_REGION(f)    (((f) & WT_REF_EVICT_ALL_MASK) >> WT_REF_EVICT_ALL_SHIFT)
#define WT_REF_DIRTY_REGION(f)  (((f) & WT_REF_EVICT_DIRTY_MASK) >> WT_REF_EVICT_DIRTY_SHIFT)
```

### Sentinel allocation and identity

Sentinel elements for all region boundaries are permanently allocated `WT_REF_EVICT` structs:
- Each sentinel allocation must be followed by an alignment assertion: `WT_ASSERT(session, ((uintptr_t)sentinel & 1) == 0)`. Required because LTAILQ uses the lowest bit of `tqe_prev` as a per-element spinlock (see [Per-element locking strategy](LRU-LISTS.md#per-element-locking-strategy)).
- `WT_REF_EVICT_SENTINEL` is set in `evict_flags` at creation time and never cleared.
- Sentinel elements are list sentinels, not page candidates: they are never removed by worker page-pop logic and are never evicted.
- Sentinel identity is determined by pointer comparison against predefined sentinel pointers stored in the connection's eviction-related struct.
- Sentinel elements do not participate in WT_REF lifecycle transitions (`ref->page` changes) and are excluded from per-page deallocation handling.

The `WT_REF` struct gains two fields:
```c
struct __wt_ref {
    /* ... existing fields ... */
    wt_shared WT_REF_EVICT *evict;                  /* NULL when page not in memory */
    wt_shared uint64_t last_promotion_timestamp;     /* LRU promotion throttle */
};
```

### Allocation and initialization (page-in)

Allocate `ref->evict` when a page is read into memory — i.e., when `ref->page` transitions from `NULL` to a valid `WT_PAGE *`. This occurs during **page-in** (`__wt_page_in` and related paths) while the **WT_REF is in `WT_REF_LOCKED` state**.

Because the WT_REF is locked during page-in, no concurrent thread can observe `ref->evict` in a partially initialized state. No additional synchronization is needed for the allocation.

Initialization steps (performed before the ref is unlocked and made visible):
1. Allocate the `WT_REF_EVICT` struct via `__wt_calloc_one` (zero-fills the entire struct).
2. Assert memory alignment: `WT_ASSERT(session, ((uintptr_t)evict & 1) == 0)`. Required because LTAILQ uses the lowest bit of `tqe_prev` as a per-element spinlock (see [Per-element locking strategy](LRU-LISTS.md#per-element-locking-strategy)).
3. Set `ref->evict->ref = ref` (back-reference to the owning WT_REF).
4. All `LTAILQ_ENTRY` link fields are `NULL` after zero-fill (not in any list).
5. `evict_flags` is `0` (all regions `NONE`, urgent not set, sentinel not set).
6. `inmem_children` is `0`.
7. `cooldown_enter_ts` is `0` (not in cooldown).
8. `last_retry_ts` is `0`.
9. `cooldown_retry_count` is `0`.
10. `cooldown_reason` is `0`.
11. Set `ref->evict` to point to the newly allocated struct.
12. Do **not** insert into any list at this point. List insertion is a **separate step** after the ref is unlocked and the page is visible, following the lifecycle rules (leaf pages → insert into List 1 LRU region and set `all_pages_region = LRU`; internal pages → insert only when `inmem_children == 0`).
13. Do **not** modify `ref->last_promotion_timestamp` here — it lives in WT_REF and retains its value (0 from initial ref allocation, or 0 from a previous eviction reset).

### Deallocation and cleanup (page-out)

Deallocation relies on **exclusive WT_REF locking** combined with the **LTAILQ per-element locking protocol** to safely handle concurrent access. No reference counting is needed. This protocol applies to normal page entries only (`WT_REF_EVICT_SENTINEL` clear). Sentinel entries (`WT_REF_EVICT_SENTINEL` set) are permanent and excluded from page deallocation.

See [Removing page from memory or destroying WT_REF](LTAILQ-EXAMPLES.md#removing-page-from-memory-or-destroying-wt_ref) for the step-by-step locking algorithm.

**Deallocation protocol** (when evicting a page):

1. The **WT_REF must already be locked** for exclusive access (`WT_REF_LOCKED` state). Assert this.
2. Use the [Removing an entry from list](LTAILQ-EXAMPLES.md#removing-an-entry-from-list) algorithm to remove the page from **List 1** (if present).
3. Use the same algorithm to remove the page from **List 2** (if present).
4. Because the page can potentially be re-inserted from one list to another by a concurrent cross-list cooldown coordination, **double-check** both lists:
   - If the page is in List 1, re-remove it.
   - If the page is in List 2, re-remove it.
5. At this point: WT_REF is locked (no worker can be processing it), and the page is not in any list (no worker can pop it). It is safe to deallocate.
6. Set `ref->evict = NULL` (detach from the WT_REF). Reset `ref->last_promotion_timestamp = 0`.
7. Free the `WT_REF_EVICT` struct.

**Safety guarantee**: because the WT_REF is locked, no worker can be concurrently processing this page (workers must lock the WT_REF before processing). Because the page has been removed from all lists (with double-check), no worker can obtain a new reference to the `WT_REF_EVICT` struct. Therefore, the struct can be freed immediately.

**Why two passes (double-check) are sufficient**: The only mechanism that can re-insert a page into a list after it has been removed is **cross-list cooldown coordination** — a cooldown scanner on one list moving the page in the other list. A single cross-list operation is a one-shot action: scanner removes from cooldown in list A, then inserts into LRU of list B. It does not trigger a further insertion back into list A. Therefore, after the first pass removes the page from both lists, at most one pending cross-list operation can re-insert the page into one of the lists. The second pass catches that re-insertion. No further re-insertion is possible because: (1) the page's region indicators are set to `NONE` during removal, so no new cross-list coordination will target it, and (2) the WT_REF is locked, so no worker can pick up, process, or fail the page (which would be the only other path to list insertion). Two passes are therefore guaranteed to leave the page in no list.

**Debug assertions** (before freeing):
1. All `LTAILQ_ENTRY` links are `NULL`.
2. `evict_flags` is `0` (all regions `NONE`, urgent not set, sentinel not set).

Note: Unlike the previous circular-buffer design, there is no "Destroy-ref" exception. LTAILQ supports removal from any position in O(1). When a page needs to be evicted (e.g., file close), it is removed from all list regions directly using the per-element locking protocol.

### Access discipline

- **Never** dereference `ref->evict` without knowing the evict struct is allocated. Any code path that accesses eviction metadata must either:
  - Hold the WT_REF lock (guaranteeing the evict struct's lifetime), or
  - Have confirmed the page is in memory via its own invariants (e.g., the code is running inside an eviction worker that has locked the ref via CAS on `ref->state`).
- `ref->last_promotion_timestamp` can be read with a lock-free atomic load at any time without checking `ref->evict` — it lives directly in `WT_REF` and is valid for the entire WT_REF lifetime.
- **`evict_flags`** must be accessed using **atomic operations**:
  - The **urgent flag** (bit 0) is set using atomic OR (`flags |= WT_REF_EVICT_URGENT`) and cleared using atomic AND (`flags &= ~WT_REF_EVICT_URGENT`).
  - The **region indicators** (`all_pages_region` bits 1-2, `dirty_pages_region` bits 3-4) are changed using **CAS** on the whole byte: read old value, compute new value with the target bits changed, CAS. This ensures atomicity with respect to concurrent urgent flag changes and region changes on the other list.
  - The **sentinel flag** (`WT_REF_EVICT_SENTINEL`, bit 5) is set only for sentinel entries during sentinel initialization and remains set for the sentinel's lifetime.
  - Atomic access is required because concurrent threads may read and write different bit fields (e.g., a non-clean worker reads `all_pages_region` while a clean worker updates `dirty_pages_region`, or a caller sets the urgent flag while a worker clears a region indicator).
- **`ref` (back-reference)** must be accessed using **atomic operations** (load/store). It is read by workers after popping from lists, and written by the eviction path during deallocation.
