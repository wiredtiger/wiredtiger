## LTAILQ locking example scenarios

Because all useful regions are bounded by sentinel elements, HEAD and TAIL are these sentinels and are not supposed to be moved or removed.

To avoid deadlocks, locking sequence is always head-to-tail.

Cooldown region scanning requires head-to-tail traversal: entries are appended at the tail (newest) and scanned from the head (oldest). The scanner must stop at the first non-eligible entry because all later entries have more recent timestamps and are also non-eligible. This early-stop property only works when scanning from head to tail. Head-to-tail traversal requires head-to-tail lock ordering (each step reveals the next pointer under the current lock). The reverse — tail-to-head traversal — would require reading forward pointers without locks, then locking backward, introducing retry loops at the system's highest-contention point (worker dequeue from the LRU head). Since cooldown scanning is the only operation that requires traversal, and it dictates head-to-tail, all other operations use the same lock ordering for consistency. See the [Crab scanning](#crab-scanning-cooldown-region-traversal) scenario below.

None of the scenarios below require both lists to be locked at the same time. Operations on different lists are performed sequentially, so cross-list locking order is not a concern.

---

### Inserting an entry at the TAIL

```
* Lock ENTRY
  * Read ENTRY->prev and ENTRY->next. If any of them is non-zero, the entry is already somewhere in the list. Unlock all, return.
  * Since the ENTRY is not in the list, it can't be accessed by anyone else.
  * -- Restart point --
  * Lock TAIL
    * Read TAIL->prev into local variable TAIL_PREV.
    * Try lock TAIL_PREV. On failure => unlock TAIL, yield, restart from restart point. (ENTRY remains locked.)
      * Note: TAIL_PREV cannot be freed while we hold TAIL, because removing TAIL_PREV requires locking its next neighbor (TAIL).
      * Check if TAIL_PREV->next == TAIL. On failure => unlock all except ENTRY, restart from restart point.
      * Note: TAIL->prev == TAIL_PREV is guaranteed since we held TAIL the entire time.
      * Insert ENTRY between TAIL_PREV and TAIL.
* Unlock all, done
```

---

### Inserting an entry at the HEAD

```
* Lock ENTRY
  * Read ENTRY->prev and ENTRY->next. If any of them is non-zero, the entry is already somewhere in the list. Unlock all, return.
  * Since the ENTRY is not in the list, it can't be accessed by anyone else.
  * Lock HEAD
    * Read HEAD->next into a local variable HEAD_NEXT
    * Lock HEAD_NEXT
      * Note: there's no need to check HEAD_NEXT->prev since we've read HEAD_NEXT under a lock and never unlocked it since.
      * Insert ENTRY in the list between HEAD and HEAD_NEXT
* Unlock all, done
```

---

### Removing an entry from list

```
* -- Restart point --
* Lock the ENTRY
  * Read ENTRY->next and ENTRY->prev pointers into local variables NEXT_COPY and PREV_COPY.
  * If both NEXT_COPY and PREV_COPY are NULL, the entry is not in any list. Unlock ENTRY, return.
  * Try lock PREV_COPY. On failure => unlock ENTRY, yield, restart from restart point.
    * Note: PREV_COPY cannot be freed while we hold ENTRY, because removing PREV_COPY requires locking its next neighbor (ENTRY).
    * Check that PREV_COPY->next == ENTRY. On failure => unlock all, restart from restart point.
    * Note: ENTRY->next == NEXT_COPY and ENTRY->prev == PREV_COPY are guaranteed since we held ENTRY the entire time.
    * Lock NEXT_COPY
      * Note: NEXT_COPY cannot be freed while we hold ENTRY, because removing NEXT_COPY requires locking its prev neighbor's next chain through ENTRY. Safe to use blocking lock (head-to-tail order: PREV_COPY -> ENTRY -> NEXT_COPY).
      * Check that NEXT_COPY->prev == ENTRY. On failure => unlock all, restart from restart point.
      * Zero out ENTRY->prev = ENTRY->next = NULL
      * Remove the ENTRY from list: PREV_COPY->next = NEXT_COPY, NEXT_COPY->prev = PREV_COPY.
* Unlock all, done
```

---

### Atomic reinsertion or promotion to MRU

"Element promotion" is the same as reinsertion of the element into the MRU part of the LRU list.

Promotion and reinsertion always happens from "earlier" parts of the list into "later" parts of the list or between different regions of the list. So, there is a guarantee of either no-overlap in locks or the destination is closer to the list tail (MRU).

Reinsertion algorithm:

```
Lock the entry:

* -- restart point 1 --
* Lock the ENTRY
  * Read ENTRY->next and ENTRY->prev pointers into local variables NEXT_COPY and PREV_COPY.
  * If both NEXT_COPY and PREV_COPY are NULL, the entry is not in any list. Unlock ENTRY, return.
  * Try lock PREV_COPY. On failure => unlock ENTRY, yield, restart from restart point 1.
    * Note: PREV_COPY cannot be freed while we hold ENTRY, because removing PREV_COPY requires locking its next neighbor (ENTRY).
    * Check that PREV_COPY->next == ENTRY. On failure => unlock all, restart from restart point 1.
    * Note: ENTRY->next == NEXT_COPY and ENTRY->prev == PREV_COPY are guaranteed since we held ENTRY the entire time.
    * Lock NEXT_COPY
      * Check that NEXT_COPY->prev == ENTRY. On failure => unlock all, restart from restart point 1.

      Lock the destination:

      * -- restart point 2 --
      * Lock DEST (skip if DEST is already locked, i.e. same as PREV_COPY, ENTRY, or NEXT_COPY)
        * Read DEST->prev into local variable DEST_PREV.
        * Try lock DEST_PREV (skip if already locked, i.e. same as PREV_COPY, ENTRY, or NEXT_COPY). On failure => unlock DEST (if newly acquired in this section), yield, restart from restart point 2.
          * Note: DEST_PREV cannot be freed while we hold DEST, because removing DEST_PREV requires locking its next neighbor (DEST).
          * Check if DEST_PREV->next == DEST. On failure => unlock all acquired in this section, restart from restart point 2.
          * Note: DEST->prev == DEST_PREV is guaranteed since we held DEST the entire time.
          * Relink the entry to the new position

* Unlock all, done
```

---

### Atomically getting a work item from HEAD and dequeueing it

```
* Lock HEAD
  * Copy HEAD->next into a local variable HEAD_NEXT
  * If HEAD_NEXT->sentinel is set, the list is empty. Unlock HEAD, return "no work item".
  * Lock HEAD_NEXT
    * Note: there's no need to check HEAD_NEXT->prev since we've read HEAD_NEXT under a lock and never unlocked it since.
    * Copy HEAD_NEXT->next into a local variable HEAD_NEXT_NEXT
      * Lock HEAD_NEXT_NEXT
        * Note: there's no need to check HEAD_NEXT_NEXT->prev since we've read HEAD_NEXT_NEXT under a lock and never unlocked it since.
        * Zero out HEAD_NEXT->prev = HEAD_NEXT->next = NULL
        * Remove HEAD_NEXT from list: HEAD->next = HEAD_NEXT_NEXT, HEAD_NEXT_NEXT->prev = HEAD.
* Unlock all, done
* Use HEAD_NEXT as the work item
```

---

### Crab scanning (cooldown region traversal)

Cooldown region scanning walks entries head-to-tail, checking each entry's `last_retry_ts` against the tier's retry timeout. For each entry, the scanner either removes it (to reinsert into LRU or urgent queue later) or advances past it. Three locks are held at each step: PREV, CURRENT, and NEXT — forming a "window" that slides forward through the region.

Initial lock acquisition (first entry):

```
* Lock REGION_HEAD (the sentinel at the head of the cooldown region)
  * Read REGION_HEAD->next into CURRENT
  * If CURRENT is a sentinel (WT_REF_EVICT_SENTINEL set) → region is empty. Unlock, done.
  * Lock CURRENT
    * Read CURRENT->next into NEXT
    * Lock NEXT
      → Now holding: REGION_HEAD, CURRENT, NEXT (three contiguous locks)
      → Proceed to per-entry decision below.
```

Per-entry decision (holding locks on PREV, CURRENT, NEXT):

```
* Check if CURRENT is a sentinel → end of region. Unlock all, done.
* Check if now - CURRENT->last_retry_ts >= retry_timeout
  * If NOT eligible (timeout not elapsed) → stop scanning. Unlock all, done.
    (All later entries are newer and also not eligible — early stop.)
  * If eligible → two options:

    Option A — Remove CURRENT from the region:
      * Zero out CURRENT->prev = CURRENT->next = NULL
      * Relink: PREV->next = NEXT, NEXT->prev = PREV
      * Unlock CURRENT (it is now detached from the list)
      * The caller will reinsert CURRENT into the LRU region or urgent queue
        later, after releasing all remaining locks.
      → Save CURRENT aside. Advance the window:
        * PREV stays (same element, still locked)
        * Read NEXT->next into NEW_NEXT
        * Lock NEW_NEXT
        → Now holding: PREV, NEXT (as new CURRENT), NEW_NEXT
        → Repeat per-entry decision with CURRENT=NEXT, NEXT=NEW_NEXT.

    Option B — Skip CURRENT (keep it in the region):
      * Unlock PREV
      * Read NEXT->next into NEW_NEXT
      * Lock NEW_NEXT
      → Now holding: CURRENT (as new PREV), NEXT (as new CURRENT), NEW_NEXT
      → Repeat per-entry decision with PREV=CURRENT, CURRENT=NEXT, NEXT=NEW_NEXT.
```

Notes:
- Lock ordering is always head-to-tail: PREV < CURRENT < NEXT in list order. Each new lock acquisition is on an element further toward the tail. No backward locking ever occurs.
- After removing an entry (Option A), it is set aside for later reinsertion. The actual reinsertion into LRU or urgent queue happens **after unlocking all scanning locks** to avoid holding locks across regions.
- After skipping an entry (Option B), PREV advances by one position. The previously locked PREV is released before the new NEXT is locked, so exactly three locks are held at all times (no lock accumulation).
- The early-stop guarantee means the scanner examines only the prefix of eligible entries. Since entries are appended at the tail, they are naturally ordered by `last_retry_ts` (oldest at head).
- Only one worker scans a given cooldown tier per list at a time (enforced by the atomic timer CAS). Other workers inserting into the same region operate at the **tail**, while the scanner operates at the **head**, so contention between scanning and insertion is minimal.

---

### Worker

* Choose the queue to process.

For the selected process queue:

* Use "Remove from HEAD" algorithm to acquire an element from urgent queue or LRU list.
* Try to exclusively lock the REF.
  * If failed then:
    * Increment retry count
    * Set last retry timestamp
    * If first retry timestamp is 0, also set it to current time.
    * Use "Insert at TAIL" to add the page to the relevant cooldown queue based on the first retry timestamp.
  * If succeeded, identify the work that needs to be done on the page and do in this order:
    * Reconcile the page (transform in memory) - if needed
    * Write the page to disk - if needed
    * Remove page from memory - if needed

---

### Removing page from memory or destroying WT_REF

Operations connected with deallocating the `__wt_ref_evict` struct require careful consideration.

Whenever the page is evicted from memory (page_out), the `__wt_ref_evict` struct is deallocated.
Also, `__wt_ref_evict` is deallocated before WT_REF is destroyed.

Deallocation of the `__wt_ref_evict` struct always happens with WT_REF locked for exclusive access. It's also crucial to ensure no other threads are concurrently accessing LTAILQ entries and `__wt_ref_evict` when deallocation is happening. To ensure that:

* WT_REF is already locked (add an ASSERT for it)
* Use "Removing an entry from list" to remove the page from "list 1".
* Use "Removing an entry from list" to remove the page from "list 2".
* Now, because the page can potentially be re-inserted from one list to another, do it again:
* Check if the page is in list 1 and if it is, re-remove it from there.
* Check if the page is in list 2 and if it is, re-remove it from there.
* Because the REF is locked, it's guaranteed that the page is not being worked on by any workers.
* Also, it's not in any lists anymore so it's safe to delete `__wt_ref_evict` from memory.

**Why two passes are sufficient (and a third is not needed):**

The only mechanism that can re-insert a page into a list after removal is **cross-list cooldown coordination** — a cooldown scanner operating on one list that also moves the page in the other list. This is a one-shot action: the scanner removes from cooldown in list A, then inserts into LRU (or urgent queue) of list B. It does not trigger a further insertion back into list A.

After the first pass (remove from list 1, remove from list 2), at most one pending cross-list operation can re-insert the page into one of the two lists. The second pass catches that re-insertion. No further re-insertion is possible because:
1. The page's region indicators are set to `NONE` during removal, so no new cross-list coordination will target it.
2. The WT_REF is locked, so no worker can pick up, process, or fail the page (which would be the only other path that leads to list insertion).

Therefore, two passes are guaranteed to leave the page in no list.
