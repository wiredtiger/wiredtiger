# WT-17236 Review Fixes Design

## Goal

Resolve the validated PR review findings without increasing `WT_REF` or `WT_PAGE` size, preserve the dirty index as a best-effort optimization, and add deterministic coverage for its concurrency and adaptive scheduling contracts.

## Publication During Split

Reserve one value in the existing `WT_PAGE::dirty_index_slot` field to mean that publication is temporarily blocked. This changes only the interpretation of the existing field; it adds no structure members.

Before retiring a ref, split acquires this blocked state. If a producer already owns a slot but has not finished publishing it, split waits for that attempt to finish, removes the completed entry, and retries until it owns the blocked state. While blocked, new producers abandon insertion and leave eviction walking authoritative. Split then transitions the old ref to `WT_REF_SPLIT`. If the page survives under a replacement ref, split releases the blocked state to `WTI_DIRTY_BP_NONE`; otherwise page destruction needs no release.

The required invariant is that split cannot retire a ref while a producer can still publish that ref into the ring. Clearing a ring entry remains conditional on the expected ref so retirement cannot remove an entry belonging to a replacement ref that shares the page.

## Allocation And Configuration

Allocate a small dirty-index descriptor for every structurally eligible btree at open, independent of the current runtime enable flags. Store the chosen capacity in the descriptor but allocate and initialize the slot array only on the first qualifying insertion.

One producer atomically becomes the allocator. Other producers abandon their best-effort insertion while allocation is in progress. Allocation failure also falls back to walking and must not fail an already-linked application update. Slot storage remains allocated until btree destruction; runtime disable never frees it.

This gives runtime configuration predictable behavior:

- Enabling activates existing eligible handles without reopening.
- Disabling immediately prevents new insertions and drains, except an already-started insertion may finish.
- Re-enabling reuses existing slot storage.
- Both `WT_BTREE_DISAGGREGATED` stable btrees and `WT_BTREE_GARBAGE_COLLECT` ingest btrees obey `eviction_dirty_index_disagg`.

## Adaptive Scheduling

Separate drain outcome from ordinary queue-slot consumption. A drain reports whether it was not attempted, empty, unproductive, or productive. Adaptive state changes only after a real attempt, and urgent candidates count as productive.

Use a per-btree next-probe generation rather than a global modulo test so a parked tree cannot miss every probe. Preserve walker fallback when the drain repeatedly consumes the full per-tree budget.

Extract the policy transitions into small internal helpers so Catch2 can exercise thresholds and state transitions without background eviction timing.

## Statistics

Keep combined walker-and-drain page discovery accounting, but change descriptions from “eviction walk” to “eviction candidate discovery.” Move stable-lag skip accounting after ring and eviction-mode eligibility checks. Describe insertion races as ownership or state races rather than only back-pointer contention.

Add only telemetry needed to validate lazy allocation and adaptive transitions. Regenerate all derived statistics and configuration files from their `dist/` sources.

## Tests

Add deterministic Catch2 coverage for:

- A producer paused between page ownership and slot publication while split retires the ref.
- Duplicate suppression on repeated updates to one page.
- Runtime enable, disable, and re-enable using fresh pages.
- Lazy slot allocation and concurrent allocation fallback.
- Disaggregated stable and ingest gating.
- Adaptive parking, probing, resumption, and walker fallback.

Retain Python suite coverage for public configuration wiring and end-to-end insertion/draining. Replace fixed sleeps with statistic-backed waits and add a positive eviction control to negative drain assertions.

## Cleanup

Add defensive assertions for invalid back-pointers, move the existing page ownership check earlier on the hot path, and apply the validated declaration, comment, naming, and wrapping corrections. No unrelated refactoring is included.

## Verification

Run the new Catch2 target, `test_eviction08.py` with and without the disaggregated hook where available, generated-file checks, and `dist/s_fast`.
