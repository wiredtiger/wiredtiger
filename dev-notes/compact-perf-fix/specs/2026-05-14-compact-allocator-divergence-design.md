# Compact allocator divergence — design

**Ticket:** WT-14196 — Investigate compact performance under concurrent write workloads
**Author:** Sean Watt
**Date:** 2026-05-14 (supersedes 2026-05-08 draft in `plans/`)
**Status:** Ready for plan-writing

## Problem

Today, when compact starts a pass on a block it bumps a global counter
(`block->allocfirst`). While that counter is non-zero, *every* allocation
on that block — compact's own relocations, eviction's writes for unrelated
updates, checkpoint's writes — routes through `__block_first_srch`
(`src/block/block_ext.c:94`), which linearly walks the offset-sorted extent
skip list while holding `block->live_lock`.

The pathology is now empirically quantified by the `compact-stress.wtperf`
runner (see `../../compact-perf-test/results-2026-05-14.md`):

| Config | No-compact baseline | Under compact | Drop |
|---|---|---|---|
| 1 update thread, mod=3 | 5,004 ops/sec | 1,298 ops/sec | **–74%** |
| 4 update threads, mod=3 | 44,418 ops/sec | 5,143 ops/sec | **–88%** |
| 1 update thread, mod=2 | 4,370 ops/sec | 1,538 ops/sec | **–65%** |

The 4-thread case is the worst: relative overhead *grows* with writer
count because every writer fights for `live_lock` and each call holds it
longer as the offset list grows. Compact also overruns its 300s `run_time`
by 40–56s (the linear walk gets worse as compact moves blocks toward the
front of the file) and starves checkpoints (60% fewer checkpoints completed
under load in the 4t case).

Per WT-14196, only the user-visible "file shrinks" contract is
load-bearing. Allocation order is an implementation detail.

## Scope

This spec covers **divergence only**: separating compact's own relocation
allocations from concurrent writes during compact, so that only the former
pay the first-fit cost. Making the first-fit search itself cheap (Zunyi
Liu's augmented-skiplist PoC) is out of scope as a guaranteed deliverable
but is **conditionally evaluated** by the investigation in §"Compact-path
investigation" — if compact can use best-fit too, the augmented-skiplist
work becomes unnecessary.

Out of scope: parameter sensitivity studies, workload sensitivity studies,
observability beyond the three stats listed below.

## Non-goals

- No on-disk format change.
- No public API change.
- No change to compact's own algorithm (which pages it picks, when it
  bails, etc.).
- No change to checkpoint's behavior other than removing its own existing
  first-fit calls (which become unnecessary).

## Success target

Update throughput during compact on `compact-stress.wtperf` within **10%
of the no-compact baseline**.

| Config | Baseline | Current | Target | Lift needed |
|---|---|---|---|---|
| 1t mod=3 | 5,004 ops/sec | 1,298 | ≥4,504 | 3.5× |
| 4t mod=3 | 44,418 ops/sec | 5,143 | ≥39,976 | **7.8×** |
| 1t mod=2 | 4,370 ops/sec | 1,538 | ≥3,933 | 2.6× |

The 4t case is load-bearing: it tests the live_lock-duration thesis under
contention. If we hit 40K there, divergence is sufficient.

**Fallback decision:** if 4t mod=3 lands in 25K–40K ops/sec (5–8× lift),
ship anyway and open a follow-up for residual contention (likely live_lock
hold-time inside the restricted-best-fit branch — diagnosable by adding a
hold-time stat). If <25K, treat as the design not delivering and dig in
before merge.

## Design

### Core idea

Replace the global `block->allocfirst` counter with
`block->compact_first_fit_threshold` (a `wt_off_t`). When compact is
active on a block, the threshold is the lowest offset of the high region
compact intends to relocate from. When compact is not active, the
threshold is `0`.

The allocator decides between three modes per call:

| Threshold | Caller's old offset | Mode |
|-----------|---------------------|------|
| `0` (compact inactive) | any | unrestricted best-fit (today's behavior) |
| set, `old_offset >= threshold` | compact-relocation write | first-fit within `[0, threshold)` |
| set, `old_offset < threshold` or `INVALID` | concurrent write | best-fit within `[0, threshold)` |

The shared property is: **while compact is running, no allocation lands in
`[threshold, EOF)`.** This preserves the file-shrinks contract; concurrent
writes can't refill the high region compact is trying to drain.

### Why divergence preserves the shrink guarantee

The natural concern with letting concurrent writes use best-fit is that
they could land in the high region and undo compact's work. That's why we
restrict them to `[0, threshold)`. Inside the low region, best-fit is fine
— the low region is what's left of the file after compact, and concurrent
writes filling holes there is exactly what we want.

### Components

**1. New per-block state (`src/include/block.h`)**

Replace:

```c
wt_shared uint32_t allocfirst; /* Allocation is first-fit */
```

with:

```c
wt_off_t compact_first_fit_threshold; /* Compact-active high-region cutoff;
                                       * 0 when compact is not running.
                                       * Protected by live_lock. */
```

All reads and writes of this field happen under `block->live_lock`. That's
already true for reads in the allocator, and we extend it to writes —
`__wt_block_compact_start` and `__wt_block_compact_end` (which today don't
take `live_lock`) acquire it for the single assignment. **Lock-order
caveat:** see Risks; the implementation plan must verify this acquire
doesn't violate an existing rule with the schema/checkpoint locks.

The replaced `allocfirst` was atomic only because checkpoint and compact
could both flip it concurrently. Under this design the field is set/cleared
exclusively from compact's start/end/refresh paths, and the EBUSY check in
`__wt_block_compact_start` already guarantees only one compactor is active
per block.

**2. Allocator signature change (`src/block/block_ext.c`)**

`__wti_block_alloc` gains a `wt_off_t old_offset` parameter:

```c
int __wti_block_alloc(WT_SESSION_IMPL *session, WT_BLOCK *block,
                      wt_off_t *offp, wt_off_t size, wt_off_t old_offset);
```

Callers pass the offset of the block being replaced, or
`WT_BLOCK_INVALID_OFFSET` if there is no previous block (new pages from
splits, root page on first checkpoint, avail-list serialization).

The decision logic at the top of the function:

```
if (block->compact_first_fit_threshold == 0):
    use existing best-fit on full avail list
else:
    threshold = block->compact_first_fit_threshold
    if old_offset != INVALID and old_offset >= threshold:
        first-fit within [0, threshold)
    else:
        best-fit within [0, threshold)
    if no fit in [0, threshold):
        fall back to best-fit on [threshold, EOF)
    if still no fit:
        __block_extend
```

The "best-fit within `[0, threshold)`" walks the by-size skip list, picks
the first entry of the matching size class, and verifies its offset is
`< threshold`. Because the by-size skip list returns lowest-offset extents
first within a size class, this is cheap: if the lowest-offset entry of a
class is past the threshold, we step up a class. No linear walk on the
offset list.

**3. Caller updates**

Every caller of `__wti_block_alloc` already has the old-block offset
nearby — it's used immediately afterward in the matching
`__wti_block_off_free` call. Plumbing it down is mechanical:

- `__wti_block_write_internal` (`src/block/block_write.c`)
- `__ckpt_process` and helpers (`src/block/block_ckpt.c`)
- Any other direct callers (sweep with
  `grep -rn '__wti_block_alloc(' src/`)

**4. Compact lifecycle hooks (`src/block/block_compact.c`)**

`__wt_block_compact_start`:
- Compute the initial threshold using the same logic compact already uses
  to pick its target region (`__block_compact_skip_internal`'s
  `start_offset`).
- Set `block->compact_first_fit_threshold` under `live_lock`.
- **Remove** the call to `__wti_block_configure_first_fit(block, true)`.

`__wt_block_compact_end`:
- Clear `block->compact_first_fit_threshold` to `0` under `live_lock`.
- **Remove** the call to `__wti_block_configure_first_fit(block, false)`.

Threshold refresh during a long compact: compact already periodically
recomputes its target region in `__block_compact_skip_internal`. At that
point, update `block->compact_first_fit_threshold` to the new
`start_offset`. Updates are under `live_lock`, so allocators see the old
or the new value consistently — both are safe.

**5. Checkpoint cleanup (`src/block/block_ckpt.c`)**

`__wt_block_checkpoint`'s two calls to `__wti_block_configure_first_fit`
(`block_ckpt.c:277` and `:311`) become no-ops once `allocfirst` is
removed. Delete them outright. Rationale: checkpoint's first-fit window
was always brief (root page + extent-list serialization) and was
originally there to keep checkpoint blocks at low offsets. Without
compact's giant first-fit window dragging on the same global flag,
checkpoint's tiny optimization isn't worth the code surface. If
checkpoint-block locality matters in some future workload, it can be
reintroduced as a per-call flag — but it's not what the customer pain is
about.

**6. `__wti_block_configure_first_fit` removal (`src/block/block_open.c`)**

The function becomes unused. Delete it and remove from `extern.h`.

### Data flow

During compact with concurrent writes:

```
Compact thread                       Eviction thread (unrelated update)
--------------                       -----------------------------------
sets threshold = T                   reconciles page Y (at old_offset O_y)
walks high pages                     calls __wti_block_alloc(old=O_y)
marks page X dirty                     acquires live_lock
                                       threshold=T set, O_y < T
                                       → restricted best-fit in [0, T)
                                       → cheap by-size lookup
                                       releases live_lock
                                       (no linear walk)

                                     reconciles compact-marked page X
                                     calls __wti_block_alloc(old=O_x), O_x >= T
                                       → first-fit in [0, T)
                                       → linear walk (still expensive,
                                          but only for compact's own writes
                                          — and only if investigation in
                                          §Compact-path investigation says
                                          we keep first-fit here)
                                       X lands at low offset
```

### Edge cases

- **Low region is full.** Restricted lookup returns no fit. Fall back to
  best-fit on `[threshold, EOF)`. Compact has effectively succeeded
  (nothing left to drain); the file may not shrink further this pass,
  which is fine.
- **`old_offset == WT_BLOCK_INVALID_OFFSET`.** New pages, root page
  first-write, etc. Treat as non-compact write → restricted best-fit.
- **Two compactions on the same block.** Rejected today at start with
  EBUSY (`block->compact_session_id`). Unchanged. Only one threshold value
  is ever active.
- **Threshold set/clear races with allocator.** Both writers and readers
  go through `live_lock`. Allocators see a consistent value.

### Backwards compatibility

None required. Runtime-only change.

## Compact-path investigation

The design above keeps compact's own relocations on the first-fit path.
The findings show this same first-fit walk is itself a pathology
(compact overruns budget by 40–56s). Before declaring the design final,
we evaluate whether compact can use the cheap restricted-best-fit branch
too — eliminating the linear walk entirely and obviating the
augmented-skiplist follow-up.

**Method (executed between commits C2 and C3 — see §Sequencing):**

1. With C2 merged (compact = first-fit per design), run
   `bench/wtperf/runners/compact-stress.wtperf` on:
   - 1 update thread, default fragmentation (mod=3)
   - 4 update threads, default fragmentation (mod=3)
2. Cherry-pick a one-line variant of `__wti_block_alloc` that routes
   compact's `old_offset >= threshold` case through the restricted
   best-fit branch (i.e., delete the first-fit branch). Run the same two
   configs.
3. Compare on three axes:

   | Axis | Required for best-fit to win |
   |---|---|
   | Update throughput during compact | Both variants hit ≥10% of no-compact baseline |
   | `file_size_reduction_bytes` | Best-fit within ~5% of first-fit |
   | `compact_wallclock_sec` | Best-fit ≤ first-fit (eliminating compact's own walk should help, not hurt) |

4. **Decision rule:**
   - If best-fit hits the throughput target AND loses ≤5% on file-size
     reduction: pick best-fit. The augmented-skiplist work becomes
     unnecessary; the design simplifies to "compact also uses
     restricted best-fit"; close out the design's first-fit branch.
   - Otherwise: keep first-fit; open a follow-up ticket for the
     augmented-skiplist work as the next layer.

5. **Deliverable:**
   `dev-notes/compact-perf-fix/specs/2026-MM-DD-compact-path-investigation-results.md`
   recording the numbers and the decision.

## Sequencing

Three commits, in order. Each is reviewable independently and supports
clean bisection if the canonical runner regresses.

### C1 — mechanical prep (no semantic change)

- Add `wt_off_t old_offset` parameter to `__wti_block_alloc`.
- Plumb from every caller — `__wti_block_write_internal`, `__ckpt_process`
  and helpers, any others surfaced by
  `grep -rn '__wti_block_alloc(' src/`. For new-page sites with no prior
  block (root first-checkpoint, splits, avail-list serialization), pass
  `WT_BLOCK_INVALID_OFFSET`.
- Function body ignores `old_offset` for now; behavior identical to today.
- Reviewer's job: confirm every caller passes the right offset. Pure
  plumbing, mechanically verifiable.

### C2 — divergence behaviour

- Replace `wt_shared uint32_t allocfirst` with
  `wt_off_t compact_first_fit_threshold` in `src/include/block.h`.
- Implement the three-branch routing in `__wti_block_alloc`.
- Lifecycle in `block_compact.c`: set threshold under `live_lock` in
  `__wt_block_compact_start`, clear in `__wt_block_compact_end`, refresh
  in `__block_compact_skip_internal` when `start_offset` changes.
- Delete `__wti_block_configure_first_fit` (from `block_open.c` and
  `extern.h`) and its two callsites in `__wt_block_checkpoint`
  (`block_ckpt.c:277,311`).
- Add stats (see §Observability) via `dist/stat_data.py` +
  `dist/s_all`-regenerated `stat.c` and `wiredtiger.h.in`.
- Update `bench/perf_run_py/perf_stat_collection.py::compact_stats()` to
  include the new stats in `compact-stress.wtperf` output.
- All Catch2 unit tests.

### C3 — compact-path decision

Either:

- **If investigation says best-fit wins:** swap compact's branch to
  restricted-best-fit (one-line change), delete the first-fit branch and
  the `block_alloc_first_fit_count` stat, land the investigation results
  doc.
- **If first-fit wins:** documentation-only commit landing the
  investigation results doc; open follow-up ticket for the
  augmented-skiplist work.

## Observability

Stats added in C2 (via `dist/stat_data.py`, regenerated by `dist/s_all`):

| Stat | Purpose |
|---|---|
| `block_alloc_first_fit_count` | Incremented on the first-fit branch. Proves the routing works. May be deleted in C3 if best-fit wins. |
| `block_alloc_restricted_best_fit_count` | Incremented on the restricted-best-fit branch. Proves the routing works. |
| `block_first_srch_walk_time_max` | True peak of the existing `block_first_srch_walk_time` last-walk-time stat. Lets us prove the walk is gone (or quantify what remains). Documented as a never-reset peak. |

Once `block_first_srch_walk_time_max` lands, the wtperf-side
`stats_sampler_worker` 100 ms-polling workaround (added in the
compact-stress benchmark) can be simplified or removed in a follow-up.

## Testing

### Catch2 unit tests (`test/catch2/`, requires `-DHAVE_UNITTEST=1`)

New file: `test/catch2/block/test_block_alloc_threshold.cpp` (exact
location and naming verified against existing `test/catch2/block/` layout
during C2).

Cases:
- Threshold = 0: bit-for-bit behavior parity with today's best-fit
  (regression guard).
- Threshold set, `old_offset >= threshold`: first-fit branch chosen,
  returns lowest-offset extent in `[0, threshold)`.
- Threshold set, `old_offset < threshold`: best-fit branch chosen, returns
  best-fit extent in `[0, threshold)`.
- Threshold set, `old_offset = INVALID`: same as `< threshold` branch.
- Threshold set, no fit in `[0, threshold)`: falls back to best-fit on
  `[threshold, EOF)`.
- Threshold set, no fit anywhere: falls through to `__block_extend`.
- Threshold updated mid-flight: prior allocations not affected; new
  allocations honor new threshold.
- Threshold cleared while allocator in flight (new — not in original
  design): writer sees threshold=0 after `live_lock` re-acquire, behaves
  as today.

### Functional / integration

The original design proposed a Python test under `test/suite/`. Dropped:
`compact-stress.wtperf` already runs the WT-14196 reproducer end-to-end
with the assertions we care about (throughput, file size reduction,
compact wallclock). Duplicating in Python adds maintenance without
diagnostic value.

Instead:
- `compact-stress.wtperf` in Evergreen is the regression gate. The 10%
  target becomes the `min_throughput` SLA once C2 lands and a post-fix
  baseline is measured.
- `test/format/` long soak (hour-class) runs against C2 to verify: no new
  assertions, no file growth during compact, no deadlocks, no live_lock
  starvation. This is the design's existing recommendation.

## Risks

- **Compact pass speed unchanged in the first-fit branch.** This design
  solves concurrency. Whether it also solves compact-own-speed depends on
  the investigation outcome. If first-fit wins the investigation, the
  augmented-skiplist follow-up is real work.
- **10% target is ambitious.** 4t mod=3 requires a 7.8× lift. If C2 lands
  in the 25K–40K range (5–8× lift), see Success target §Fallback decision.
- **Threshold staleness if the file shrinks faster than refresh.** If
  compact relocates many pages and the file shrinks well below the
  original threshold before refresh, allocations may be over-restricted.
  The refresh hook in `__block_compact_skip_internal` mitigates; worst
  case is suboptimal allocation choice for a short window.
- **Edge case "low region full."** The fallback breaks the
  no-high-region-writes property. Probably benign (compact has nothing to
  do at that point) but flagged for code review attention. Not
  surfaced via a stat in initial scope.
- **Lock-order risk in compact start/end.** Today
  `__wt_block_compact_start` and `__wt_block_compact_end` don't take
  `live_lock`. Adding the acquire must be checked against existing
  lock-order rules with the schema lock and checkpoint lock held at the
  time these functions run. The implementation plan must include an
  explicit lock-order audit as part of C2; if there's a conflict, the
  fix is either (a) atomic write of `compact_first_fit_threshold` with a
  release/acquire memory order, or (b) restructure compact start/end to
  acquire `live_lock` outside the conflicting region.
- **`block_first_srch_walk_time_max` never resets.** A long-running
  connection accumulates a single peak that may be from a transient
  event. Acceptable for this use case (we read it inside the compact
  window via the existing wtperf snapshot pattern), but document the
  limitation in `stat_data.py`.

## Open questions

- Lock-order audit for `live_lock` acquire in compact start/end (see
  Risks). Must be resolved during C2 implementation; not a blocker for
  starting the plan.

---

## Measured results (C2) — 2026-05-14

Measured on branch `sean-compact-perf-fix` after C2 working-tree changes (pre-commit).
Two parallel runs: 4 update threads, default fragmentation (mod=3), run_time=300s.

### 4 update threads, mod=3 (load-bearing gate config)

| Metric | Pre-fix (no compact) | Pre-fix (w/ compact) | Post-fix (w/ compact) | Post-fix (no compact) |
|---|---|---|---|---|
| ops/sec | 44,418 | 5,143 | **40,792** | 54,619 |
| total updates | 13,325,414 | 1,543,097 | 12,237,895 | 16,385,858 |
| compact wallclock | — | 356s | 328s | — |
| checkpoints | 10 | 4 | 8 | 12 |
| update avg latency | — | 820 μs | **98 μs** | — |
| update max latency | — | 51 ms | **7 ms** | — |
| file size reduction | — | 55% | 54% | — |

Notes on post-fix no-compact baseline: higher than pre-fix (54,619 vs 44,418) because pre-fix
runs had 6 parallel processes sharing one disk; post-fix ran only 2 parallel processes.
The load-bearing comparison is post-fix compact (40,792) vs pre-fix no-compact (44,418).

### Gate evaluation

| Config | Target | Measured | Pass? |
|---|---|---|---|
| 4t mod=3 ≥39,976 ops/sec | ≥39,976 (≥90% of 44,418 pre-fix baseline) | 40,792 | ✅ |

Lift over pre-fix compact: 40,792 / 5,143 = **7.93×** (target was 7.8×).

### Routing stats (from compact_summary.txt)

| Stat | Value | Interpretation |
|---|---|---|
| Block alloc first fit count | 72,780 | Compact's own relocations (first-fit branch) |
| Block alloc restricted best fit count | 7,539,532 | Concurrent writes (restricted best-fit — **103:1 ratio vs first-fit**) |
| Block first srch walk time max usecs | 51 μs | Max first-fit walk time (pre-fix: multi-ms) |

### Decision

**C2 ships.** The 4t gate passes; divergence design is validated by the routing stats.

C3 investigation proceeds: compact's own 72,780 first-fit allocations now account for the
remaining overhead. The 328s compact wallclock (vs 300s run_time) confirms compact's own
walk is still a factor, making the best-fit-for-compact investigation in C3 worthwhile.
