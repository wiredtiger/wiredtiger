# WT-14196 Compact Performance Benchmark — Design

**Status:** Draft for review
**Author:** Sean Watt
**Date:** 2026-05-11
**Jira:** [WT-14196](https://jira.mongodb.org/browse/WT-14196)

## Goal

Build a permanent, parameterised performance benchmark that reproduces the
WT-14196 scenario — `session->compact()` running concurrently with random
updates and periodic checkpoints — and emits measurable stats that can
distinguish a baseline run from a run with a prototype fix. The benchmark
lives in the WiredTiger Evergreen perf pipeline so it tracks compact
performance over time and gates future regressions.

The pathology being measured: `__block_first_srch` in
`src/block/block_ext.c:94` linearly walks the offset extent list while
holding `live_lock`. During compact's first-fit allocation phase this walk
gets progressively longer, and because every write must take `live_lock`,
concurrent updates and checkpoints stall behind it.

## Non-goals

- A new compaction algorithm. This benchmark *evaluates* fixes; it does not
  fix anything itself.
- Microbenchmarks of `__block_first_srch` in isolation. Catch2 already
  covers that level. We want the customer-visible, system-level signal.
- Coverage of the `background_compact` server. The ticket reproducer and
  customer escalations are about foreground `session->compact()`.
  Background-compact is a follow-up variant.
- A general-purpose extent-allocator benchmark.

## Context: what already exists

| Area | Today |
|---|---|
| Reproducer | `test/csuite/compact/main.c` on `origin/wt-14196-compact-investigation` — exact ticket scenario but no timing, no throughput, no latency, no JSON stats, no parameter knobs |
| Existing stat | `block_first_srch_walk_time` already wired up in `src/block/block_ext.c:108` (`WT_STAT_CONN_SET`) — records the duration of the most recent linear walk |
| Existing compact stats | `btree_compact_pages_reviewed`, `pages_rewritten`, `pages_skipped`, `session_table_compact_passes`, `block_byte_write_compact` |
| wtperf compact support | `compact=true` triggers a *post-populate* one-shot compact — no concurrent compact thread |
| workgen compact support | None — `OpType` has no `OP_COMPACT` |
| Perf pipeline | `bench/perf_run_py/perf_run.py` supports `--wtperf` and `--workgen` test types; wraps execution, collects stats per `perf_stat_collection.py`, emits Evergreen + Atlas JSON |
| Existing latency infra in wtperf | `bench/wtperf/track.c` provides per-op TRACK structures with us/ms/sec-bucketed latency histograms, dumped to `latency.<op>` CSV files. Monitor thread writes `monitor` and `monitor.json` time series |

## Approach: extend wtperf

Three approaches were considered:

- **A. Extend wtperf with a concurrent compact thread (chosen).** Adds a
  small new surface to wtperf and reuses its mature latency / monitor /
  Atlas integration.
- **B. Standalone `bench/compact_concurrent/` binary + new `perf_run_py`
  test type.** Most flexible but duplicates latency tracking and adds a
  third test type to maintain.
- **C. Standalone binary that emits wtperf-format output.** Compromise that
  hides coupling on wtperf's output format — brittle.

A wins because the hardest pieces of any compact benchmark — latency
histograms, time-series monitor, throughput/latency SLA gates, Atlas
submission — are already done in wtperf. The new wtperf surface is small
and well-scoped: one new worker thread, one new post-populate pass, one new
stats dump.

## Architecture

```
       ┌─────────────────── wtperf binary (extended) ────────────────────┐
       │                                                                  │
       │  Phase 1: populate (existing)                                    │
       │    └─ icount records with value_sz_min..value_sz_max varied     │
       │       sizes; checkpoint at end                                   │
       │                                                                  │
       │  Phase 2: post-populate fragmentation  ◄── NEW                   │
       │    └─ either remove every Nth key (modulus) OR remove %         │
       │       of randomly-chosen keys; checkpoint at end                 │
       │                                                                  │
       │  Phase 3: workload (existing + NEW compact thread)               │
       │    ├─ N update worker threads (existing TRACK update)            │
       │    ├─ 1 checkpoint thread (existing checkpoint_threads=1)        │
       │    ├─ 1 monitor thread → monitor + monitor.json (existing)       │
       │    └─ 1 compact thread ◄── NEW                                   │
       │         └─ session->compact(uri, NULL) once; on return,         │
       │            flips wtperf->stop so workload winds down            │
       │                                                                  │
       │  Phase 4: stats dump (existing + NEW compact_stats_dump)         │
       │    └─ writes compact_summary.txt with the new metrics            │
       │                                                                  │
       └──────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
                  ┌─────────── perf_run_py (extended) ──────────┐
                  │ existing PerfStat patterns for throughput   │
                  │ and latency from monitor.json + latency.*   │
                  │                                              │
                  │ NEW compact_stats() PerfStat list           │
                  │ greps compact_summary.txt for new metrics    │
                  └──────────────────────────────────────────────┘
                                       │
                                       ▼
                       evergreen_out_compact-stress.json
                       atlas_out_compact-stress.json
                                       │
                                       ▼
                  Atlas perf tracking (existing pipeline)
```

## Files changed or added

| Path | Change |
|---|---|
| `bench/wtperf/wtperf_opt_inline.h` | Add new options (see below) |
| `bench/wtperf/wtperf.h` | Add `TRACK compact;`, `compactthreads` slot, `compact_done` flag, snapshot fields |
| `bench/wtperf/wtperf.c` | Add `compact_worker`, `stats_sampler_worker`, `execute_post_populate_remove`, `compact_stats_dump`; wire start/join into `execute_workload` |
| `bench/wtperf/runners/compact-stress.wtperf` | New canonical runner config |
| `bench/perf_run_py/perf_stat_collection.py` | Add `compact_stats()` PerfStat list; include in `all_stats()` |
| `test/evergreen.yml` | New `wtperf-test-compact-stress` task and a `-medium` variant under existing `wtperf-perf-test` tag |

## New wtperf options

Added to `wtperf_opt_inline.h`:

```c
DEF_OPT_AS_UINT32(compact_threads, 0,
  "number of foreground compact threads. Each runs session->compact() once "
  "after compact_start_after seconds into the workload phase.")
DEF_OPT_AS_STRING(compact_uri, "",
  "URI for compact threads to operate on. Empty means the first table.")
DEF_OPT_AS_UINT32(compact_start_after, 0,
  "seconds into the workload phase to wait before issuing compact, 0 fires immediately")
DEF_OPT_AS_BOOL(compact_ends_workload, 1,
  "when compact returns, signal worker threads to stop. If false, workload runs "
  "until run_time elapses even after compact finishes.")
DEF_OPT_AS_UINT32(post_populate_remove_modulus, 0,
  "after populate, remove every Nth key to create fragmentation. "
  "0 disables. Takes precedence over post_populate_remove_pct.")
DEF_OPT_AS_UINT32(post_populate_remove_pct, 0,
  "after populate, remove this percentage of randomly-chosen keys. "
  "0 disables. Ignored if post_populate_remove_modulus is set.")
```

All default to 0/disabled so existing wtperf runners are unaffected.

## Thread coordination

| Thread | Lifecycle |
|---|---|
| Worker (update) | Started in workload, runs until `wtperf->stop` is set (by compact or by `run_time` elapsing) |
| Checkpoint | Started in workload, runs every `checkpoint_interval`s, exits on `stop` |
| Compact | Started in workload, optional `compact_start_after` delay, fires compact once, sets `compact_done` and (when `compact_ends_workload=1`) `stop` |
| Stats sampler | Started alongside compact, samples `block_first_srch_walk_time` every 100 ms while `!compact_done`, tracks the peak |
| Monitor | Existing, runs every `sample_interval`s, exits on `stop` |

`compact_ends_workload=1` (the default) matches the WT-14196 reproducer:
updates run until compact returns, then everything winds down. Setting it
to 0 lets a future variant run "compact during a long sustained workload"
without forcing the workload to end immediately.

### `compact_worker` (sketch)

Modelled on `checkpoint_worker` at `wtperf.c:1679` and `:2475`:

```c
static WT_THREAD_RET
compact_worker(void *arg) {
    WTPERF_THREAD *thread = arg;
    WTPERF *wtperf = thread->wtperf;
    CONFIG_OPTS *opts = wtperf->opts;
    WT_SESSION *session;
    uint64_t start_us, elapsed_us;
    const char *uri;

    conn->open_session(conn, NULL, opts->sess_config, &session);
    uri = opts->compact_uri[0] ? opts->compact_uri : wtperf->uris[0];

    capture_pre_compact_stats(session, uri, &wtperf->compact_pre);

    if (opts->compact_start_after > 0)
        sleep_or_stop(wtperf, opts->compact_start_after);

    /* Snapshot worker TRACK aggregates so we can report
     * "ops/latency during compact". */
    snapshot_update_track(wtperf, &wtperf->update_pre_compact);

    start_us = __wt_clock_to_us(__wt_clock());
    if (!wtperf->stop)
        ret = session->compact(session, uri, NULL);
    elapsed_us = __wt_clock_to_us(__wt_clock()) - start_us;

    snapshot_update_track(wtperf, &wtperf->update_post_compact);
    capture_post_compact_stats(session, uri, &wtperf->compact_post);

    wtperf->compact.ops = 1;
    wtperf->compact.latency = elapsed_us;
    wtperf->compact_ret = ret;
    wtperf->compact_done = true;

    if (opts->compact_ends_workload)
        wtperf->stop = true;

    session->close(session, NULL);
    return WT_THREAD_RET_VALUE;
}
```

### `execute_post_populate_remove` (sketch)

Called from `main()` after `execute_populate()` returns successfully:

```c
static int
execute_post_populate_remove(WTPERF *wtperf) {
    CONFIG_OPTS *opts = wtperf->opts;
    if (opts->post_populate_remove_modulus == 0 &&
        opts->post_populate_remove_pct == 0)
        return 0;

    open session + cursor on each table in wtperf->uris;
    if (modulus) {
        for k in [0 .. icount):
            if (k % modulus == 0) remove key k+1;
    } else {
        for k in [0 .. icount):
            if (rand() % 100 < pct) remove key k+1;
    }
    checkpoint;
    record records_removed and elapsed_seconds in wtperf for compact_summary.txt;
}
```

Single-threaded by design; deterministic when modulus is set.

## Edge cases

- Compact returns non-zero (EBUSY, etc.): record return code in
  `compact_summary.txt`, still set `stop`, exit non-zero from wtperf.
- `run_time` elapses before compact returns: the run_time timer sets
  `wtperf->stop` first. compact's internal progress-check sees it via the
  existing event handler interrupt path. Summary records
  `Compact completed : 0`.
- User sets both `post_populate_remove_modulus` and
  `post_populate_remove_pct`: modulus wins, log a warning.
- `compact_uri` references a non-existent table: error out in
  `compact_worker` setup, set `stop` so workload doesn't run forever.

## Stats output

### `<test_home>/compact_summary.txt`

Plain key:value text, grepped by existing `PerfStat` machinery
(`bench/perf_run_py/perf_stat.py:60`). Sample:

```
Compact configuration uri : table:test
Compact wallclock seconds : 145.32
Compact completed : 1
Compact return code : 0
Compact pages reviewed : 53442
Compact pages rewritten : 12876
Compact pages skipped : 40566

File size before compact bytes : 5368709120
File size after compact bytes : 4123456789
File size reduction bytes : 1245252331
File size reduction pct : 23.20

Block reuse bytes before compact : 1789934592
Block reuse bytes after compact : 234567890
Block first srch walk time peak usecs : 23145

Post-populate remove records : 333333
Post-populate remove seconds : 8.21

Update ops during compact : 1245678
Update avg latency during compact us : 184
Update max latency during compact us : 51234
```

### How each value is sourced

| Line | Source |
|---|---|
| Compact wallclock seconds | `__wt_clock` snapshots around `session->compact()` in `compact_worker` |
| Pages reviewed / rewritten / skipped | `statistics:URI` cursor read in `compact_worker` post-compact. `session_table_compact_passes` is a session-level internal stat and is not exposed via the public stats cursor, so we omit it from this v1 |
| File size before / after, block_reuse_bytes before / after | `statistics:URI` cursor reads in `compact_worker` pre and post compact |
| Block first srch walk time peak | `stats_sampler_worker` samples `WT_STAT_CONN_BLOCK_FIRST_SRCH_WALK_TIME` every 100 ms while `!compact_done`, keeps the max |
| Post-populate remove records / seconds | `execute_post_populate_remove` accumulates count and elapsed `__wt_clock` delta |
| Update ops / avg / max during compact | TRACK update snapshot diffs (compact_worker captures TRACK before and after using existing `sum_update_latency`-style helpers) |

### Reusing the existing latency histogram

wtperf's existing `latency.update` CSV (us/ms/sec buckets in
`monitor_dir`) covers the *entire workload*. The canonical config sets
`compact_start_after=0` so all three threads start together (matches the
ticket repro), making "whole workload" equal to "compact window". A new
`PerfStatLatencyPercentile` class (~50 lines) reads `latency.update` and
computes p50/p95/p99/p99.9 from the cumulative-operations column. This is
used by Evergreen to gate p99 regressions the same way `min_throughput`
gates average throughput.

### `perf_run_py` extension

Add a new classmethod `compact_stats()` to `PerfStatCollection`, called
from `all_stats()`:

```python
@staticmethod
def compact_stats():
    return [
        PerfStat(short_label="compact_wallclock_sec",
                 stat_file='compact_summary.txt',
                 pattern=r'Compact wallclock seconds\s+:\s+[\d.]+',
                 input_offset=4, output_precision=2,
                 conversion_function=float,
                 output_label='Compact wallclock seconds'),
        PerfStat(short_label="compact_pages_rewritten",
                 stat_file='compact_summary.txt',
                 pattern=r'Compact pages rewritten\s+:\s+\d+',
                 input_offset=4,
                 output_label='Compact pages rewritten'),
        # Plus the rest of the compact_summary.txt entries:
        #   compact_pages_reviewed, compact_pages_skipped,
        #   file_size_reduction_bytes, block_reuse_bytes_after,
        #   block_first_srch_walk_peak_us,
        #   update_ops_during_compact,
        #   update_avg_latency_during_compact_us,
        #   update_max_latency_during_compact_us
        #
        # Plus four percentile entries read from latency.update by the new
        # PerfStatLatencyPercentile class (see below):
        #   update_p50_latency_us, update_p95_latency_us,
        #   update_p99_latency_us, update_p999_latency_us
    ]
```

The new `PerfStatLatencyPercentile` class (~50 lines, lives next to the
existing `PerfStatLatency` in `perf_stat.py`) parses the existing
`latency.update` CSV (`#usecs,operations,cumulative-operations,total-operations`)
and computes percentiles from the cumulative column. The four percentile
entries above are instances of this class. The `PerfStatLatency` class
that already reads `monitor.json` is unsuitable here because it only
returns per-window max latencies, not distribution percentiles across the
full run.

Selection in Evergreen is via the existing `-ops` flag on `perf_run.py`.

### Output files at end of run

```
<home>/
├── test.stat              ← wtperf summary (existing)
├── monitor                ← throughput time series text (existing)
├── monitor.json           ← throughput time series JSON (existing)
├── latency.update         ← us/ms/sec histogram CSV (existing, reused)
├── latency.{insert,read,modify}  (existing, mostly empty for this test)
├── compact_summary.txt    ← NEW
└── WiredTigerStat.*       ← conn statistics_log JSON (existing)
```

## Canonical runner config

`bench/wtperf/runners/compact-stress.wtperf`:

```ini
# WT-14196 compact-under-load benchmark.
# Reproduces the ticket scenario: populate a row-store with varied values,
# remove every 3rd key to fragment, then run a foreground compact in parallel
# with random updates and periodic checkpoints.

conn_config="cache_size=20G,statistics=(all),statistics_log=(json,on_close,wait=1)"
table_config="allocation_size=4KB,leaf_page_max=32KB,leaf_value_max=64MB,memory_page_max=10M,split_pct=90,key_format=Q,value_format=u"

# Populate: 1M records, varied 512B-4KB values, ~5GB on disk.
icount=1000000
populate_threads=1
value_sz_min=512
value_sz_max=4096
value_sz=2048
random_value=true

# Fragmentation: remove every 3rd key after populate.
post_populate_remove_modulus=3

# Workload phase: one update thread, one checkpoint thread, one compact thread.
threads=((count=1,updates=1))
run_time=1800
checkpoint_threads=1
checkpoint_interval=15
compact_threads=1
compact_start_after=0
compact_ends_workload=1

# Time-series monitor (existing) + per-op latency histogram (existing).
sample_interval=2
sample_rate=1
report_interval=10

# SLA gates — disabled until we have baseline runs to tune them.
min_throughput=0
max_latency=0
```

### Why these defaults

| Setting | Rationale |
|---|---|
| `statistics=(all)` | Required for `block_first_srch_walk_time`, `block_reuse_bytes`, `btree_compact_*`. `(fast)` omits block_ext detail |
| `cache_size=20G`, dataset ~5GB | Matches the ticket. Working set fits in cache so eviction isn't the bottleneck — isolates the block_first_srch pathology |
| Single update thread | Matches the ticket reproducer exactly. More threads amplify live_lock contention; the canonical config should mirror the reported repro |
| `value_format=u` | Matches the ticket: raw byte vectors, varied sizes |
| `compact_start_after=0` | All three threads start together (the ticket uses a 3-way barrier). Makes the whole-workload `latency.update` histogram equal the during-compact histogram |
| `compact_ends_workload=1` | Workload winds down when compact returns. `run_time=1800` is just the safety upper bound |
| `checkpoint_interval=15` | The ticket reproducer does ~3 checkpoints during compact with random 1–15 s pauses. 15 s gives roughly the same shape deterministically |
| `random_value=true` | Avoids compressible filler skewing block allocation |

## Evergreen task wiring

Follows the existing `wtperf-test-*` pattern at `test/evergreen.yml:5718+`:

```yaml
  - name: wtperf-test-compact-stress
    tags: ["wtperf-perf-test"]
    commands:
      - func: "fetch artifacts"
      - func: "run-perf-test"
        vars:
          perf-test-name: compact-stress.wtperf
          maxruns: 3
          wtarg: -ops ['"compact_wallclock_sec", "compact_pages_reviewed",
                        "compact_pages_rewritten", "compact_pages_skipped",
                        "file_size_reduction_bytes", "block_reuse_bytes_after",
                        "block_first_srch_walk_peak_us", "update_ops_during_compact",
                        "update_max_latency_during_compact_us",
                        "update_avg_latency_during_compact_us",
                        "update_p50_latency_us", "update_p95_latency_us",
                        "update_p99_latency_us", "update_p999_latency_us",
                        "min_max_update_throughput", "max_latency_read_update"']
      - func: "convert-to-atlas-evergreen-format"
        vars:
          input_file:  ./wiredtiger/cmake_build/bench/wtperf/test_stats/atlas_out_compact-stress.wtperf.json
          output_path: ./wiredtiger/cmake_build/bench/wtperf/test_stats/atlas_out_compact-stress.json
          test_name:   compact-stress
      - func: "upload atlas perf test results"
        vars: { test-name: compact-stress }
      - func: "upload test stats"
        vars: { test_path: bench/wtperf/test_stats/evergreen_out_compact-stress.wtperf }

  - name: wtperf-test-compact-stress-medium
    tags: ["wtperf-perf-test"]
    commands:
      - func: "fetch artifacts"
      - func: "run-perf-test"
        vars:
          perf-test-name: compact-stress.wtperf
          maxruns: 2
          wtarg: -args ['"-o icount=5000000", "-o run_time=3600",
                         "-o conn_config=cache_size=10G"']
                 -ops ['"compact_wallclock_sec", "compact_pages_reviewed",
                        "compact_pages_rewritten", "compact_pages_skipped",
                        "file_size_reduction_bytes", "block_reuse_bytes_after",
                        "block_first_srch_walk_peak_us", "update_ops_during_compact",
                        "update_max_latency_during_compact_us",
                        "update_avg_latency_during_compact_us",
                        "update_p50_latency_us", "update_p95_latency_us",
                        "update_p99_latency_us", "update_p999_latency_us",
                        "min_max_update_throughput", "max_latency_read_update"']
      # Same convert-to-atlas-evergreen-format / upload-atlas / upload-test-stats funcs as the
      # canonical variant above, with output names prefixed compact-stress-medium.
```

Both variants piggy-back on the existing `wtperf-perf-test` tag so no new
build-variant entries are needed.

### Task variants

| Variant | Overrides | Expected wallclock | Cadence |
|---|---|---|---|
| **wtperf-test-compact-stress** | (defaults) | ~3–6 min | every PR perf check |
| **wtperf-test-compact-stress-medium** | `-o icount=5000000 -o run_time=3600 -o cache_size=10G` | ~15–25 min | nightly |
| ~~**wtperf-test-compact-stress-long-soak**~~ | requires `compact_interval` option | ~60+ min | weekly, follow-up |

The long-soak variant is deferred because "compact periodically during
sustained workload" needs a new `compact_interval` option in wtperf —
explicitly out of scope for v1.

### Local-iteration parameter knobs

The same canonical config is the entry point for local prototype iteration
via `-o` overrides:

```bash
# Larger dataset, same fragmentation pattern.
./wtperf -O compact-stress.wtperf -o icount=10000000

# Higher contention, multiple update threads.
./wtperf -O compact-stress.wtperf -o 'threads=((count=8,updates=1))'

# Stress the allocator with a small allocation size.
./wtperf -O compact-stress.wtperf -o 'table_config=allocation_size=512B,leaf_page_max=512B,...'
```

This supports the "research while evaluating solutions" workflow: same
entry point, swap knobs to test hypotheses, the stats output stays
consistent so before/after runs are directly comparable.

## Validating the test itself

Before we trust the numbers, we need confidence that the benchmark is
measuring the right thing.

1. **Smoke test**: a tiny `bench/wtperf/runners/compact-stress-smoke.wtperf`
   (icount=10K, run_time=30) that completes in <30 s. A small bash
   assertion in the Evergreen task (run alongside the existing
   `test_conf_dump.py` step) confirms `compact_summary.txt` exists with
   all expected keys. Catches regressions in the wtperf changes themselves.
2. **Baseline run on develop**: before declaring v1 done, run the
   benchmark on unmodified `develop` and confirm the qualitative
   observations from the WT-14196 flame graphs:
   - `block_first_srch_walk_peak_us` is non-trivial (millions of
     microseconds, not zero)
   - `compact_pages_rewritten > 0` and `file_size_reduction_bytes > 0`
     (compact does real work)
   - Update throughput drops measurably compared to a control run with
     `compact_threads=0`
3. **Differential check**: cherry-pick the rough block-first-srch-v2
   prototype from `origin/wt-14196-compact-block-first-srch-v2-prototype`
   and confirm the benchmark distinguishes it from baseline. If it
   doesn't, the benchmark isn't measuring the right thing and we iterate
   before committing.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Extending wtperf affects other teams using it | All new options default to 0/disabled. Existing wtperf runners unchanged. Get review from wtperf maintainers |
| Run-to-run variance, especially compact wallclock | `maxruns: 3` with drop-min/drop-max averaging in `perf_run.py` (already standard). Document expected variance bands in the runner file as a comment |
| `block_first_srch_walk_time` is a "last walk" value — 100 ms sampling can miss spikes | Acceptable for v1 trend signal. Follow-up: add proper "max walk time" and "walks count" stats directly in WT |
| "During-compact" metrics depend on `compact_start_after=0` + `compact_ends_workload=1` | Design already supports snapshotting TRACK at compact start/end. Defer the change until a future variant actually needs steady-state-then-compact semantics |
| Deterministic `post_populate_remove_modulus=3` always produces identical fragmentation | Good for reproducibility. Expose `post_populate_remove_pct` for randomised variants when investigating |

## Out of scope (v1) / follow-ups

- `compact_interval` option for periodic compact during long workloads —
  enables the long-soak variant.
- `background_compact_enabled` switch to exercise the background-compact
  code path through the same benchmark.
- `compact_during_inserts=true` mode (inserts instead of updates) —
  different allocation pattern.
- Atlas dashboards / SLA gates (`min_throughput`, `max_latency_fatal`) —
  enable once 2–3 weeks of baseline data exists.
- Adding a proper `block_first_srch_walk_time_max` (max-tracking)
  connection stat in WT itself — improves the peak measurement.
- A separate task variant where `compact_threads=0` provides a "no
  compact" control baseline for direct A/B comparison.

## Open questions

None blocking — knobs have explicit defaults and risks have explicit
mitigations.
