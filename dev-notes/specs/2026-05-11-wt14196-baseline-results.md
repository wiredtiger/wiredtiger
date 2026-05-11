# WT-14196 Baseline Results (smoke)

These results gate v1 of the compact-stress benchmark — they confirm the
benchmark machinery is intact and update throughput is measurable in both
control (`compact_threads=0`) and treatment (`compact_threads=1`) configurations.

The **canonical** 30-minute `compact-stress.wtperf` runner is where the real
pathology is expected to show. The **smoke** runner used here is intentionally
small (~20k records, 60s) and is only validated for structural correctness.

## Setup

- Branch: `sean-compact-perf` @ 4dacbee32b855d40993fe4c7ecb5a71e182237fd
- Build: Release (cmake default, HAVE_DIAGNOSTIC=0, ENABLE_PYTHON=0)
- Runner: `bench/wtperf/runners/compact-stress-smoke.wtperf`

## Control (`compact_threads=0`)

| Metric | Value |
|---|---|
| Avg update throughput (ops/sec) | 302,888 |
| Run wallclock seconds | 60 |

Sustained throughput across 5-second sampling intervals ranged from ~284K to ~314K ops/sec, demonstrating stable baseline performance with no compact interference.

## Treatment (`compact_threads=1`)

| Metric | Value |
|---|---|
| Compact wallclock seconds | 0.37 |
| Compact pages reviewed | 548 |
| Compact pages rewritten | 198 |
| Compact pages skipped | 350 |
| File size before compact bytes | 75,767,808 |
| File size after compact bytes | 94,126,080 |
| File size reduction bytes | -18,358,272 (file grew — expected on smoke runs) |
| Block reuse bytes before compact | 44,183,552 |
| Block reuse bytes after compact | 31,666,176 |
| Block first srch walk time peak usecs | 0 |
| Update ops during compact | 93,512 |
| Update avg latency during compact us | 3 |
| Update max latency during compact us | 2,675 |
| Avg update throughput (ops/sec) | 1,708 (over full 60s; see note below) |

**Note on 60-second average:** `compact_ends_workload=true` halts the update
workload as soon as compact finishes (at t=0.37s). The 60-second average of
1,708 ops/sec is dominated by the ~59.6s of zero-update idle time that follows.
The **instantaneous rate during the compact window** is ~252,000 ops/sec
(93,512 ops ÷ 0.37s), still well below the control baseline of ~303K ops/sec —
a ~17% throughput reduction visible even on this tiny smoke dataset.

## Conclusion

- [x] Benchmark machinery runs end-to-end on both control and treatment.
- [x] `compact_summary.txt` produced under treatment with all expected keys.
- [x] Smoke runner does NOT reliably reproduce the WT-14196 pathology — the canonical 30-min `compact-stress.wtperf` is required for that. This is expected.

### Notes on smoke-run limitations

- The file **grew** after compact (-18 MB reduction = negative). On this tiny 20k-record dataset the update workload writes more data than compact reclaims — this is expected.
- `Block first srch walk time peak usecs` = 0 because compact finished in 0.37s; there was no opportunity to accumulate a measurable block-search walk time at this scale.
- The compact thread finished before the first 5-second reporting interval, so the test.stat output shows 0 updates in every subsequent interval — the "pathology" manifests as compact ending the workload rather than degrading it mid-run.

## Differential check (prototype branch)

**Not run as part of this baseline.** The differential check against
`origin/wt-14196-compact-block-first-srch-v2-prototype` requires cherry-picking
the wtperf changes into a separate worktree and running the canonical (not
smoke) configuration. Captured here as a follow-up:

| Metric | Baseline (smoke) | Prototype (smoke) | Delta |
|---|---|---|---|
| Compact wallclock seconds | 0.37 | TBD | TBD |
| Block first srch walk time peak usecs | 0 | TBD | TBD |
| Update ops during compact | 93,512 | TBD | TBD |

Run the canonical runner against both branches once initial baseline is published.
