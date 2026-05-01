# Disagg CI Testing — Format Stress Tests

> Category: test/format/t run with CONFIG.disagg in various modes

---

## Overview

The format stress test (`test/format/t`) is a randomized read/write/transaction stress tester. For disagg, it is run with `test/format/CONFIG.disagg` as the base configuration, plus mode-specific overrides. Format tests are the primary **stress-level** disagg tests — they run for minutes to hours and exercise random workloads.

There are two groups:
1. **evergreen.yml** — normal tests (passing expected), running on the main CI project
2. **evergreen_disagg.yml** — "Failure Expected" tests in the separate `wiredtiger-disagg` Evergreen project, testing modes that are still being developed (switch, multi)

---

## CONFIG.disagg Base Configuration

`test/format/CONFIG.disagg`:

```
disagg.enabled=1            # Enable disagg storage
disagg.page_log=palite      # Use PALite page log for tests
disagg.layered=1            # Use layered table URI
runs.source=layered         # Tables are layered type
runs.tables=3               # 3 row-store tables
runs.timer=...              # (set per task)
transaction.timestamps=1    # Timestamps enabled
checkpoint.precise=1        # Precise checkpoints
# Disabled:
backup.incremental=0        # No backup
lsm=0                       # No LSM
compact=0                   # No compaction
alter=0                     # No alter
salvage=0                   # No salvage
tiered=0                    # No tiered storage
# Not yet supported:
reverse_collator=0           # No reverse collator
modify_pct=0                 # No modify (deferred)
```

---

## evergreen.yml Format Stress Tasks (Passing Expected)

### Pull-Request Tasks

These run on every PR build:

| Task Name | Mode | Config / Args | Duration | Build Variants |
|---|---|---|---|---|
| `format-stress-test-disagg-leader-pull-request-1` | `leader` | `runs.rows=10000 runs.ops=50000` | 10 min | PR variants |
| `format-stress-test-disagg-leader-pull-request-2` | `leader` | `runs.rows=10000 runs.ops=50000` | 10 min | PR variants |
| `format-stress-test-disagg-follower-pull-request` | `follower` | `runs.rows=10000 runs.ops=50000` | 10 min | PR variants |

**What runs:** Two steps:
1. `format test script` — runs `format_disagg_multi.sh` to invoke format with a disagg-aware script wrapper
2. `format test disagg` function — runs `./t -c CONFIG.disagg <args>` directly, followed by `-R` (reopen/recovery) run

**PR variants that run these tasks:** `ubuntu2004-arm64`, `amazon2023-arm64`, `ubuntu2004-ubsan`, `amazon2023-arm64-ubsan`

### TSAN Tasks (Pull-Request)

| Task Name | Mode | Config / Args | Iterations | Notes |
|---|---|---|---|---|
| `format-stress-test-disagg-leader-tsan` | `leader` | `runs.rows=1000:3000 runs.tables=1:3 runs.ops=3000` | 3 | Limited ops — avoids internal TSAN issue (FIXME-WT-16313) |
| (same task, second run) | `leader` | `runs.rows=100000:300000 runs.tables=1:3 runs.ops=300000` | 3 | `detect_deadlocks=0` to work around TSAN deadlock detector |

**Build variants:** `ubuntu2004-tsan`, `amazon2023-arm64-tsan`

### Stress Test Tasks (tagged `stress-test-disagg`, not on every PR)

These run as part of the stress test pool — not necessarily on every PR, but regularly:

#### Leader mode — basic

Template: `format-stress-test-disagg-leader` (used for -1 and -2):
- `format test script`: `-t 180 -c CONFIG.disagg -- disagg.mode=leader runs.timer=5:10`, `num_jobs=1`
- `format test disagg`: `disagg.mode=leader runs.timer=5:10`, 5 iterations
- **Note:** `num_jobs=1` — FIXME-WT-16134, PALite can't run multiple jobs

| Instances | Tag |
|---|---|
| `format-stress-test-disagg-leader-1` | `stress-test-disagg` |
| `format-stress-test-disagg-leader-2` | `stress-test-disagg` |

#### Leader mode — data validation (mirrored table check)

Template: `format-stress-test-disagg-leader-data-validation`:
- Adds `ops.verify=1 runs.mirror=1 table1.runs.source=table table1.disagg.enabled=0`
- Mirror table (`table1`) is a non-layered table for cross-validation

| Instances | Tag |
|---|---|
| `format-stress-test-disagg-leader-data-validation-1` | `stress-test-disagg` |
| `format-stress-test-disagg-leader-data-validation-2` | `stress-test-disagg` |

#### Follower mode

Template: `format-stress-test-disagg-follower`:
- `format test disagg`: `disagg.mode=follower runs.ops=500000:2000000`, 5 iterations
- No `format test script` step (follower mode doesn't use the multi-instance wrapper)

| Instances | Tag |
|---|---|
| `format-stress-test-disagg-follower-1` | `stress-test-disagg` |
| `format-stress-test-disagg-follower-2` | `stress-test-disagg` |

### Long-Running Task

| Task Name | Mode | Args | Iterations | Variants |
|---|---|---|---|---|
| `format-stress-disagg-leader-long-running` | `leader` | `runs.timer=30:45 cache=10000 cache.eviction_dirty_trigger=50 cache.eviction_updates_trigger=50` | 1 | nonstandalone variants |

This runs for 30–45 minutes. Cache settings are explicitly enlarged — FIXME-WT-16228 notes this will be re-evaluated after PALI replaces PALite.

**Build variants:** `ubuntu2004-arm64-nonstandalone`, `ubuntu2004-arm64-release-nonstandalone`, `amazon2023-arm64-nonstandalone`, `amazon2023-arm64-release-nonstandalone`

---

## Build Variant Assignment (evergreen.yml)

| Build Variant | PR Tasks | Stress Tasks | Long-Running |
|---|---|---|---|
| `ubuntu2004-arm64` | leader-pr-1/2, follower-pr | — | — |
| `amazon2023-arm64` | leader-pr-1/2, follower-pr | — | — |
| `ubuntu2004-ubsan` | leader-pr-1/2, follower-pr | — | — |
| `amazon2023-arm64-ubsan` | leader-pr-1/2, follower-pr | — | — |
| `ubuntu2004-tsan` | leader-tsan | — | — |
| `amazon2023-arm64-tsan` | leader-tsan | — | — |
| `ubuntu2004-stress-tests` | — | `.stress-test-disagg` | — |
| `ubuntu2004-stress-tests-arm64` | — | `.stress-test-disagg` | — |
| `ubuntu2004-release-stress-tests` | — | `.stress-test-disagg` | — |
| `ubuntu2004-release-stress-tests-arm64` | — | `.stress-test-disagg` | — |
| `ubuntu2004-stress-nonstandalone` | — | `.stress-test-disagg` | — |
| `amazon2023-stress-tests-arm64` | — | `.stress-test-disagg` | — |
| `amazon2023-stress-nonstandalone` | — | `.stress-test-disagg` | — |
| `ubuntu2004-asan` | — | `.stress-test-disagg` | — |
| `amazon2023-arm64-asan` | — | `.stress-test-disagg` | — |
| `ubuntu2004-release-nonstandalone` | — | `.stress-test-disagg` | — |
| `ubuntu2004-arm64-nonstandalone` | — | `.stress-test-disagg` | yes |
| `ubuntu2004-arm64-release-nonstandalone` | — | `.stress-test-disagg` | yes |
| `amazon2023-arm64-nonstandalone` | — | `.stress-test-disagg` | yes |
| `amazon2023-arm64-release-nonstandalone` | — | `.stress-test-disagg` | yes |

---

## evergreen_disagg.yml Format Stress Tasks (Failure Expected)

These run in the separate **`wiredtiger-disagg`** Evergreen project on two build variants:
- `amazon2023-disagg-stress` — runs all `.stress-test-disagg-fail` tasks
- `amazon2023-disagg-asan-stress` — runs `.stress-test-disagg-san-fail` tasks (a subset)

### Switch Mode (disagg.mode=switch)

Exercises leader-to-follower-to-leader role switching during a running workload:

| Task Name | Args | Iterations | Tag |
|---|---|---|---|
| `format-stress-test-disagg-switch-1` | `disagg.mode=switch runs.timer=10:15` | 5 | `stress-test-disagg-fail`, `stress-test-disagg-san-fail` |
| `format-stress-test-disagg-switch-2` | `disagg.mode=switch runs.timer=10:15` | 5 | `stress-test-disagg-fail` |
| `format-stress-test-disagg-switch-3` | `disagg.mode=switch runs.timer=10:15` | 5 | `stress-test-disagg-fail` |
| `format-stress-test-disagg-switch-data-validation-1` | `disagg.mode=switch ops.verify=1 runs.mirror=1 table1.runs.source=table table1.disagg.enabled=0 runs.timer=5:10` | 5 | `stress-test-disagg-fail` |
| `format-stress-test-disagg-switch-data-validation-2` | same as above | 5 | `stress-test-disagg-fail` |
| `format-stress-test-disagg-switch-data-validation-3` | same as above | 5 | `stress-test-disagg-fail` |

**Note:** FIXME-WT-16394 — switch mode is not yet enabled on all platforms; failures are expected.

### Multi Mode (disagg.multi=1)

Tests multiple disagg nodes (multi-follower scenario):

| Task Name | Args | Iterations | Tag |
|---|---|---|---|
| `format-stress-test-disagg-multi-1` | `disagg.multi=1 runs.predictable_replay=1 runs.ops=500000:2000000 ops.pct.delete=0` | 5 | `stress-test-disagg-fail`, `stress-test-disagg-san-fail` |
| `format-stress-test-disagg-multi-2` | same | 5 | `stress-test-disagg-fail` |
| `format-stress-test-disagg-multi-validation-1` | adds `ops.verify=1 disagg.multi_validation=1` | 5 | `stress-test-disagg-fail` |
| `format-stress-test-disagg-multi-validation-2` | same | 5 | `stress-test-disagg-fail` |
| `format-stress-test-disagg-multi-delete-enabled-1` | `disagg.multi=1 runs.predictable_replay=1 runs.ops=500000:2000000` (with deletes) | 5 | `stress-test-disagg-fail`, `stress-test-disagg-san-fail` |
| `format-stress-test-disagg-multi-delete-enabled-2` | same | 5 | `stress-test-disagg-fail` |
| `format-stress-test-disagg-multi-delete-enabled-validation-1` | adds `ops.verify=1 disagg.multi_validation=1` | 5 | `stress-test-disagg-fail` |
| `format-stress-test-disagg-multi-delete-enabled-validation-2` | same | 5 | `stress-test-disagg-fail` |

**Notes:**
- FIXME-WT-16481: Reopen currently causes failures in multi mode (`reopen: false`)
- FIXME-WT-17278: Remove operations in multi mode cause issues; delete=0 in most tasks, delete-enabled tasks are separate

---

## Coverage Gap

None of the format disagg stress tests are included in code coverage measurement. The `format test disagg` function and `CONFIG.disagg` paths are completely absent from `code_coverage_config.json`.
