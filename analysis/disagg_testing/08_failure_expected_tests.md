# Disagg CI Testing — Failure-Expected Tests

> Category: Tests in the separate `wiredtiger-disagg` Evergreen project known to fail

---

## Overview

`test/evergreen_disagg.yml` defines a separate Evergreen project (`wiredtiger-disagg`) for disagg tests that are **currently expected to fail**. These tests exercise disagg modes that are still under development. The word "Failure Expected" appears in the build variant display names.

This project defines its own `compile` task, `format test disagg` function, and `timestamp abort disagg` function — duplicated from `evergreen.yml` rather than included via the `include` mechanism.

---

## Build Variants

| Name | Display Name | Run On | Build Type | Tasks |
|---|---|---|---|---|
| `amazon2023-disagg-stress` | `[Failure Expected] Amazon2023 ARM64 Disagg Stress tests` | `amazon2023.3-arm64-large` | Default (TCMalloc) | `.stress-test-disagg-fail` |
| `amazon2023-disagg-asan-stress` | `[Failure Expected] Amazon2023 ARM64 ASAN Disagg Stress tests` | `amazon2023.3-arm64-large` | ASan (clang) | `.stress-test-disagg-san-fail` |

Both variants set `disagg_run: true` (an expansion that may influence test behavior).

---

## Format Stress Tests — Switch Mode

**`disagg.mode=switch`** cycles through leader and follower roles during a single format test run. This exercises the full role-switching code path under a running workload.

FIXME-WT-16394: Switch mode is not yet stable on all platforms.

| Task Name | Tag | Args | Iterations |
|---|---|---|---|
| `format-stress-test-disagg-switch-1` | `stress-test-disagg-fail`, `stress-test-disagg-san-fail` | `disagg.mode=switch runs.timer=10:15` | 5 |
| `format-stress-test-disagg-switch-2` | `stress-test-disagg-fail` | `disagg.mode=switch runs.timer=10:15` | 5 |
| `format-stress-test-disagg-switch-3` | `stress-test-disagg-fail` | `disagg.mode=switch runs.timer=10:15` | 5 |
| `format-stress-test-disagg-switch-data-validation-1` | `stress-test-disagg-fail` | `disagg.mode=switch ops.verify=1 runs.mirror=1 table1.runs.source=table table1.disagg.enabled=0 runs.timer=5:10` | 5 |
| `format-stress-test-disagg-switch-data-validation-2` | `stress-test-disagg-fail` | same as above | 5 |
| `format-stress-test-disagg-switch-data-validation-3` | `stress-test-disagg-fail` | same as above | 5 |

**Total: 6 tasks.** `data-validation` variants mirror one table as a non-layered type for cross-validation.

---

## Format Stress Tests — Multi Mode

**`disagg.multi=1`** enables a multi-node disagg scenario (multiple concurrent connections). This is more complex than single leader/follower.

Key constraints in multi mode:
- **FIXME-WT-16481:** Reopen (`-R`) causes failures → `reopen: false` for all multi tasks
- **FIXME-WT-17278:** Remove operations cause failures → separate tasks with `ops.pct.delete=0` (no deletes) vs. delete-enabled tasks

#### Without deletes (ops.pct.delete=0)

| Task Name | Tag | Args | Iterations |
|---|---|---|---|
| `format-stress-test-disagg-multi-1` | `stress-test-disagg-fail`, `stress-test-disagg-san-fail` | `disagg.multi=1 runs.predictable_replay=1 runs.ops=500000:2000000 ops.pct.delete=0` | 5 |
| `format-stress-test-disagg-multi-2` | `stress-test-disagg-fail` | same | 5 |
| `format-stress-test-disagg-multi-validation-1` | `stress-test-disagg-fail` | adds `ops.verify=1 disagg.multi_validation=1` | 5 |
| `format-stress-test-disagg-multi-validation-2` | `stress-test-disagg-fail` | same | 5 |

#### With deletes enabled (transitional — FIXME-WT-17278)

These tasks will be removed once the remove issues are resolved:

| Task Name | Tag | Args | Iterations |
|---|---|---|---|
| `format-stress-test-disagg-multi-delete-enabled-1` | `stress-test-disagg-fail`, `stress-test-disagg-san-fail` | `disagg.multi=1 runs.predictable_replay=1 runs.ops=500000:2000000` (removes enabled) | 5 |
| `format-stress-test-disagg-multi-delete-enabled-2` | `stress-test-disagg-fail` | same | 5 |
| `format-stress-test-disagg-multi-delete-enabled-validation-1` | `stress-test-disagg-fail` | adds `ops.verify=1 disagg.multi_validation=1` | 5 |
| `format-stress-test-disagg-multi-delete-enabled-validation-2` | `stress-test-disagg-fail` | same | 5 |

**Total: 8 multi-mode tasks.**

---

## Timestamp Abort Tests — Disagg Mode

`test/csuite/timestamp_abort/test_timestamp_abort` run with `-G -s` flags to exercise crash recovery in disagg mode:

```bash
./test_timestamp_abort -G -s
```

| Task Name | Tag | Iterations |
|---|---|---|
| `timestamp-abort-test-disagg-1` | `stress-test-disagg-fail` | 50 |
| `timestamp-abort-test-disagg-2` | `stress-test-disagg-fail` | 50 |

50 iterations per task because the failures are intermittent. These test that crash recovery works correctly after a disagg workload.

**Note:** These run only in `amazon2023-disagg-stress` (not the ASAN variant), as ASAN would change timing characteristics.

---

## Task-to-Variant Matrix

| Task Tag | `amazon2023-disagg-stress` | `amazon2023-disagg-asan-stress` |
|---|---|---|
| `stress-test-disagg-fail` | yes | — |
| `stress-test-disagg-san-fail` | — | yes |

Tasks tagged `stress-test-disagg-san-fail` (switch-1, multi-1, multi-delete-1) run on the ASAN variant in addition to the normal variant.

---

## Key Known Issues in These Tests

| FIXME | Issue | Affected Tasks |
|---|---|---|
| WT-16394 | Switch mode not stable on all platforms | all switch tasks |
| WT-16481 | Reopen causes failures in multi mode | all multi tasks (`reopen: false`) |
| WT-17278 | Remove operations cause failures in multi mode | multi delete-enabled tasks are a workaround |

---

## Coverage Gap

All tasks in `evergreen_disagg.yml` are in the separate failure-expected project. They are not measured for code coverage. They represent the leading edge of disagg feature development.
