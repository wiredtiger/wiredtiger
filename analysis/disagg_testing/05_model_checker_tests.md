# Disagg CI Testing — Model Checker Tests

> Category: Formal model checker with disaggregated storage enabled

---

## Overview

The WiredTiger model checker (`test/model/tools/model_test`) uses formal model checking to verify correctness properties. The disagg variants run with `-G disaggregated=1`, which instructs the model to generate and verify workloads in a disaggregated storage configuration.

---

## Tasks

| Task Name | Tag | Args | Duration | Description |
|---|---|---|---|---|
| `model-test-long-disagg` | `model_checking` | `-G disaggregated=1 -l 2000-3000 -t 3600` | 60 min | Fixed workload length range (2000-3000 operations), disagg mode |
| `model-test-long-random-config-disagg` | `model_checking` | `-G disaggregated=1 -l 100-200 -g -t 3600` | 60 min | Random config (`-g`), shorter workload range (100-200 ops), disagg mode |

Comparison with the non-disagg model tests:

| Non-Disagg Task | Args |
|---|---|
| `model-test-long` | `-l 2000-3000 -t 3600` |
| `model-test-long-random-config` | `-l 100-200 -g -t 3600` |

The only difference is the addition of `-G disaggregated=1`.

---

## Compile Flags

Model checker tests use the **default build** (no special coverage or sanitizer flags). They run on the same binary as the general compile task on each build variant.

---

## Build Variant Assignment

All disagg model tests run **once per day** (`batchtime: 1440`):

| Build Variant | `model-test-long-disagg` | `model-test-long-random-config-disagg` |
|---|---|---|
| `ubuntu2004` | yes (daily) | yes (daily) |
| `ubuntu2004-arm64` | yes (daily) | yes (daily) |
| `amazon2023-arm64` | yes (daily) | yes (daily) |
| `ubuntu2004-asan` | yes (daily) | yes (daily) |
| `amazon2023-arm64-asan` | yes (daily) | yes (daily) |

---

## What Is Being Verified

The model checker verifies **transactional correctness** properties (snapshot isolation, timestamp ordering, durability) for WiredTiger operations. With `-G disaggregated=1`, the model generates workloads that include disaggregated storage semantics:
- Leader/follower checkpoint propagation
- Visibility rules across role transitions
- Correctness of the page log checkpoint protocol

The `-g` (random config) variant generates random model configurations rather than a fixed one, increasing the space of scenarios explored.

---

## Coverage Gap

Model checker tests are not included in code coverage measurement. There is a separate `model-test-long-with-coverage` task that measures coverage, but it only covers `src/rollback_to_stable` — **there is no `model-test-long-disagg-with-coverage` task**. This means the disagg model test code paths under `src/` are not measured.
