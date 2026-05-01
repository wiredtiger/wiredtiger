# Disagg CI Testing — Checkpoint and csuite Tests

> Category: ctest with `check_disagg` label; timestamp abort in disagg mode

---

## Checkpoint Tests with `check_disagg` Label

### Background

The WiredTiger checkpoint test (`test/checkpoint/`) uses ctest with labels to control which tests run. Tests are labeled `check_disagg` if they specifically exercise disaggregated storage checkpoint behavior.

The standard `csuite-tests-fast` and `checkpoint-test` tasks **exclude** `check_disagg`-labeled tests with:
```
ctest_extra_args: -LE "long_running|check_disagg"
```

Disagg-specific checkpoint tests are run only by the dedicated task:

### checkpoint-test-disagg-leader

| Property | Value |
|---|---|
| Task name | `checkpoint-test-disagg-leader` |
| Tag | `stress-test-disagg` |
| ctest filter | `-L "check_disagg"` (only `check_disagg`-labeled tests) |
| Directory | `test/checkpoint` |
| Build | Standard compile (no special flags) |

This runs **only** ctest tests labeled `check_disagg` — the set that exercises disagg leader checkpoint behavior. These are C-level checkpoint tests, not Python tests.

**Build variants:** All variants that run `.stress-test-disagg` (the full stress test pool: `ubuntu2004-stress-tests`, `ubuntu2004-stress-tests-arm64`, `ubuntu2004-release-stress-tests`, `ubuntu2004-release-stress-tests-arm64`, `amazon2023-stress-tests-arm64`, and several nonstandalone/ASAN variants).

---

## Timestamp Abort in Disagg Mode (evergreen_disagg.yml)

The **`timestamp abort`** test (`test/csuite/timestamp_abort/`) is a crash-recovery test. In disagg mode it is run with special flags that enable disaggregated storage mode:

```bash
./test_timestamp_abort -G -s
```

Where `-G` enables disaggregated mode and `-s` enables the disagg-specific scenario.

### Tasks (in evergreen_disagg.yml, Failure Expected)

| Task Name | Tag | Iterations | Build Variants |
|---|---|---|---|
| `timestamp-abort-test-disagg-1` | `stress-test-disagg-fail` | 50 | `amazon2023-disagg-stress` |
| `timestamp-abort-test-disagg-2` | `stress-test-disagg-fail` | 50 | `amazon2023-disagg-stress` |

Running 50 iterations is necessary to reproduce intermittent failures. These are in the "Failure Expected" project because they are known to fail.

---

## csuite Fast Tests (Disagg Exclusion)

For reference: the standard csuite task explicitly **excludes** disagg-labeled tests:

```yaml
- name: csuite-tests-fast
  ctest_extra_args: -LE "long_running|check_disagg" -j ${num_jobs}
```

This means all `check_disagg`-labeled csuite tests are **only** run by `checkpoint-test-disagg-leader`.

---

## Coverage Gap

- `checkpoint-test-disagg-leader` (ctest with `check_disagg` label) — not in code coverage
- `timestamp-abort-test-disagg` (timestamp abort in disagg mode) — not in code coverage, and lives in the separate failure-expected Evergreen project
