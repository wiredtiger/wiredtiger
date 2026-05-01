# Disagg CI Testing — Python Hook Tests

> Category: Running the WiredTiger Python test suite under the disagg hook

---

## Mechanism

The **disagg hook** (`test/suite/hook_disagg.py`) intercepts standard WiredTiger Python API calls and transparently redirects row-store tables to layered tables. This allows the existing Python test suite (written for normal btree storage) to be re-run under disaggregated storage with no changes to the tests themselves.

What the hook does:
- Wraps `wiredtiger_open()` to inject the page log extension config and disagg storage source
- Intercepts `Session.create()` to change `table:` URIs to `layered:` URIs
- Wraps `alter`, `checkpoint`, `compact`, `drop`, `open_cursor`, `truncate`, `verify` to handle layered URI rewriting
- **Skips** tests that are incompatible with disagg (backup, bulk loads, named checkpoints, salvage, LSM) by raising `WiredTigerSkip`

Hook parameters (passed as `--hook "disagg=(...)"`):
| Parameter | Values | Default | Description |
|---|---|---|---|
| `role` | `leader`, `follower` | `leader` | Disagg role for this connection |
| `table_prefix` | `layered`, `table` | `layered` | What URI prefix to use for tables |
| `key_provider` | `true`/`false` | (not set) | Enable key encryption provider |
| `config` | string | (empty) | Extra page log config |
| `page_log` | `palite`, etc. | `palite` | Page log extension to use |

---

## Tasks in evergreen.yml

### Pull-Request Tasks (run on every PR, `unit_test_disagg` tag)

These tasks run the full Python test suite in parallel batches (5 batches × 1/5 of tests each):

| Task Name | Hook Args | Batches | Ignore List | Tags |
|---|---|---|---|---|
| `unit-test-hook-disagg-leader-bucket00` | `disagg=(role=leader)` | `--batch 0/5` | `hook_disagg.fail` | `pull_request`, `unit_test_disagg` |
| `unit-test-hook-disagg-leader-bucket01` | `disagg=(role=leader)` | `--batch 1/5` | `hook_disagg.fail` | `pull_request`, `unit_test_disagg` |
| `unit-test-hook-disagg-leader-bucket02` | `disagg=(role=leader)` | `--batch 2/5` | `hook_disagg.fail` | `pull_request`, `unit_test_disagg` |
| `unit-test-hook-disagg-leader-bucket03` | `disagg=(role=leader)` | `--batch 3/5` | `hook_disagg.fail` | `pull_request`, `unit_test_disagg` |
| `unit-test-hook-disagg-leader-bucket04` | `disagg=(role=leader)` | `--batch 4/5` | `hook_disagg.fail` | `pull_request`, `unit_test_disagg` |
| `unit-test-hook-disagg-leader-table-bucket00` | `disagg=(role=leader,table_prefix=table)` | `--batch 0/5` | `hook_disagg.fail` | `pull_request`, `unit_test_disagg` |
| `unit-test-hook-disagg-leader-table-bucket01` | `disagg=(role=leader,table_prefix=table)` | `--batch 1/5` | `hook_disagg.fail` | `pull_request`, `unit_test_disagg` |
| `unit-test-hook-disagg-leader-table-bucket02` | `disagg=(role=leader,table_prefix=table)` | `--batch 2/5` | `hook_disagg.fail` | `pull_request`, `unit_test_disagg` |
| `unit-test-hook-disagg-leader-table-bucket03` | `disagg=(role=leader,table_prefix=table)` | `--batch 3/5` | `hook_disagg.fail` | `pull_request`, `unit_test_disagg` |
| `unit-test-hook-disagg-leader-table-bucket04` | `disagg=(role=leader,table_prefix=table)` | `--batch 4/5` | `hook_disagg.fail` | `pull_request`, `unit_test_disagg` |
| `unit-test-hook-disagg-follower` | `disagg=(role=follower)` | (not batched) | (none) | `pull_request` |
| `unit-test-hook-disagg-follower-table` | `disagg=(role=follower,table_prefix=table)` | (not batched) | (none) | `pull_request` |

**Note on follower tasks:** Follower tasks run only `base01` test — a single sanity-check test, not the full suite. The comment in the config says "minimal functionality test". Timeout is 60s vs 1800s for leader tasks.

**Note on `table_prefix=table` variant:** This is a second pass that uses `table:` URIs instead of `layered:` URIs — the hook still injects disagg storage config but the URI rewriting doesn't happen. This tests the alternative URI mode.

### Per-commit / Non-PR Tasks

| Task Name | Hook Args | Scope | Notes |
|---|---|---|---|
| `unit-test-hook-disagg-leader-extra-long` | `disagg=(role=leader)` | Full suite + `--extra-long` | Runs extra-long tests; timeout 3600s |
| `unit-test-hook-disagg-leader-key-provider` | `disagg=(role=leader,key_provider=true)` | Full suite (excl. fail list) | With encryption key provider enabled |
| `unit-test-hook-disagg-leader-macos` | `disagg=(role=leader)` | Full suite | macOS build; timeout 1800s |

### TSAN-Specific Tasks

These run under the TSAN build variant using parallel test execution (`unit test tsan parallel`):

| Task Name | Hook Args | Scope | Notes |
|---|---|---|---|
| `unit-test-hook-disagg-leader-tsan` | `disagg=(role=leader)` | Full suite (excl. tsan.fail + hook_disagg.fail) | TSAN parallel; `detect_deadlocks=0` |
| `unit-test-hook-disagg-follower-tsan` | `disagg=(role=follower)` | `base01` only | Follower; minimal test |
| `unit-test-hook-disagg-leader-tsan-bucket00..04` | `disagg=(role=leader)` | 1/5 each | PR-mode TSAN buckets |

### TSAN Metrics

| Task Name | Depends On | Purpose |
|---|---|---|
| `generate-tsan-metric-disagg` | leader-tsan + follower-tsan | Counts TSAN warnings, sends to Atlas |
| `generate-tsan-metric-disagg-timestamp` | (also in ubuntu2004-tsan/amazon2023-arm64-tsan) | Same, for timestamp variant |

---

## Excluded Tests (hook_disagg.fail)

These tests are known failures under the disagg hook and are excluded from all hook tasks. Comments indicate some have known bugs (FIXME tickets):

```
test_autoclose.py
test_config02.py
test_config09.py
test_cursor13.py          # FIXME: WT-15369
test_cursor21.py          # FIXME: WT-15369
test_cursor_random.py     # FIXME: WT-15189
test_drop03.py
test_dump.py, test_dump01-05.py
test_dupc.py
test_durable_ts01.py      # FIXME: WT-15370
test_durable_ts03.py
test_empty.py
test_encrypt06.py
test_env01.py
test_error_info01.py
test_error_info03.py      # FIXME: WT-16872
test_hs01.py              # FIXME: WT-15371
test_hs24.py              # FIXME: WT-16872
test_hs_evict_race01.py   # FIXME: WT-16872
test_log03.py
test_metadata_cursor01.py, test_metadata_cursor04.py
test_prepare28.py         # FIXME: WT-16872
test_readonly01.py
test_readonly03.py        # FIXME: WT-14582
test_shared_cache01.py
test_stat01.py
test_stat_log02.py
test_sweep05
test_util01.py, test_util02.py, test_util04.py, test_util07.py,
test_util09.py, test_util11-15.py, test_util17.py
test_verbose01.py         # FIXME: WT-15372
test_verbose02.py, test_verbose04.py
```

46 total test files excluded. Note: no known reason is documented for many exclusions ("known failures, reasons not known").

---

## Build Variants Running These Tasks

| Build Variant | `.unit_test_disagg` (PR buckets) | extra-long | TSAN | key-provider | follower |
|---|---|---|---|---|---|
| `ubuntu2004` | — | yes | — | yes | — |
| `ubuntu2004-asan` | yes | — | — | — | — |
| `ubuntu2004-tsan` | — | — | yes (leader+follower+buckets) | — | yes (tsan) |
| `ubuntu2004-arm64` | — | yes | — | — | — |
| `ubuntu2004-nonstandalone` | — | yes | — | — | — |
| `ubuntu2004-arm64-nonstandalone` | — | yes | — | — | — |
| `ubuntu2004-arm64-release-nonstandalone` | — | yes | — | — | — |
| `amazon2023-arm64` | — | yes | — | — | — |
| `amazon2023-arm64-asan` | yes | — | — | — | — |
| `amazon2023-arm64-tsan` | — | — | yes (leader+buckets) | — | — |
| `amazon2023-arm64-nonstandalone` | — | yes | — | — | — |
| `amazon2023-arm64-release-nonstandalone` | — | yes | — | — | — |
| `rhel80` | — | yes | — | — | — |

The `.unit_test_disagg` tag tasks (10 buckets total: 5 leader + 5 leader-table) run on ASAN variants only. Non-ASAN variants run only the extra-long variant (not on every PR).

---

## Coverage Gap

The hook tasks run **most of the Python test suite** under disagg conditions, but:
- The follower role only runs `base01` — a single test
- 46 test files in `hook_disagg.fail` are completely skipped with no coverage
- `table_prefix=table` variant only runs on ASAN builds (not all architectures)
- **None of these hook tasks appear in the code coverage config** (`code_coverage_config.json`). Only the disagg hook variant for `base` is included: `python3 ../test/suite/run.py --hook disagg --skip-tests-in-file ../test/suite/hook_disagg.fail base` — a single test class, not the full suite.
