# Notes for Repeating the Coverage Analysis Exercise

> Written 2026-05-01. Update the "last verified" dates when you re-run.

---

## Where to Start

The CI configuration spans **two YAML files**:

| File | What's in it |
|---|---|
| `test/evergreen.yml` | All task definitions, functions, and most build variants |
| `test/evergreen_develop.yml` | Develop-branch-only build variants — **this is where `code-statistics` is defined** |

The `code-statistics` build variant in `evergreen_develop.yml` is the primary home of coverage tasks. Without reading that file you will miss all the main coverage tasks entirely, because `evergreen.yml` only defines the tasks, not which build variant runs them.

```bash
grep -n "coverage" test/evergreen.yml
grep -n "coverage" test/evergreen_develop.yml
```

---

## Key Search Patterns

```bash
# Find all coverage tasks in evergreen.yml
grep -n "coverage" test/evergreen.yml

# Find all coverage-related files in the repo
find . -name "*coverage*" -type f

# Find which build variants run coverage tasks (cross-file)
grep -n "code-statistics\|pull_request_code_statistics" test/evergreen*.yml

# Find what the code-statistics build variant runs
grep -A 30 "^- name: code-statistics" test/evergreen_develop.yml
```

---

## The Coverage Pipeline (Step by Step)

1. **Build** — CMake with `CMAKE_BUILD_TYPE=Coverage` generates `.gcno` files alongside object files
2. **Setup** — `parallel_code_coverage.py -s` (setup mode): creates N build directories, compiles in the first, copies to the rest
3. **Run tests** — `parallel_code_coverage.py` (no `-s`): runs each test in a worker process using its own build directory; sets `GCOV_PREFIX`/`GCOV_PREFIX_STRIP` so `.gcda` files land in the right place
4. **Aggregate** — `code_coverage_analysis.sh` calls `gcovr` which crawls all `build_*` directories for `.gcda` + `.gcno` and produces HTML + JSON
5. **Combine** — for the split python/other approach, `generate-coverage-report` combines two gcovr JSON tracefiles with `--add-tracefile`
6. **Publish** — HTML to S3, summary metrics to MongoDB Atlas

### Key insight: The split python/other approach

`coverage-report-python` and `coverage-report-other` run on two separate hosts in parallel, each taking half the test list. `generate-coverage-report` then merges. This is purely a CI time optimization — gcovr supports combining JSON tracefiles directly.

---

## The Test List

The master test list lives in:
- `test/evergreen/code_coverage/code_coverage_config.json` — main list (~250 entries)
- `test/evergreen/code_coverage/code_coverage_config_catch2.json` — Catch2 only

**Splitting logic** (in `parallel_code_coverage.py`):
- `--bucket python` → only entries where the command contains the string `"python"`
- `--bucket other` → all entries where `"python"` is NOT in the command string
- No `--bucket` → everything

The split is purely string-matching on `"python"`. This means any non-python test with "python" in its path would go to the wrong bucket (not currently an issue but worth knowing).

---

## Per-Test Coverage

`coverage-report-per-test` is **disabled** (`activate: false`) and must be triggered manually. It uses `per_test_code_coverage.py` which:
1. Runs each test in the same way as the parallel script
2. **After each test**, copies the entire build directory to `build_<N>_<index>_copy/`
3. Runs `gcovr` on each copy separately to get per-test data
4. Stores a `task_info.json` in each copy identifying which test it corresponds to

This is very disk-intensive (N build dirs × M tests copies).

---

## The `model-test-long-with-coverage` Task

This task is different from all others:
- It is NOT in the `code-statistics` build variant — it runs in `ubuntu2004`, `ubuntu2004-arm64`, `amazon2023-arm64`
- It does NOT use `parallel_code_coverage.py` — it compiles and runs `model_test` directly
- The coverage filter is **`src/rollback_to_stable`** not `src` — so gcovr only reports on files under that subdirectory
- Runs daily, not on PRs

---

## Tooling Versions (pinned in `code_coverage_analysis.sh`)

```
lxml==4.8.0
Pygments==2.11.2
Jinja2==3.0.3
gcovr==5.0
```

And for per-test / change reports:
```
pygit2==1.10.1
requests==2.32.3  (or just requests)
```

These are installed into a virtualenv at runtime. If you run this locally, these versions must be compatible with your Python 3.

---

## What's Not in the Coverage System

Things that exist in the repo but are NOT measured in CI:

| Thing | Reason |
|---|---|
| `test/fuzz/fuzz_coverage.sh` | Uses LLVM instrumentation (`llvm-cov`), not gcov. Manual only. |
| `test/format/CONFIG.coverage` | A format test config tuned for coverage (fast runs, many threads). No CI task uses it. |
| Full Python test suite | Coverage config runs a curated subset; many test files are absent. |
| RTS tests 10,12,14,20,26,35,37,38,39 | Explicitly excluded — too slow. |
| TSAN/ASAN/sanitizer builds | Separate build variants, no coverage. |

---

## How to Read the Coverage Report

The gcovr reports produced are:

| File | Content |
|---|---|
| `coverage_report/2_coverage_report.html` | HTML detail report (per-file, per-line) |
| `coverage_report/1_coverage_report_summary.json` | JSON summary with `branch_percent`, `line_percent`, per-file data |
| `coverage_report/full_coverage_report.json` | Full gcovr JSON tracefile (used for combining and per-test analysis) |
| `coverage_report/atlas_out_code_coverage.json` | Atlas-format report: overall + per-component (by `src/<component>`) branch coverage |

The `atlas_out_code_coverage.json` breaks coverage down by top-level subdirectory under `src/`, e.g. `src/btree`, `src/txn`, etc.

---

## Re-Running This Analysis

If you redo this exercise:

1. Re-run the search commands above on the current `test/evergreen.yml` and `test/evergreen_develop.yml`
2. Diff `test/evergreen/code_coverage/code_coverage_config.json` against this doc's test list to find additions/removals
3. Check for new tasks tagged `pull_request_code_statistics` — this tag marks all coverage-related tasks
4. Check `evergreen_disagg.yml` for coverage-related additions (currently none)
5. Look for `CMAKE_BUILD_TYPE=Coverage` in new task definitions

The tasks tagged `pull_request_code_statistics` in `test/evergreen.yml` as of the analysis date:
- `coverage-report-python` (line 4235)
- `coverage-report-other` (line 4264)
- `generate-coverage-report` (line 4290)
- `coverage-report-catch2` (line 4343)
- `code-change-report` (line 4359)
- Plus a few code statistics tasks (cyclomatic complexity, modularity) at lines ~5552, 5589, 5601

---

## Common Gotchas

- **`check_coverage` expansion** (evergreen.yml line 961): When `check_coverage=true` is set, test failures in the Python test suite are silently ignored (`|| echo "Ignoring failed test..."`). This means coverage runs don't abort on test failures. Check whether this is set if you're debugging why a broken test doesn't fail the coverage task.

- **GCOV path issues**: When build directories are copied, gcov can fail because the `.gcno` files have hardcoded absolute paths from the original build. This is why `GCOV_PREFIX` and `GCOV_PREFIX_STRIP` must be set correctly — they remap the source paths. If coverage data looks wrong or empty, this is the first thing to check.

- **The `combine_coverage_report` flag**: When `code_coverage_analysis.sh` is called with `combine_coverage_report=true`, it uses `gcovr --add-tracefile` to merge two JSON tracefiles. It does NOT re-run any tests — it only combines existing data. The filter (`-f coverage_filter`) still applies during combination.

- **Test ordering in `code_coverage_config.json`**: Tests are ordered by descending runtime (longest first) to minimize time waiting for the slowest worker. If you add tests, keep them sorted or run `parallel_code_coverage.py --optimize_test_order` to re-sort (cannot be combined with `--bucket`).
