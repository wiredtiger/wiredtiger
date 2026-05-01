# Notes for Repeating the Test Coverage Analysis Exercise

> Written after the initial survey (May 2026). Read this before starting a new round.

---

## What Was Done in the First Pass

1. Listed all directories under `test/`.
2. Read `README` files where present (`suite/README`, `csuite/README`, `cppsuite/README.md`, `catch2/README.md`, `format/README`).
3. Scanned `ls` output one level deep for each directory to see files and sub-directories.
4. Spot-checked source entry points (`main.c`, `t.c`, headers) and key config/script files for context.
5. Produced `test_suites_overview.md`.

## Critical: Two Distinct Storage Modes

These are **not the same feature** and must be tracked separately when assessing disagg coverage gaps.

| Mode | Source dirs | Test identifiers | Hook |
|---|---|---|---|
| **Tiered** | `src/tiered/` | `test_tiered*.py`, `helper_tiered.py` | `hook_tiered.py` |
| **Disaggregated** | `src/block_disagg/`, `src/conn/conn_layered*.c`, `src/cursor/cur_layered.c` | `test_disagg*.py`, `test_layered*.py` (and variants), `helper_disagg.py` | `hook_disagg.py` |

Key facts to keep in mind:
- **Layered tables are the key component of disagg, not a separate mode.** `src/block_disagg/` is the storage backend (page-log block manager); the layered table (`WT_LAYERED_TABLE`, `WT_CURSOR_LAYERED`) is the schema/cursor frontend. They are two layers of the same disagg feature.
- `hook_disagg.py` replaces row-store tables with **layered** tables — this is how it exercises disagg storage.
- `CONFIG.disagg` in `format/` tests the full layered+disagg stack; there is **no** `CONFIG.tiered`.
- Tiered uses the standard local block manager plus an extension flush path; disagg replaces the block manager entirely with `WT_BLOCK_DISAGG`. Completely different code paths.
- The Evergreen file `test/evergreen_disagg.yml` is a separate CI project (`wiredtiger-disagg`) specifically for disagg testing.

---

## What Was Deliberately Skipped

- Individual test file content (748 Python tests in `suite/`, 40+ csuite subdirs) — too granular for a first pass.
- `bench/` tree (outside `test/`) — contains `wtperf` workloads and `workgen`; worth a separate pass.
- `examples/` and `ext/` at repo root — these are not tests but contain exercisable code.
- The `evergreen.yml` / `evergreen_disagg.yml` / `evergreen_develop.yml` files — these map tests to CI tasks/variants; important for understanding coverage gaps in CI vs "can be run".
- The `dist/` tree — contains build-time code-generation scripts that can also affect what is testable.

---

## Recommended Next Steps

### Step 2: Map test files → source files
For each test prefix in `suite/` (e.g., `test_checkpoint*.py`) identify the corresponding `src/` subsystem. This produces a test-to-code coverage matrix. Strategy:
- `grep -r "WT_SESSION\|WT_CURSOR\|__wt_" test/suite/test_checkpoint*.py` to find what APIs are called
- Cross-reference with `src/` modules to see which `.c` files those APIs live in

### Step 3: Identify gaps
Run a source-file sweep: for each `.c` file in `src/`, check whether any test file imports or calls functions it defines. Likely gap areas based on this survey:
- `src/os_posix/` — only `syscall/` tests I/O patterns; no direct unit tests
- `src/lsm/` — covered by `format` with LSM config but sparse in `suite/`
- `src/packing/` — only `packing/` tests; `suite/test_pack.py` exercises it at API level
- `src/compress/` — covered by `suite/test_compress*.py` but no internal unit tests in `catch2/`
- `src/support/` — utility functions; coverage is implicit via other tests

For disagg specifically, assess:
- `src/block_disagg/` — has dedicated `test_disagg*.py` and `test_page_log_handle.cpp`; check what ops are missing
- `src/conn/conn_layered*.c` — very heavily tested by `test_layered*.py` (95 files), but crash/recovery and multi-connection scenarios are thinner
- `src/tiered/` — tiered has 20 Python tests + `hook_tiered.py`; no C-level stress tests, no catch2 unit tests

### Step 4: Evergreen CI coverage map
Parse `test/evergreen.yml` to understand which tests are actually run in CI vs exist only locally. Key things to look for:
- Tests in `suite/` that are not in any Evergreen task bucket (see the note in `suite/README` about WT-4441)
- csuite tests missing from `evergreen.yml` (the `evg_cfg.py` tool flags these)
- Long-running cppsuite tests that run only on specific variants

### Step 5: Code coverage report
Run the existing coverage infrastructure:
```bash
# Build with coverage instrumentation
cmake -DENABLE_COVERAGE=1 ...
# Run the test suite
./test/evergreen/code_coverage_analysis.sh
# Coverage report lands in test/evergreen/code_coverage/
```
This gives line-level coverage per source file and is the most precise way to answer "what is covered".

---

## Directory Structure Cheatsheet

```
test/
├── suite/          # 748 Python API tests — main regression suite
├── csuite/         # ~40 C tests — crash/recovery, bug regressions
├── cppsuite/       # C++ stress framework — configurable multi-threaded workloads
├── catch2/         # Catch2 unit tests — internals, below API
├── format/         # Comprehensive C stress test — broadest single-program coverage
├── model/          # C++ formal model — validates MVCC/timestamp/RTS semantics
├── checkpoint/     # C — concurrent checkpoint correctness
├── thread/         # C — concurrent R/W thread test
├── fops/           # C — schema/file operation concurrency
├── cursor_order/   # C — cursor ordering under concurrency
├── packing/        # C — integer pack/unpack format
├── huge/           # C — large value / eviction boundary
├── manydbs/        # C — many simultaneous connections
├── salvage/        # C — corruption recovery
├── readonly/       # C — read-only mode
├── fuzz/           # C (LibFuzzer) — config parsing, modify fuzzing
├── syscall/        # Python+strace — I/O pattern verification
├── simulator/      # C++ — timestamp logic simulator (no storage)
├── compatibility/  # Bash — cross-release on-disk format compat
├── multiversion/   # Bash — legacy cross-version compat
├── live_restore/   # Bash — live restore integration
├── wtperf/         # Python — wtperf config validation only
├── evergreen/      # Scripts — CI helpers, not tests
├── 3rdparty/       # Vendored Python test libs + nlohmann JSON
├── py_install/     # Shared Python test utilities
├── py_utility/     # Shared Python test utilities
└── wt_hang_analyzer/ # Hang detection tool
```

---

## Key Files to Revisit

| File | Why |
|---|---|
| `test/evergreen.yml` | Maps every test to an Evergreen task — authoritative CI picture |
| `test/evergreen/evg_cfg.py` | Detects csuite tests missing from CI config |
| `test/evergreen/code_coverage/` | Coverage reports if a covered build was run |
| `test/suite/run.py` | Entry point and filtering logic for the Python suite |
| `test/format/format.h` | Config options for the format stress test |
| `test/cppsuite/src/` | cppsuite framework source — understand how to add new stress tests |
| `dist/test_data.py` | Defines default config values for cppsuite tests |
| `bench/wtperf/` | Actual perf workloads (outside `test/`) |

---

## Pitfalls Encountered

- Several `main --` and `usage --` comments in C tests say "TODO: Add a comment describing this function" — the binary name and test directory name are the primary sources of truth for those.
- `test/multiversion/` is largely superseded by `test/compatibility/`; treat with lower priority.
- `test/wtperf/` is thin — the real wtperf tests live in `bench/wtperf/`.
- `test/simulator/` tests *algorithm* not *storage*; it validates timestamp invariants independently, not the WT API.
- LazyFS csuite variants exist but are **not** in Evergreen; they require manual setup.
- `test/3rdparty/` contains locally-modified upstream libs — don't mistake them for WT tests.
- **Tiered ≠ Disagg**: `hook_tiered.py` and `hook_disagg.py` look similar (both re-run normal tests with a modified storage mode) but target completely different `src/` subtrees. Never conflate them in gap analysis.
- The `disagg` prefix in test names (`test_disagg*.py`) refers to the page-log layer (`src/block_disagg/`). The `layered` prefix (`test_layered*.py`) refers to the layered table abstraction on top. Both are parts of the same disagg feature — `layered` tests are the *primary* disagg test body.
- `test/format/CONFIG.disagg` exercises the full layered+disagg stack. There is no format config for tiered.
