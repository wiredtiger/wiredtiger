# Notes for Repeating the Test Coverage Analysis Exercise

> Written after the initial survey (May 2026). Updated after gap analysis phase (May 2026).  
> Read this before starting a new round.

---

## Directory Structure

```
test/analysis/
├── NOTES.md                    ← this file: methodology and pitfalls
├── SUMMARY.md                  ← executive summary of all findings
│
├── 01_per_test_analysis/       ← one .md per source test file
│   ├── suite/                  (747 files — Python API test suite)
│   ├── csuite/                 (42 files  — C crash/regression tests)
│   ├── cppsuite/               (16 files  — C++ stress test configs)
│   └── catch2/                 (65 files  — Catch2 unit tests)
│
├── 02_suite_analysis/          ← suite-level analysis (one entry per test program/dir)
│   ├── test_suites_overview.md (master overview of all 22 suites)
│   ├── format/                 (format stress test + all CONFIG profiles)
│   ├── model/                  (5 formal model test analyses)
│   ├── checkpoint/             checkpoint.md
│   ├── compatibility/          compatibility.md, multiversion.md
│   ├── cursor_order/           cursor_order.md
│   ├── fops/                   fops.md
│   ├── fuzz/                   fuzz.md
│   ├── huge/                   huge.md
│   ├── live_restore/           live_restore.md
│   ├── manydbs/                manydbs.md
│   ├── packing/                packing.md
│   ├── readonly/               readonly.md
│   ├── salvage/                salvage.md
│   ├── simulator/              simulator.md
│   ├── syscall/                syscall.md
│   └── thread/                 thread.md
│
└── 03_gap_analysis/            ← coverage gaps, duplicates, proposed tests
    ├── disagg_block_layer.md
    ├── disagg_cross_cutting_features.md
    ├── disagg_layered_checkpoint_rts.md
    ├── disagg_layered_cursor.md
    ├── disagg_layered_ingest_drain.md
    ├── disagg_layered_role_transitions.md
    ├── disagg_schema_metadata_recovery.md
    ├── c_level_and_unit_test_gaps.md
    ├── general_checkpoint_rts_hs_prepare.md
    └── test_suite_duplicates.md
```

---

## What Was Done

### Pass 1 — Suite inventory (May 2026)
1. Listed all directories under `test/`.
2. Read `README` files where present (`suite/README`, `csuite/README`, `cppsuite/README.md`, `catch2/README.md`, `format/README`).
3. Scanned `ls` output one level deep for each directory.
4. Spot-checked source entry points and key config/script files.
5. Produced `02_suite_analysis/test_suites_overview.md`.

### Pass 2 — Per-test analysis (May 2026)
For each test file in `suite/`, `csuite/`, `cppsuite/`, and `catch2/`, read the source and produced a dedicated `.md` file in `01_per_test_analysis/` covering:
- What test cases it contains
- Which WT components are exercised
- Notable observations (skips, FIXMEs, disagg/tiered coverage)

### Pass 3 — Gap analysis (May 2026)
Ten parallel analyses of specific subsystems and cross-cutting concerns, each reading source + test files and git history. Results in `03_gap_analysis/`. See `SUMMARY.md` for the consolidated findings.

---

## Critical: Two Distinct Storage Modes

These are **not the same feature** and must be tracked separately when assessing disagg coverage gaps.

| Mode | Source dirs | Test identifiers | Hook |
|---|---|---|---|
| **Tiered** | `src/tiered/` | `test_tiered*.py`, `helper_tiered.py` | `hook_tiered.py` |
| **Disaggregated** | `src/block_disagg/`, `src/conn/conn_layered*.c`, `src/cursor/cur_layered.c` | `test_disagg*.py`, `test_layered*.py` (and variants), `helper_disagg.py` | `hook_disagg.py` |

Key facts:
- **Layered tables are the key component of disagg, not a separate mode.** `src/block_disagg/` is the storage backend (page-log block manager); the layered table (`WT_LAYERED_TABLE`, `WT_CURSOR_LAYERED`) is the schema/cursor frontend. They are two layers of the same disagg feature.
- `hook_disagg.py` replaces row-store tables with **layered** tables — this is how it exercises disagg storage.
- `CONFIG.disagg` in `format/` tests the full layered+disagg stack; there is **no** `CONFIG.tiered`.
- Tiered uses the standard local block manager plus an extension flush path; disagg replaces the block manager entirely with `WT_BLOCK_DISAGG`. Completely different code paths.
- `test/evergreen_disagg.yml` is a separate CI project (`wiredtiger-disagg`) specifically for disagg testing.

---

## What Was Deliberately Skipped

- Individual test file content for `bench/` (outside `test/`) — `wtperf` workloads and `workgen`.
- `examples/` and `ext/` at repo root — not tests but exercisable code.
- The `evergreen.yml` / `evergreen_disagg.yml` / `evergreen_develop.yml` files — map tests to CI tasks/variants; important for CI coverage gaps (see Recommended Next Steps).
- The `dist/` tree — build-time code-generation scripts.

---

## Recommended Next Steps

### Step 4: Evergreen CI coverage map
Parse `test/evergreen.yml` to understand which tests actually run in CI vs exist only locally:
- Tests in `suite/` not in any Evergreen task bucket (see `suite/README` note about WT-4441)
- csuite tests missing from `evergreen.yml` (the `evg_cfg.py` tool flags these)
- Long-running cppsuite tests that run only on specific variants

### Step 5: Code coverage report
Run the existing coverage infrastructure:
```bash
cmake -DENABLE_COVERAGE=1 ...
./test/evergreen/code_coverage_analysis.sh
# Coverage report lands in test/evergreen/code_coverage/
```
This gives line-level coverage per source file and is the most precise gap signal.

### Step 6: Act on gap analysis findings
See `SUMMARY.md` → "Implementation Roadmap" for prioritized list of proposed tests.
Start with the CRITICAL disagg gaps (no crash/recovery test for disagg, no unit tests for `block_disagg/`).

---

## Key Files to Revisit

| File | Why |
|---|---|
| `test/evergreen.yml` | Maps every test to an Evergreen task — authoritative CI picture |
| `test/evergreen/evg_cfg.py` | Detects csuite tests missing from CI config |
| `test/evergreen/code_coverage/` | Coverage reports if a covered build was run |
| `test/suite/run.py` | Entry point and filtering logic for the Python suite |
| `test/format/format.h` | Config options for the format stress test |
| `test/cppsuite/src/` | cppsuite framework source |
| `dist/test_data.py` | Default config values for cppsuite tests |
| `bench/wtperf/` | Actual perf workloads (outside `test/`) |

---

## Pitfalls Encountered

- Several `main` and `usage` comments in C tests say "TODO: Add a comment describing this function" — the binary name and directory name are the primary sources of truth.
- `test/multiversion/` is largely superseded by `test/compatibility/`; treat with lower priority.
- `test/wtperf/` is thin — the real wtperf tests live in `bench/wtperf/`.
- `test/simulator/` tests *algorithm* not *storage*; it validates timestamp invariants, not the WT API.
- LazyFS csuite variants exist but are **not** in Evergreen; they require manual setup.
- `test/3rdparty/` contains locally-modified upstream libs — don't mistake them for WT tests.
- **Tiered ≠ Disagg**: `hook_tiered.py` and `hook_disagg.py` look similar but target completely different `src/` subtrees. Never conflate them in gap analysis.
- The `disagg` prefix in test names (`test_disagg*.py`) refers to the page-log layer (`src/block_disagg/`). The `layered` prefix (`test_layered*.py`) refers to the layered table abstraction on top. Both are parts of the same disagg feature.
- `test/format/CONFIG.disagg` exercises the full layered+disagg stack. There is no format config for tiered.
- **`test_layered43` is permanently skipped** (FIXME-WT-15663) — do not count it as coverage.
