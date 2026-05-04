## Project Overview

WiredTiger is a high-performance, embedded key-value storage engine written in C. It is the default storage engine for MongoDB. Tests and tooling use C, C++, and Python.

## Build & Test

```bash
# Configure (Ninja recommended)
mkdir build && cd build
cmake -G Ninja ..

# Useful CMake options:
#   -DHAVE_DIAGNOSTIC=1   diagnostic checks (default for non-Release)
#   -DHAVE_UNITTEST=1     Catch2 unit tests
#   -DENABLE_PYTHON=1     Python API
#   -DCMAKE_BUILD_TYPE=   Release | Debug | ASan | TSan | UBSan | MSan

ninja                           # build
ctest -j$(nproc)                # all C/C++ tests
ctest -R <regex> -j$(nproc)     # subset by name

# Catch2 (run from build/, requires -DHAVE_UNITTEST=1)
./test/catch2/catch2-unittests              # all
./test/catch2/catch2-unittests "[tag]"      # one subsystem tag

# Python suite (run from build/, requires -DENABLE_PYTHON=1)
python3 ../test/suite/run.py                # all
python3 ../test/suite/run.py <test_name>    # single test
```

## Formatting & Validation

Run from the repo root before submitting:

```bash
cd dist && ./s_all     # full validation + clang-format
cd dist && ./s_fast    # fast subset, only changed files
```

`s_all` also regenerates code from data definitions in `dist/` (see below). Re-run it after editing any `dist/*.py` data file.

## Code Architecture

### Source layout (`src/`)

- **Data path**: `btree/`, `cursor/`, `block/`, `reconcile/`
- **Transactions & durability**: `txn/`, `checkpoint/`, `log/`, `rollback_to_stable/`, `history/`
- **Connection & session**: `conn/`, `session/`, `schema/`
- **Memory**: `cache/`, `evict/`
- **Storage extensions**: `block_cache/`, `block_disagg/`, `live_restore/`, `tiered/`
- **Platform**: `os_posix/`, `os_win/`, `os_common/`, `os_darwin/`, `os_linux/`
- **Infrastructure**: `config/`, `support/`, `meta/`, `packing/`, `checksum/`
- **Headers**: `include/` — `wiredtiger.h.in` is the public API template; `wt_internal.h` aggregates internal headers
- **CLI**: `utilities/` — the `wt` command-line tool

### Public API handles

- `WT_CONNECTION` — database connection (typically one per process)
- `WT_SESSION` — operational context (one per thread)
- `WT_CURSOR` — key-value iterator (owned by a session)

### Generated code (`dist/`)

Python scripts generate C code from data definitions. Edit the data file, then run `dist/s_all`:

- `api_data.py` → config parsing
- `stat_data.py` → statistics
- `log_data.py` → log records
- `flags.py` → flag values
- `prototypes.py` → function prototypes

### Examples

Working code samples live in `examples/c/` and `examples/python/`.

## C Coding Conventions

Full rules in `CONTRIBUTING.rst`. Highlights:

- **Style**: K&R, 2-space indent, 100-char lines, `clang-format` enforced
- **Comments**: C-style `/* ... */` only, full sentences. Use `FIXME-WT-NNNN` to reference open Jira tickets
- **Function naming**:
  - `__wt_<subsys>_*` — used across directories
  - `__wti_<subsys>_*` — used within a single subsystem directory
  - `__<subsys>_*` — file-local (static)
- **Returns**: `return (value);` with parentheses; output params end in `p` and go last
- **Variables**: `WT_SESSION_IMPL *session`, `WT_CONNECTION_IMPL *conn`. Declare in alphabetical order. `WT_DECL_RET` declares `ret`.
- **Error handling** (`src/include/error.h`):
  - `WT_RET(call)` — return on error
  - `WT_ERR(call)` — set `ret`, jump to `err:` label
  - `WT_TRET(call)` — accumulate without overwriting
- **Misc**: `p == NULL` (not `!p`); `for (;;)` (not `while (true)`); single-statement blocks omit braces unless ambiguous

## Test Frameworks

| Framework | Location | Purpose |
|-----------|----------|---------|
| Catch2 | `test/catch2/` | C++ unit tests below the API (needs `-DHAVE_UNITTEST=1`) |
| Python suite | `test/suite/` | Functional/integration via Python API |
| csuite | `test/csuite/` | C-based sanity and integration |
| format | `test/format/` | Randomized stress/fuzz |
| cppsuite | `test/cppsuite/` | C++ stress framework |
| checkpoint | `test/checkpoint/` | Checkpoint stress |
| model | `test/model/` | Lightweight formal verification |
| wtperf | `bench/wtperf/` | Performance benchmarks |

## CI

Runs on MongoDB Evergreen. Config: `test/evergreen.yml` (and `test/evergreen_disagg.yml` for disaggregated storage).
