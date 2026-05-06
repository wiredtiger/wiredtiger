# WiredTiger Test Suites Overview

> **Status:** Initial survey — May 2026  
> **Scope:** Top-level directories under `test/`; one level deep with rough code-area mapping.

---

## Storage Mode Glossary

Two distinct storage modes exist in WT for this analysis. They are **not interchangeable** and test different `src/` subtrees.

| Mode | Key Source | What it is |
|---|---|---|
| **Tiered** | `src/tiered/` | Hot/cold tiering: older immutable objects are flushed from local disk to shared object storage (S3, GCS, `dir_store`). Uses extension (`storage_source`) API and `WT_BUCKET_STORAGE`. Tables are still normal btrees locally; only cold objects migrate. |
| **Disaggregated** | `src/block_disagg/`, `src/conn/conn_layered*.c`, `src/cursor/cur_layered.c` | Compute–storage separation. The block manager is replaced with a *page-log* backend (`WT_BLOCK_DISAGG`, default implementation: `palite`). The primary user-facing construct is the **layered table** (`WT_LAYERED_TABLE`): an *ingest* btree receives writes locally and a background drain process checkpoints them into the *stable* btree, which is backed by the page log. Layered tables are not a separate mode — they *are* the disagg storage model. There are no local data files for the stable side; everything lives on the page log. |

> **Rule of thumb for this analysis:** "Disagg testing" covers both `src/block_disagg/` (the page-log block backend) and `src/conn/conn_layered*.c` + `src/cursor/cur_layered.c` (the layered table abstraction) — they are two layers of the same feature. Tiered is a separate, unrelated feature and should be tracked independently.

---

## Directory Inventory

### `suite/` — Python API Test Suite
**Language/Framework:** Python (custom unittest runner via `run.py`)  
**Scale:** 748 test files  
**Trigger:** CI (`make check`), Evergreen tasks (batched into buckets)  
**What it tests:**  
WiredTiger public API surface end-to-end. Each test prefix maps to a subsystem.

**General storage tests (mainstream btree / row-store / col-store):**
- `alter` — table alteration
- `backup` — backup API (full + incremental)
- `bulk` — bulk cursor load
- `checkpoint` / `checkpoint_snapshot` — checkpoint correctness and snapshot isolation
- `compact` — online compaction
- `cursor`, `cursor_bound`, `cursor_random`, `cursor_compare` — cursor semantics
- `encrypt` — encryption extensions
- `eviction` — eviction behaviour and cache policy
- `hs`, `hs_evict_race`, `prepare_hs` — history store
- `import` — table import
- `index` — secondary indices
- `isolation` — snapshot/read-committed/read-uncommitted correctness
- `live_restore` — online restore from backup
- `log` — write-ahead logging
- `modify` — in-place partial updates
- `prepare` / `prepare_cursor` / `prepare_discover` — prepared transactions
- `recovery` — crash/restart recovery
- `rollback_to_stable` / `durable_rollback_to_stable` — RTS correctness
- `salvage` — salvage from corruption
- `schema`, `drop`, `drop_create` — schema operations
- `stat`, `stat_log` — statistics API
- `timestamp`, `durable_ts` — timestamp APIs
- `truncate` — table/range truncation
- `txn`, `txn_uncommitted` — transaction lifecycle
- `verify` — on-disk verification
- `compress`, `pack`, `intpack` — data encoding
- `config`, `baseconfig`, `reconfig` — configuration parsing + reconfiguration
- `debug_mode`, `debug_info` — diagnostic/debug modes
- `reconcile` — page reconciliation
- `split` — page splits
- `sweep` — handle/cursor sweeping
- `inmem` — in-memory mode
- `util` — `wt` CLI utility
- ... and ~20 more

**Tiered storage tests (20 test files):**
- `test_tiered02` – `test_tiered23` — tiered tree creation, flush, shared object storage, concurrent flush and CRUD, `dir_store` / cloud storage backends

**Disaggregated storage tests (110+ test files):**
- `test_disagg01` – `test_disagg04` — basic disagg connection and table operations over the page log (block_disagg layer)
- `test_disagg_checkpoint_size01` – `test_disagg_checkpoint_size04` — checkpoint size tracking with disagg storage
- `test_layered01` – `test_layered97` — the main disagg test body; exercises the layered table API end-to-end (creation, CRUD, checkpointing, ingest drain, metadata correctness); layered tables are the primary disagg user-facing construct
- `test_layered_cursor01` — cursor semantics on layered tables
- `test_layered_fast_truncate01` – `test_layered_fast_truncate03` — fast truncate on layered tables
- `test_layered_modify01` — partial updates on layered tables
- `test_verify_disagg`, `test_verify_disagg02` — on-disk verification for disagg tables
- `test_key_provider_disagg01`, `test_key_provider_disagg02` — encryption key provider integration with disagg storage
- `test_leaf_delta_disagg01` — leaf-page delta encoding specific to disagg

**Hooks (execution-context modifiers):**
- `hook_tiered.py` — re-runs any normal test with all tables converted to tiered storage (separate from disagg)
- `hook_disagg.py` — re-runs any normal test with all tables converted to layered tables backed by disagg storage; used to expand disagg test coverage of the general suite without writing new tests

**Code areas covered (roughly):**  
`src/cursor/`, `src/btree/`, `src/txn/`, `src/schema/`, `src/log/`, `src/history/`, `src/backup/`, `src/evict/`, `src/block/`, `src/conn/`, `src/config/`, `src/stat/`, `src/reconcile/`, `src/compress/`, `src/tiered/` (tiered tests), `src/block_disagg/` (disagg tests), `src/conn/conn_layered*.c` (layered tests)

---

### `csuite/` — C Sanity Tests
**Language/Framework:** C (one executable per subdirectory)  
**Scale:** ~40 individual tests  
**Trigger:** `make check`, Evergreen (one task per test)  
**What it tests:**

- Crash-safety / recovery scenarios requiring a real subprocess fork+kill:
  - `random_abort`, `schema_abort` — random ops + SIGABRT; verify post-recovery
  - `timestamp_abort` — timestamp-aware txn crash + recovery (has minor disagg references but is not a disagg-targeted test)
  - `truncated_log` — log truncation edge cases
- Storage stress:
  - `random`, `random_directio`, `random_session` — random CRUD under various I/O modes
  - `incr_backup` — incremental backup correctness under concurrent writes
  - `wt8057_compact_stress`, `wt7989_compact_checkpoint` — compaction + checkpoint races
  - `wt8963_insert_stress`, `wt10461_skip_list_stress` — insert concurrency
  - `wt13867_interrupt_eviction_handler` — eviction interruption
- Specific bug regressions: `wt2695_checksum`, `wt2909_checkpoint_integrity`, `wt3338_partial_update`, `wt6185_modify_ts`, `wt6616_checkpoint_oldest_ts`, `wt9199_checkpoint_txn_commit_race`, `wt4156_metadata_salvage`, `wt12015_backup_corruption` — each named after the ticket it guards
- Config / schema edge cases: `wt11126_compile_config`, `wt11440_config_check`, `wt3184_dup_index_collator`, `wt3874_pad_byte_collator`
- `rwlock` — R/W lock correctness
- `scope` — object-lifetime / ownership checks

> **Disagg / Tiered coverage:** None of the csuite tests directly target disagg or tiered storage. There are no csuite subdirectories for `disagg`, `layered`, or `tiered`.

**LazyFS variants:** Some tests have FUSE-based power-failure emulation (requires `ENABLE_LAZYFS=1`); not in Evergreen, manual only.

**Code areas covered (roughly):**  
`src/txn/`, `src/log/`, `src/block/`, `src/btree/`, `src/backup/`, `src/evict/`, `src/lsm/`, `src/conn/`, `src/config/`, `src/checksum/`, lock primitives

---

### `cppsuite/` — C++ Configurable Stress Framework
**Language/Framework:** C++ (custom framework with JSON-like config files)  
**Scale:** ~18 test executables  
**Trigger:** Evergreen (long-running stress tasks)  
**What it tests:**
- Multi-threaded, highly configurable workloads (insert/update/remove/read/custom threads)
- Built-in operation tracker validates DB state at end of run
- TimestampManager stresses oldest/stable timestamp advancement
- MetricsMonitor asserts statistics stay within configured bounds

**General storage tests:**
- `bounded_cursor_*` — prefix/bound cursor performance and correctness under concurrency
- `hs_cleanup` — history store garbage collection under load
- `operations_test` — general CRUD with timestamps
- `background_compact` — compaction while performing CRUD
- `cache_resize` — dynamic cache size changes under load
- `burst_inserts` — write amplification / insert burst handling
- `reverse_split` — reverse-order split stress
- `test_live_restore` — live restore under concurrent writes
- Benchmarks: `api_instruction_count_benchmarks`, `api_timing_benchmarks`, `bounded_cursor_perf`

**Disaggregated storage tests (1 test):**
- `test_disagg_failover_perf` — measures how long disagg failover takes (picking up the latest checkpoint from the page log) on a running system with concurrent CRUD

> **Tiered coverage:** No cppsuite tests target tiered storage directly.

**Code areas covered (roughly):**  
`src/cursor/`, `src/btree/`, `src/evict/`, `src/history/`, `src/txn/`, `src/block/`, `src/conn/`;  
disagg: `src/block_disagg/`, `src/conn/conn_layered*.c`

---

### `catch2/` — Internal Unit Tests (below API)
**Language/Framework:** C++ / Catch2 framework  
**Trigger:** `make check` (built with `-DHAVE_UNITTEST=1`), runs `./test/catch2/catch2-unittests`  
**What it tests:**  
WiredTiger *internals*, bypassing the public API. Organised by WT module.

**General internal tests:**
- `block/` — block manager extent list, block-level operations
- `cursors/` — cursor internal structures and contracts
- `live_restore/` — live restore internal state and recovery logic
- `sub_level_error/` — error propagation / error code semantics

**Disaggregated storage unit tests (in `misc_tests/` and `ext/`):**
- `test_page_log_handle.cpp` — unit tests for `WT_PAGE_LOG_HANDLE` mock interface (the `block_disagg` page-log backend)
- `test_disagg_meta_config.cpp` — unit tests for disagg metadata config format (checkpoint/timestamp fields)
- `test_layered_incomplete_table.cpp` — Catch2 equivalent of `test_layered90.py`; tests all 8 combinations of removing metadata entries from an otherwise-complete layered table (layered tables are the key disagg construct, hence this lives in disagg coverage)
- `ext/test_checkpoint_meta_version.cpp` — checkpoint metadata version parsing (used by disagg)
- `ext/test_key_provider_header.cpp` — key provider header for disagg encryption

> **Tiered coverage:** No catch2 tests target tiered storage internals.

**Code areas covered (roughly):**  
`src/block/`, `src/cursor/`, `src/log/`, internal WT data structures (extent lists, skip lists), live-restore internal state machine;  
disagg: `src/block_disagg/` (page log handle), `src/conn/conn_layered*.c` (layered table metadata), `src/schema/` (layered table schema)

---

### `format/` — Comprehensive Randomised Stress Test
**Language/Framework:** C (single long-running binary)  
**Trigger:** Evergreen stress tasks, nightly, predictable-format runs  
**What it tests:**
- The most complete single-program stress test in WT
- Runs configurable random mixes of: insert, update, delete, read, truncate, bulk load, checkpoint, alter, compact, backup, import, salvage, verify, prepared transactions, timestamps, HS operations
- Supports all table types: row-store, column-store VLCS, column-store FLCS, LSM

**Configuration profiles:**
- `CONFIG.stress` — general heavy-stress run
- `CONFIG.coverage` — tuned for code coverage
- `CONFIG.msan`, `CONFIG.endian` — sanitiser / endianness runs
- `CONFIG.mirror` — two tables kept in sync for cross-validation
- `CONFIG.replay` — reproduce a previously recorded operation sequence
- `CONFIG.disagg` — **disaggregated storage** run: sets `disagg.enabled=1`, `disagg.layered=1`, `runs.source=layered`, uses `palite` page log. This config tests the full disagg+layered stack (not tiered).

> **Important:** `CONFIG.disagg` exercises the layered table + disagg block manager path. There is **no** `CONFIG.tiered` — tiered storage is not tested by `format`.

**Code areas covered (roughly):**  
Almost the entire `src/` tree — this is the broadest coverage test. Especially: `src/btree/`, `src/txn/`, `src/history/`, `src/log/`, `src/block/`, `src/evict/`, `src/reconcile/`, `src/backup/`, `src/schema/`, `src/lsm/`;  
disagg profile also covers: `src/block_disagg/`, `src/conn/conn_layered*.c`

---

### `model/` — Formal Verification / Reference Model
**Language/Framework:** C++ (standalone model + WT comparison harness)  
**Trigger:** Evergreen (`run_model_workloads.sh`)  
**What it tests:**
- A reference implementation of WiredTiger's KV semantics (MVCC, timestamps, RTS, checkpoints)
- Tests compare WiredTiger's actual output to the model's expected output
- Sub-test suites:
  - `model_basic` — basic CRUD semantics
  - `model_checkpoint` — checkpoint visibility rules
  - `model_rts` — rollback-to-stable correctness
  - `model_transaction` — snapshot isolation, read/write timestamps
  - `model_workload` — full workload comparison
- Uses a debug log parser (`src/driver/debug_log_parser`) to replay WT operations into the model
- Workload files in `workloads/` define specific operation sequences

> **Disagg / Tiered coverage:** The model tracks MVCC and checkpoint semantics but does not specifically exercise layered, disagg, or tiered code paths.

**Code areas covered (roughly):**  
Validates: `src/txn/` (MVCC, timestamps), `src/history/` (HS eviction and lookup), `src/log/` (debug log format), `src/btree/` (checkpoint visibility), rollback-to-stable logic

---

### `checkpoint/` — Concurrent Checkpoint Test (C)
**Language/Framework:** C  
**What it tests:**
- Multiple reader/writer threads running concurrent with background checkpointing
- Verifies checkpoint consistency and crash-recovery correctness
- Tests row-store and column-store table types

> **Disagg / Tiered coverage:** Standard btree storage only; no disagg or tiered paths.

**Code areas covered (roughly):**  
`src/txn/`, `src/btree/`, `src/checkpoint/`, `src/reconcile/`

---

### `thread/` — Concurrent Read/Write Thread Test (C)
**Language/Framework:** C  
**What it tests:**
- Multiple reader and writer threads on row-store, variable-length column-store, and LSM tables
- Tests for data races and incorrect reads under concurrent modification

> **Disagg / Tiered coverage:** None.

**Code areas covered (roughly):**  
`src/btree/`, `src/cursor/`, `src/lsm/`, concurrency primitives

---

### `fops/` — Schema / File Operations Concurrency (C)
**Language/Framework:** C  
**What it tests:**
- Concurrent schema operations (create, drop, rename tables) interleaved with CRUD
- Validates that the schema lock and handle lifecycle are race-free

> **Disagg / Tiered coverage:** None.

**Code areas covered (roughly):**  
`src/schema/`, `src/conn/` (dhandle management), `src/meta/`

---

### `cursor_order/` — Cursor Ordering Correctness (C)
**Language/Framework:** C  
**What it tests:**
- Verifies that cursors return keys in the correct sorted order under concurrent inserts/deletes
- Tests both forward and reverse iteration

> **Disagg / Tiered coverage:** None.

**Code areas covered (roughly):**  
`src/cursor/`, `src/btree/` (page splits, in-memory structures)

---

### `packing/` — Integer Packing Format (C)
**Language/Framework:** C  
**What it tests:**
- WiredTiger's variable-length integer packing/unpacking routines
- Validates correctness of encoded values across the full integer range (incl. negative, boundary values)

**Code areas covered (roughly):**  
`src/packing/` (`wt_pack_int`, `wt_unpack_int`, format strings)

---

### `huge/` — Large Value / Eviction Boundary (C)
**Language/Framework:** C  
**What it tests:**
- Inserts very large single updates (~1 MB values)
- Stresses eviction threshold logic when a single page can breach cache limits

**Code areas covered (roughly):**  
`src/evict/`, `src/btree/` (overflow items), `src/block/`

---

### `manydbs/` — Multiple Database Connections (C)
**Language/Framework:** C  
**What it tests:**
- Opening and managing many simultaneous WiredTiger connections/databases
- Validates resource management (file handles, memory) at scale

**Code areas covered (roughly):**  
`src/conn/`, OS-level resource management

---

### `salvage/` — Corruption Recovery (C)
**Language/Framework:** C  
**What it tests:**
- Deliberately corrupts WiredTiger data files, then invokes the salvage API
- Verifies that the engine recovers as much data as possible without crashing

**Code areas covered (roughly):**  
`src/block/` (salvage path), `src/btree/`, `src/meta/`

---

### `readonly/` — Read-Only Connection Mode (C)
**Language/Framework:** C  
**What it tests:**
- Opens a WiredTiger database in `readonly=true` mode
- Verifies that write operations are rejected and reads succeed
- Tests transitions between read-write and read-only connections

**Code areas covered (roughly):**  
`src/conn/`, `src/session/`, API enforcement layer

---

### `fuzz/` — LibFuzzer Fuzz Tests
**Language/Framework:** C (LibFuzzer / AFL compatible)  
**Trigger:** Manual / continuous fuzz infrastructure  
**What it tests:**
- `config/` — fuzzes WiredTiger configuration string parsing
- `modify/` — fuzzes `WT_CURSOR.modify()` input (partial update format); has a seed corpus
- `fuzz_run.sh` and `fuzz_coverage.sh` drive the runs and coverage collection

> **Disagg / Tiered coverage:** None.

**Code areas covered (roughly):**  
`src/config/` (config parsing), `src/modify/` / `src/btree/` (partial update application)

---

### `syscall/` — System Call Trace Verification
**Language/Framework:** Python driver + strace  
**What it tests:**
- Runs WT programs under `strace` and compares actual system calls against a recorded "golden" `.run` file
- Validates that WT's I/O patterns (open/close/pread/pwrite/fsync calls) match expectations
- Useful for detecting accidental I/O regressions or missing fsyncs

> **Disagg / Tiered coverage:** Not observed; syscall patterns for page-log I/O are not recorded.

**Code areas covered (roughly):**  
`src/os_posix/` (OS abstraction layer), `src/block/` (I/O paths), log fsync paths

---

### `simulator/` — Timestamp Logic Simulator
**Language/Framework:** C++ (standalone)  
**What it tests:**
- Simulates WiredTiger's timestamp management algorithm (oldest/stable/durable timestamps)
- Uses a call log manager to record `set_timestamp` / `query_timestamp` call sequences
- Validates that the timestamp ordering invariants hold without running the full engine

> **Disagg / Tiered coverage:** None; pure timestamp algorithm, storage-agnostic.

**Code areas covered (roughly):**  
`src/txn/` timestamp management logic (isolated from storage layer)

---

### `compatibility/` — Cross-Release Forward/Backward Compatibility
**Language/Framework:** Bash + `format` binary  
**Trigger:** Evergreen scheduled runs  
**What it tests:**
- Runs `format` on one WiredTiger branch, copies the data directory, then opens it with a different branch
- Validates upgrade (older → newer) and downgrade (newer → older) data file compatibility
- Covers MongoDB 4.4 through develop
- Checks backup format compatibility (some branches create `BACKUP.copy`)

> **Disagg / Tiered coverage:** Runs `format` in its standard (non-disagg) configuration. Disagg compatibility across releases is not currently tested here.

**Code areas covered (roughly):**  
On-disk file format (`src/block/`, `src/meta/`, `src/btree/`), checkpoint and log format versioning

---

### `multiversion/` — Multi-Version Compatibility (Legacy)
**Language/Framework:** Bash  
**What it tests:**
- Earlier approach to cross-version testing (targets WT 4.2 / `mongodb-4.2`)
- Builds two versions and runs cross-version reads
- Largely superseded by `compatibility/`

---

### `live_restore/` — Live Restore Integration Tests
**Language/Framework:** Bash scripts + Python helper  
**What it tests:**
- The live restore feature: starting a WT database from a backup source and serving reads while restoring in the background
- `short_test.sh` — quick smoke (10 iterations, 1000 ops)
- `long_test.sh` — extended test (50 k ops, die/recovery cycles, background thread)
- Tests recovery from mid-restore crash (`-d` die flag + `-r` recovery flag)
- Tests per-directory database mode and background thread completion

> **Disagg / Tiered coverage:** Live restore is an independent feature (`src/live_restore/`); these scripts do not test disagg or tiered storage.

**Code areas covered (roughly):**  
`src/live_restore/`, `src/block/` (copy-on-read), `src/conn/` startup path

---

### `wtperf/` — Performance Test Config Helper
**Language/Framework:** Python utility  
**What it tests / does:**
- `test_conf_dump.py` — validates that wtperf configuration files can be parsed and dumped correctly
- Actual perf workloads live in `bench/wtperf/`; this folder holds only the config-validation test

---

### `evergreen/` — CI Support Scripts (Not Tests)
Scripts and helpers used by Evergreen CI — not tests themselves:
- `evg_cfg.py` — validates Evergreen config has entries for all csuite tests
- `code_coverage/` — coverage report generation
- `code_coverage_analysis.py` — analysis of coverage data
- `checkpoint_stress_test.sh`, `format_test_predictable.sh`, `run_model_workloads.sh` — test runner wrappers
- `perf_submission.sh` — uploads performance results to Atlas
- Note: the `evergreen_disagg.yml` file is a **separate Evergreen project** (`wiredtiger-disagg`) that runs disagg-specific test variants using `hook_disagg.py` and `CONFIG.disagg`

---

### `3rdparty/` — Vendored Test Dependencies
Python test support libraries (discover, testscenarios, etc.) and `nlohmann` JSON for cppsuite.

### `py_install/` / `py_utility/` — Shared Python Test Utilities
Base classes, random utilities, scenario generation helpers used by `suite/` and other Python tests.

### `wt_hang_analyzer/` — Hang Detection Tool
Python script that detects and reports WiredTiger process hangs (stack traces, diagnostics).

### `windows/` — Windows Compatibility Shims
Windows-specific shims used by tests to fill in POSIX APIs not available on Windows.

---

## Summary Table

| Directory | Language | Type | CI Trigger | Tiered | Disagg (incl. layered tables) | Rough Scope |
|---|---|---|---|---|---|---|
| `suite/` | Python | API integration (748 tests) | Evergreen batches | 20 tests (`test_tiered*`) + `hook_tiered.py` | 110+ tests (`test_disagg*` + `test_layered*` + cross-cutting) + `hook_disagg.py` | Full API surface |
| `csuite/` | C | Sanity / crash-recovery | `make check` + Evergreen | None | None | Crash paths, recovery, bug regressions |
| `cppsuite/` | C++ | Multi-threaded stress | Evergreen long-running | None | 1 test (`test_disagg_failover_perf`) | CRUD under load, HS, compaction |
| `catch2/` | C++ | Internal unit tests | `make check` | None | 3 disagg/layered unit tests | Internals below API |
| `format/` | C | Randomised comprehensive stress | Evergreen nightly | None | `CONFIG.disagg` profile | Almost entire `src/` |
| `model/` | C++ | Formal verification | Evergreen | None | None | MVCC / timestamp / RTS semantics |
| `checkpoint/` | C | Concurrent checkpoint | `make check` | None | None | Checkpoint + recovery |
| `thread/` | C | Concurrent R/W | `make check` | None | None | Btree concurrency |
| `fops/` | C | Schema concurrency | `make check` | None | None | Schema / dhandle lifecycle |
| `cursor_order/` | C | Cursor ordering | `make check` | None | None | Cursor iteration |
| `packing/` | C | Format unit | `make check` | None | None | Integer packing |
| `huge/` | C | Large value | `make check` | None | None | Eviction / overflow |
| `manydbs/` | C | Resource mgmt | `make check` | None | None | Connection pool |
| `salvage/` | C | Corruption recovery | `make check` | None | None | Salvage path |
| `readonly/` | C | Mode enforcement | `make check` | None | None | Read-only connections |
| `fuzz/` | C | Fuzzing | Manual / fuzz infra | None | None | Config parsing, modify |
| `syscall/` | Python+strace | I/O pattern | Manual / Evergreen | None | None | OS I/O paths |
| `simulator/` | C++ | Algorithm unit | Evergreen | None | None | Timestamp logic |
| `compatibility/` | Bash | Cross-release | Evergreen scheduled | None | None (non-disagg format runs only) | On-disk format compat |
| `multiversion/` | Bash | Cross-version (legacy) | Manual | None | None | Legacy compat |
| `live_restore/` | Bash | Integration | Evergreen | None | None | Live restore feature |
| `wtperf/` | Python | Config validation | `make check` | None | None | wtperf config parsing |
