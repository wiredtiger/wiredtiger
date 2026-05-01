# model_workload — Workload Executor, Generator, and Regression Workloads

## Overview

`model_workload` tests the model's workload execution framework: a declarative, serialisable
representation of a WiredTiger operation sequence that can be run against both the model and a
live WiredTiger instance for comparison. It covers:

- The `kv_workload` executor (programmatically assembled sequences).
- The `kv_workload_generator` (random workload synthesis).
- The workload parser (round-trip serialisation/deserialisation of operation strings).
- Regression workloads: hand-crafted `*.workload` files in `test/model/workloads/` that
  reproduce previously-found bugs.

Source: `/data/work/git/wiredtiger4/test/model/test/model_workload/main.cpp`

---

## Test Cases / Scenarios

### test_workload_basic
- **What it verifies:** A manually assembled workload without timestamps:
  `create_table` → two concurrent transactions (insert key1/key2) → commit both → second
  transaction inserts key3/key4, removes key1 → commit → third transaction truncates [key4,key4]
  → commit. Verifies final state (key1=NONE, key2=value2, key3=value3, key4=NONE) in the model
  then runs `verify_workload` against WiredTiger and also checks debug-log replay.
- **Model components:** `kv_workload`, `kv_workload::run`, model verifier, `debug_log_parser`.
- **Notes:** No timestamps; exercises insert, remove, truncate, commit.

### test_workload_txn
- **What it verifies:** A workload with timestamped transactions and RTS:
  insert key1/key2 at ts=10/20 → insert key3/key4 at ts=30, remove key1 → rollback (key3 and
  key4 are dropped, key1 survives) → a new transaction sets per-key commit timestamps (key1→35,
  key4→40) → set_stable_timestamp(35) → rollback_to_stable. Verifies that after RTS only data at
  or below ts=35 survives (key1=value5, key2=value2, key3=value3 at stable, key4=value4 at ts=30
  survives because its commit_ts=40 > stable=35 is rolled back). WiredTiger and debug-log
  verified.
- **Model components:** `kv_workload` with timestamps, `set_commit_timestamp`, `rollback_to_stable`.
- **Notes:** Exercises per-operation timestamp assignment within a transaction.

### test_workload_prepared
- **What it verifies:** A workload with prepared transactions and RTS:
  Two transactions insert key1/key2, each prepared at ts=10/15 and committed at ts=20/25 with
  different durable timestamps. `set_stable_timestamp(24)` → `rollback_to_stable`. Because
  txn2's durable_ts=26 > stable=24, key2 is rolled back. Final state: key1=value1, key2=NONE,
  key3=NONE. WiredTiger and debug-log verified.
- **Model components:** `kv_workload` with `prepare_transaction`, `commit_transaction` (3-arg
  form for durable timestamp), `rollback_to_stable`.

### test_workload_restart
- **What it verifies:** A workload that includes a `restart` operation:
  Two prepared transactions commit; `set_stable_timestamp(22)` → a transaction removes key1 →
  checkpoint → `restart` → a new prepared transaction inserts key3 → `set_stable_timestamp(25)`.
  After restart, key1 is visible (stable=22 was before the remove; RTS at open restores key1),
  key2=NONE (durable_ts=26 > stable=22), key3=value3 (committed after restart).
  WiredTiger and debug-log verified.
- **Model components:** `kv_workload` with `checkpoint`, `restart`.

### test_workload_crash
- **What it verifies:** Same workload as `test_workload_restart` but using `crash` (unclean
  shutdown) and `checkpoint_crash` (a checkpoint that is in-progress during the crash):
  After restart from crash, WiredTiger uses only the last durable checkpoint; state should match
  the model's post-crash view. WiredTiger and debug-log verified.
- **Model components:** `kv_workload` with `crash`, `checkpoint_crash` (ID-based crash point).

### test_workload_generator
- **What it verifies:** The `kv_workload_generator` produces a valid random workload that,
  when executed in both the model and WiredTiger, produces the same result. The generator is
  retried up to 10 times if a `known_issue_exception` is raised. Disaggregated storage is
  disabled in the spec for this test (verification config differs for disagg).
- **Model components:** `kv_workload_generator::generate`, `verify_workload`.
- **Notes:** Exercises the full random workload generation and cross-validation pipeline.

### test_workload_parse
- **What it verifies:** Every operation type that the workload language supports can be serialised
  to a string and parsed back to an identical operation object. The test builds a workload
  containing every supported operation (`create_table`, `begin_transaction`, `insert`, `remove`,
  `prepare_transaction`, `commit_transaction`, `rollback_transaction`, `set_stable_timestamp`,
  `checkpoint`, `checkpoint_crash`, `crash`, `get`, `truncate`, `rollback_to_stable`, `restart`,
  `set_commit_timestamp`), converts each to string via `operator<<`, parses it, and asserts
  equality. Also tests whitespace tolerance, hex integer literals, quoted vs. unquoted strings,
  string escaping, adjacent-string concatenation, and optional argument handling for `checkpoint`,
  `commit_transaction`, and `checkpoint_crash`.
- **Model components:** `model::operation::parse`, all operation variant types.
- **Notes:** Uses `key_format=Q,value_format=Q` (unsigned integer types) because the parser
  currently supports only numeric keys/values.

---

## Regression Workload Files

Each `*.workload` file in `/data/work/git/wiredtiger4/test/model/workloads/` encodes the minimal
operation sequence that triggered a specific bug. They are replayed by the model_workload test
runner (`test_model.sh`).

| File | Jira ticket | Brief description |
|------|-------------|-------------------|
| `SLS-1601.workload` | SLS-1601 | Disaggregated storage (`config("database","disaggregated=true")`): concurrent inserts with prepared transactions. |
| `WT-12539.workload` | WT-12539 | Prepared transaction followed by a truncate covering the prepared key, then commit. |
| `WT-12709-1.workload` | WT-12709 | Multi-table scenario with small `leaf_page_max=4KB` to trigger page splits; multiple tables with concurrent transactions. |
| `WT-12709-2.workload` | WT-12709 | Second repro for the same ticket; column-store (`r`) with small page size. |
| `WT-12909.workload` | WT-12909 | `rollback_to_stable` after a `set_oldest_timestamp` + checkpoint sequence. |
| `WT-12939.workload` | WT-12939 | Column-store table with a prepared-then-rolled-back transaction followed by an evict; exercises the prepared-value eviction path. |
| `WT-12966.workload` | WT-12966 | Prepared transaction, evict before checkpoint, then crash; tests that evicted prepared values are recovered correctly. |
| `WT-13252.workload` | WT-13252 | Full table truncation followed by checkpoint and RTS — tests a specific interaction between truncation and stable timestamp. |
| `WT-13612.workload` | WT-13612 | Disaggregated storage: next_page_id initialisation bug after a checkpoint-then-restart cycle. |
| `WT-13618.workload` | WT-13618 | Disaggregated storage: multiple consecutive restarts with only `set_stable_timestamp` calls, then a `set_oldest_timestamp`. |
| `WT-14832.workload` | WT-14832 | Prepared transaction visible in a checkpoint then committed; on restart the checkpoint must not contain the prepared write (the scenario noted in `test_checkpoint` as hard to test with a live WT). |
| `WT-15041-1.workload` | WT-15041 | Disaggregated storage: a particular commit ordering that exposed a disagg page-log bug. |
| `WT-15041-2.workload` | WT-15041 | Second variant for the same disagg ticket. |
| `WT-15086.workload` | WT-15086 | Disaggregated storage: two restarts with stable-timestamp advances and oldest-timestamp; validates stable/oldest ordering across restarts. |
| `WT-15311.workload` | WT-15311 | Disaggregated storage: concurrent transactions committing around a stable-timestamp advance; tests disagg visibility. |
| `WT-15389.workload` | WT-15389 | Disaggregated storage: large number of inserts across multiple transactions and a commit/stable interaction. |
| `WT-16426.workload` | WT-16426 | Disaggregated storage: concurrent transactions with a particular prepare/commit/stable ordering that triggered a disagg bug. |
| `WT-16523.workload` | WT-16523 | Column-store (`r`) with a prepared transaction followed by an eviction and a rollback, exposing an eviction/prepare interaction. |
