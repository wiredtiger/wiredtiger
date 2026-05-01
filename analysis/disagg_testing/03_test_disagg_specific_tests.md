# Disagg CI Testing — Disagg-Specific Unit Tests

> Category: Python tests in test/suite/ with "disagg" in their filename (not test_layered*)

---

## Overview

These are targeted tests for specific disagg subsystems: the disagg storage API itself, checkpoint size tracking, verify in disagg mode, leaf page delta construction, and key provider integration. Unlike `test_layered*.py` (which test the full layered table user experience), these test internal disagg mechanisms.

---

## File Inventory

### test_disagg01–04: Core Disagg Storage API

| File | Description | Class | Test Methods |
|---|---|---|---|
| test_disagg01.py | Direct test of disagg internal APIs (not public). Tests cold write and cold read paths via the page log extension. | `test_disagg01` | `test_disagg_basic`, `test_cold_write`, `test_cold_read` |
| test_disagg02.py | Compact operation fails in disagg storage mode | `test_disagg02` | `test_disagg_compact` |
| test_disagg03.py | Tiered table creation fails when disagg storage is enabled | `test_disagg03` | `test_disagg_tiered_disabled`, `test_disagg_tiered_create_disabled` |
| test_disagg04.py | Direct test of disagg internal storage tier APIs | `test_disagg04` | `test_disagg_storage_tier` |

### test_disagg_checkpoint_size: Checkpoint Size Accounting

These test that checkpoint metadata correctly tracks stable table sizes, database-level sizes, and detects leaks.

| File | Description | Class | Test Methods |
|---|---|---|---|
| test_disagg_checkpoint_size01.py | Checkpoint size field stored in metadata for stable tables | `test_disagg_checkpoint_size` | `test_checkpoint_size_populated_non_compressed`, `test_checkpoint_size_populated_compressed`, `test_checkpoint_size_increases`, `test_checkpoint_size_persists_across_restart` |
| test_disagg_checkpoint_size02.py | Database-level size in checkpoint completion record | `test_disagg_checkpoint_size02` | `test_new_database`, `test_database_size_increases`, `test_database_size_decreases`, `test_database_size_multiple_btrees` |
| test_disagg_checkpoint_size03.py | Checkpoint size doesn't grow due to `bytes_total` leak | `test_disagg_checkpoint_size03` | `test_bytes_total_leak`, `test_bytes_total_leak_delta`, `test_bytes_total_leak_delta_normal_ops`, `test_size_leak_after_rec_result_page_clean`, `test_cumulative_size_leak_after_eviction` |
| test_disagg_checkpoint_size04.py | Dropping a table reduces database size | `test_disagg_checkpoint_size04` | `test_drop_reduces_database_size`, `test_drop_one_of_multiple_tables` |

### test_verify_disagg: Verify in Disagg Mode

| File | Description | Class | Test Methods |
|---|---|---|---|
| test_verify_disagg.py | `session.verify()` testing for disagg storage: verify disagg, leader with no table, follower with no metadata, follower with no checkpoint | `test_verify_disagg` | `test_verify_disagg`, `test_verify_leader_no_table`, `test_verify_follower_no_metadata`, `test_verify_follower_no_checkpoint` |
| test_verify_disagg02.py | Duplicate btree IDs among stable files are detected | `test_verify_disagg02` | `test_verify_duplicate_btree_ids` |

### test_leaf_delta_disagg: Leaf Page Delta Construction

| File | Description | Class | Test Methods |
|---|---|---|---|
| test_leaf_delta_disagg01.py | Building leaf delta disk images from base + deltas correctly; tests various key operations | `test_leaf_delta_disagg01` | `test_delta_no_duplicate_keys`, `test_delta_duplicate_keys`, `test_delta_inserted_keys`, `test_base_empty_values_all`, `test_base_empty_values_mixed`, `test_comprehensive`, `test_delete` |

### test_key_provider_disagg: Encryption Key Provider

| File | Description | Class | Test Methods |
|---|---|---|---|
| test_key_provider_disagg01.py | Basic key provider scenarios: initial key fetch, key with expiration | `test_key_provider_disagg01` | `test_key_provider_disagg01` |
| test_key_provider_disagg02.py | Crash during checkpoint doesn't corrupt key provider metadata | `test_key_provider_disagg02` | `test_key_provider_crash_recovery` |

---

## How They Run in CI

These tests run as part of the general Python test suite (`unit-test` tasks). They are also run under the disagg hook, but since they already use disagg storage directly, the hook interaction is mostly a no-op for them.

**Key coverage entry**: `test_key_provider_disagg01.py` and `test_key_provider_disagg02.py` are **explicitly included** in `code_coverage_config.json`:
```
"python3 ../test/suite/run.py test_key_provider_disagg01.py",
"python3 ../test/suite/run.py test_key_provider_disagg02.py",
```
This makes them the **only disagg-specific Python tests with code coverage measurement**.

**Coverage gap**: `test_disagg01-04`, `test_disagg_checkpoint_size01-04`, `test_verify_disagg`, `test_verify_disagg02`, and `test_leaf_delta_disagg01` are **not in the code coverage config**.

---

## Infrastructure Files

| File | Purpose |
|---|---|
| `test/suite/hook_disagg.py` | The hook that intercepts WT API calls to enable disagg mode transparently |
| `test/suite/hook_disagg.fail` | List of tests excluded from hook runs (46 files) |
| `test/suite/helper_disagg.py` | Shared utilities: `DisaggConfigMixin`, `gen_disagg_storages()`, `Oplog` class, role-switching helpers |
