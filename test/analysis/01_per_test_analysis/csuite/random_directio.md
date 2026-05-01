# random_directio — Direct-I/O crash simulation with schema-operation recovery

**Path:** `test/csuite/random_directio/`
**Language:** C
**Storage mode:** General (Linux only; requires O_DIRECT support)
**Jira ticket:** N/A
**Components under test:** Log-based recovery, direct I/O copy, schema operations (create/drop/insert/update), row-store and reverse-table consistency, tiered storage

## What This Test Does
This test simulates a system crash by forking a child writer process and, on a configurable cycle, sending SIGSTOP, copying the entire database directory using direct I/O (bypassing the filesystem buffer cache), then running recovery on the copy and verifying data consistency. It repeats this suspend-copy-verify cycle N times (default 5) before killing the child. Because the copy is done with O_DIRECT it sees only data that has actually reached the block device, closely approximating a real power-failure snapshot.

## Test Scenarios / Cases

### Scenario: Basic row-store recovery (no schema ops)
- **What it tests:** That after a direct-I/O snapshot and recovery, the main table and its reverse table contain a consistent, contiguous set of records (keys and values in both tables match and are properly paired up to the last observed complete thread write).
- **Components:** Log recovery, row-store, reverse table, direct I/O copy.
- **Notes:** Each key in the main table has a corresponding reversed-key in `table:rev`; the test verifies transactional atomicity.

### Scenario: Schema operations — create/insert/update/drop (`-S create,drop,...`)
- **What it tests:** That schema operations interleaved with main-table inserts either fully complete or fully roll back after recovery. The test searches the metadata cursor for any table that should not be present.
- **Components:** `session->create()`, `session->drop()`, metadata cursor scan, log recovery.
- **Notes:** Schema frequency (`-f`) controls how often sequences of create/insert/update/drop occur.

### Scenario: Integrated schema transactions (`-S integrated`)
- **What it tests:** That schema operations committed inside the same transaction as the main-table insert appear atomically after recovery. The reverse table key must not exist without the matching main-table key and associated schema tables.
- **Components:** Multi-table transactions, log recovery atomicity.
- **Notes:** Requires `-S create` to be set as well. Adds `create_check`, `data_check`, `drop_check` validations.

### Scenario: Checkpoint thread (`-C`)
- **What it tests:** That periodic checkpoints running concurrently with the writer do not corrupt recovery or cause stale data to appear.
- **Components:** Checkpoint thread, log recovery.
- **Notes:** Checkpoint interval is randomized (0–6 s).

### Scenario: Tiered storage (`-B -C`)
- **What it tests:** Schema operation + checkpoint recovery when tiered storage is enabled (`dir_store` extension). Requires checkpoint mode.
- **Components:** Tiered storage, flush_tier, log recovery.
- **Notes:** `flush_tier` is called at random intervals alongside checkpoints.

## LazyFS Variant
None. This test uses direct I/O as its own crash simulation mechanism.
