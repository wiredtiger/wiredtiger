# format — Comprehensive Randomised Stress Test

## Overview

`format` is WiredTiger's primary randomised stress-test harness. Each invocation reads one or more
configuration files (CONFIG.*) and uses them to drive a fully randomised multi-threaded database
workload. Configuration controls everything from table type and row counts to which optional
subsystems (backup, compaction, encryption, tiered/disaggregated storage, etc.) are active.

The high-level lifecycle is:

1. Parse config, resolve random seeds, optionally fork follower processes (disagg multi-node).
2. Open (or reopen) the database, bulk-load initial rows, run WiredTiger `verify`.
3. Run `FORMAT_OPERATION_REPS` (3) rounds of concurrent worker threads plus optional auxiliary
   threads.
4. After each round, run rollback-to-stable, then at the end: verify, salvage, statistics.

All decisions that are not fixed by the config file are made randomly, so each run explores a
different region of the state space.

---

## Configuration Profiles

### CONFIG.stress
- **Purpose:** General high-concurrency stress testing; the "standard" CI configuration.
- **Key settings:**
  - `cache.minimum=20` — keeps the cache small to force eviction pressure.
  - `runs.rows=1000000:5000000` — random row count between 1 M and 5 M.
  - `runs.tables=3:10` — 3–10 tables per run.
  - `runs.threads=4:32` — 4–32 concurrent worker threads.
  - `runs.timer=6:30` — run wall-clock duration 6–30 minutes.
- **Components exercised:** Eviction under pressure, multi-table concurrency, all CRUD operations,
  optional checkpointing, backup, timestamps, RTS, salvage, etc. Everything is randomised from the
  global defaults unless overridden.

### CONFIG.disagg
- **Purpose:** Disaggregated / layered storage testing (the "palite" page-log back-end).
- **Key settings:**
  - `disagg.page_log=palite` — uses the lightweight in-process page-log service.
  - `disagg.enabled=1`, `disagg.layered=1` — enables disaggregated mode with layered tables.
  - `runs.source=layered` — all tables use the layered storage source.
  - `runs.type=row-store` — row-store only (column-store not yet supported).
  - `runs.tables=3` — fixed at 3 tables.
  - `checkpoint=on`, `precise_checkpoint=1` — checkpoints are always on.
  - `transaction.timestamps=1` — timestamps are always enabled.
  - Disables: `backup`, `background_compact`, `block_cache`, `btree.reverse`, `import`,
    `ops.alter`, `ops.pct.modify` (FIXME-WT-16479), `ops.compaction`, `ops.salvage`,
    `ops.throttle`, `tiered_storage.*`.
- **Components exercised:** Disaggregated leader/follower topology, page-log interactions,
  checkpoint with precise semantics, timestamped transactions, RTS, multi-table row-store reads
  and writes.

### CONFIG.coverage
- **Purpose:** Code coverage runs — slightly longer timer, mmap enabled, no throttle.
- **Key settings:**
  - `cache.minimum=20`, `mmap=1` — enables memory-mapped I/O path.
  - `leak_memory=0` — turn off intentional memory leak.
  - `ops.throttle=0` — no artificial rate limiting.
  - `runs.rows=1000000:5000000`, `runs.threads=4:32`, `runs.timer=15` (minutes).
  - `checkpoints=1` — enables checkpointing.
- **Components exercised:** Same breadth as CONFIG.stress but with memory-mapped file paths
  exercised and a fixed longer timer for better line coverage.

### CONFIG.mirror
- **Purpose:** Table-mirroring stress test. Two or more tables are kept in sync and compared after
  every mutating operation to detect divergence.
- **Key settings:**
  - `runs.mirror=1` — activates mirroring.
  - `cache.minimum=20`, `runs.rows=1000000:5000000`, `runs.tables=3:10`,
    `runs.threads=4:32`, `runs.timer=6:30` — same ranges as CONFIG.stress.
- **Components exercised:** Cross-table consistency (row-store vs column-store mirroring),
  mirrored truncate verification, all standard CRUD operations with cross-table invariant
  checking after every commit/rollback.

### CONFIG.msan
- **Purpose:** MemorySanitizer (MSan) build. Identical to CONFIG.stress except encryption is
  disabled because the `rotn`/`sodium` extensions are not instrumented.
- **Key settings:**
  - `disk.encryption=none`
  - All other settings same as CONFIG.stress.
- **Components exercised:** Same as CONFIG.stress but without encryption, allowing MSan to run
  without false positives from uninstrumented extension code.

### CONFIG.endian
- **Purpose:** Endianness testing; minimal configuration used to test byte-order correctness,
  notably when archiving or reading logs.
- **Key settings:**
  - `cache.minimum=20`, `format.abort=0`, `logging.archive=0`, `logging=1`.
  - `ops.throttle=0`, `block_cache=0`.
  - `runs.timer=4`, `runs.rows=1000000`.
- **Components exercised:** Write-ahead logging paths, on-disk format, block manager, log
  file encoding/decoding under potentially different byte-order assumptions.

### CONFIG.replay
- **Purpose:** Predictable (deterministic) replay. Two runs with the same seeds produce
  byte-identical on-disk state up to any given timestamp.
- **Key settings:**
  - `runs.predictable_replay=1`, `format.independent_thread_rng=1`.
  - `transaction.timestamps=1`, `transaction.implicit=0` — all transactions must be timestamped.
  - `runs.rows=100000:500000`, `runs.tables=3:10`, `runs.threads=4:32`, `runs.timer=30`.
  - Disables: `backup`, `import`, `ops.alter`, `ops.compaction`, `ops.salvage`, `ops.throttle`,
    `ops.truncate`, `runs.mirror`, `format.abort`, `runs.in_memory`.
- **Components exercised:** Timestamp determinism, lane-based key assignment (1024 lanes),
  per-timestamp RNG seeding, rollback and retry without changing committed state, RTS at the
  end of each round.

---

## Operations Covered

### Core Worker Operations (`ops.c`)
Each worker thread (`ops()`) loops, randomly choosing an operation per the configured percentages
(`ops.pct.*`). Supported operations:

| Operation | Description |
|-----------|-------------|
| **INSERT** | Row-store: generate unique key and insert. Column-store: append record. |
| **UPDATE** | Overwrite an existing row's value. |
| **MODIFY** | Apply a random change vector (up to 5 modify entries) to an existing row. Verifies the modify return by re-reading and comparing. |
| **REMOVE** | Delete a row (existence-checked on row-store). |
| **TRUNCATE** | Delete a random key range (up to 2% of the table). Both cursor-to-cursor and one-sided (start/end) truncates are exercised. |
| **READ** | Point read by key; 25% of the time uses `search_near` instead of `search`. |
| **NEXT/PREV** | After any positioned cursor operation, the thread walks forward or backward a random number of steps (1–100). |
| **RESERVE** | Snapshot-isolation only: reserve a row before writing, to test the reservation path. |
| **BOUND_CURSOR** | Randomly sets lower/upper bounds on a cursor before reads (exercising the bounded-cursor API). |

Transactions are started explicitly at snapshot isolation in timestamp mode (75% of reads carry a
read timestamp). 10% of transactions are prepared before commit. 10% of transactions are rolled
back rather than committed.

### Snapshot Repeat Verification (`snap.c`)
Within a snapshot-isolation transaction, every read, insert, modify, update, and remove is
remembered. Before committing, the reads are replayed and values compared to detect data
corruption or visibility bugs. After RTS, the stable set of saved operations is replayed again.

### Auxiliary Background Threads (`ops.c`)
Depending on config flags, additional threads run concurrently:

| Thread | Source | Purpose |
|--------|--------|---------|
| `alter` | `alter.c` | Periodically alters table configurations. |
| `background_compact` | `compact.c` | Periodically enables/disables background compaction server. |
| `backup` | `backup.c` | Runs full and incremental block-level backups; opens the backup and verifies it (supports live-restore mode). |
| `checkpoint` | `checkpoint.c` | Takes named and unnamed checkpoints with varying wait intervals and log-size triggers. Exercises named checkpoint rotation. |
| `compact` | `compact.c` | Runs foreground compaction against tables. |
| `follower` | `follower.c` | In multi-node disagg mode: the follower process polls for new checkpoints from the leader. |
| `hs_cursor` | `hs.c` | Walks the internal history-store cursor to exercise the HS ordering checker. |
| `import` | `import.c` | Periodically imports a separately-created table into the main connection. |
| `random_kv` | `kv.c` (random) | Issues random key/value operations for additional concurrency. |
| `timestamp` | `format_timestamp.c` | Advances oldest/stable timestamps; triggers RTS at the end of each round. |

### Additional Feature Paths
- **Bulk load** (`wts.c`): Initial data is loaded using `WT_CURSOR.bulk` for efficiency.
- **Verify** (`verify.c`): `session->verify(strict)` after load and after each round of operations;
  mirrors are verified by scanning all mirrored tables in sync.
- **Salvage** (`format_salvage.c`): Runs `session->salvage` at end-of-run.
- **Disaggregated storage** (`format_disagg.c`): Leader/follower process management via fork + Unix
  socket synchronisation; role-switch mode alternates leader/follower every round.
- **Predictable replay** (`replay.c`): Full machinery to make every timestamp's committed changes
  deterministic given fixed RNG seeds; lane-based coordination prevents concurrent key conflicts.
- **Prepare/discover** (`format_prepare_discover.c`): After reopen, scans for any surviving
  prepared transactions.
- **Encryption** (`format.h`): `rotn` and `sodium` encryptors are plugged in randomly.
- **Compression** (`format.h`): `lz4`, `snappy`, `zlib`, `zstd` compressors are selected randomly.
- **Checksums**: Per-table checksum configuration.
- **Tracing** (`trace.c`): Optionally records per-operation trace records to a separate WiredTiger
  log for post-mortem debugging.

---

## Key Observations

### Coverage Strengths
- Exercises virtually every WiredTiger user-visible API surface: CRUD, transactions
  (prepared/unprepared, timestamped/non-timestamped), checkpoints (named, unnamed, automatic),
  backup (full, incremental, live-restore), salvage, compaction, alter, import, RTS, eviction.
- Multi-table scenarios, including mirroring across row-store and column-store, provide
  cross-table consistency guarantees beyond what single-table tests can offer.
- The predictable-replay mode enables differential debugging: the same seeds run twice should
  produce identical state, making it possible to bisect state divergence across commits.
- The disagg profile covers the new disaggregated storage layer including the palite page-log
  back-end, layered cursors, and leader/follower topology.
- The stress/msan/endian/coverage CONFIG variants ensure the same codebase is exercised under
  different sanitizer, endianness, and code-coverage build modes.

### Coverage Gaps / Known Limitations
- Column-store is excluded from disagg testing (FIXME-WT-14738 — reverse collator unavailable).
- `ops.pct.modify=0` is forced in the disagg profile (FIXME-WT-16479).
- Predictable replay cannot test column-store inserts (unpredictable key allocation), truncate,
  salvage, backup, import, alter, or compaction.
- In-memory databases (`runs.in_memory`) are only a minor variant; the bulk of coverage is
  on-disk.
- The history-store cursor thread only exercises the HS scan path; direct HS correctness
  verification relies on the broader snapshot-repeat and verify machinery.
- Encryption and compression are selected randomly and may not be exercised in every run; there
  are no dedicated CI profiles that guarantee encryption or each compressor is always active.
