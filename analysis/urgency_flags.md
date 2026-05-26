# WiredTiger Ticket Urgency Flags

Analysis of 574 open WiredTiger tickets with missing "Assigned Teams" fields.
Date: 2026-05-06

---

## HIGH URGENCY

Tickets with data loss/corruption risks, security issues, customer-impacting crashes/hangs, or references to production incidents.

---

### WT-10829 — Security: auth_token/AccountKey printed in logs on config parse error
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Persistence
- **Last Updated:** 2025-12-03
- **Reason:** SECURITY — Cloud storage credentials (Azure AccountKey, AWS auth_token) are printed to WiredTiger logs when there is a config parse error. Any user with log access can read these secrets. This is a credential leak vulnerability. Labels include `security`. **Note:** Tiered storage is now deprecated; urgency is reduced since new deployments will not use this feature, but the fix is still advisable before the tiered code is removed. Also listed in [wont_do_candidates.md](wont_do_candidates.md) as a low-priority candidate.

---

### WT-5832 — Detect potential corruption as part of recovery/rollback to stable
- **Priority:** P3 | **Status:** Open | **Type:** Improvement
- **Team:** Persistence
- **Last Updated:** (circa 2020, persistent open)
- **Reason:** DATA INTEGRITY — Corruption during recovery or RTS is not reliably detected before it causes silent data loss. This is a foundational correctness gap with direct customer impact.

---

### WT-6431 — RTS with corrupted files leads to unrecoverable state
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Persistence
- **Last Updated:** 2024-05-02
- **Reason:** DATA INTEGRITY / UNRECOVERABLE FAILURE — When RTS encounters a corrupted file it can leave the database in an unrecoverable state. The ticket remains open with recent activity. Crash/unrecoverable failure criteria met.

---

### WT-7969 — Recovery allocates excessive disk space, exceeding available space (customer reported)
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Persistence
- **Last Updated:** (circa 2021, open)
- **Reason:** CUSTOMER BUG / UNRECOVERABLE FAILURE — Recovery allocated 17 GB on a 15 GB disk, making the database unrecoverable. Customer-reported production incident. Direct MongoDB user impact.

---

### WT-8278 — Salvage leaves incorrect history store records causing undefined behavior
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Persistence
- **Last Updated:** 2025-03-18
- **Reason:** DATA INTEGRITY — Salvage (the corruption recovery path) can leave incorrect records in the history store, leading to undefined behavior on subsequent reads. Recently active. Dangerous interaction: corruption recovery itself introduces new corruption.

---

### WT-8881 — Commit durable_ts can be set earlier than a data read timestamp
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Last Updated:** (circa 2022, open)
- **Reason:** DATA INTEGRITY — A committed transaction's durable timestamp can be placed before data that was read, violating the MVCC consistency guarantee. Silent incorrect reads are possible.

---

### WT-9613 — alter() not transactional: server can fassert and crash at startup
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Foundations
- **Last Updated:** (circa 2022, open)
- **Reason:** CRASH / PRODUCTION INCIDENT RISK — The non-atomic nature of `alter()` means a crash mid-alter can leave metadata in an inconsistent state that causes a fatal assertion (`fassert`) and prevents MongoDB from restarting. MongoDB production crash scenario.

---

### WT-3965 — Make schema operations atomic
- **Priority:** P3 | **Status:** Open | **Type:** Improvement
- **Team:** Foundations
- **Last Updated:** 2025-11-06
- **Reason:** DATA INTEGRITY / CRASH SAFETY — Schema operations (create, drop, rename) are not atomic: a crash in the middle leaves orphaned files, missing metadata, or inconsistent state. Ongoing active interest (updated late 2025). This is the root cause of multiple other bugs including WT-9613.

---

### WT-6500 — History store tombstone txn id=0 use-after-free
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Last Updated:** (circa 2020–2021, open)
- **Reason:** MEMORY SAFETY — A use-after-free involving history store tombstones with transaction ID 0. Memory safety bugs can cause crashes or silent data corruption.

---

### WT-7688 — Corrupted file handling fragility
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Persistence
- **Last Updated:** (circa 2021, open)
- **Reason:** DATA INTEGRITY — The code path for handling corrupted files is fragile and can lead to unexpected behavior rather than safe failure. Risk of silent damage on corruption.

---

### WT-9066 — test/format uses all_durable to set stable_ts: dangerous, can move backwards
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Last Updated:** 2025-03-18
- **Reason:** DATA INTEGRITY IN TESTING — Using `all_durable` to set the stable timestamp in test/format is dangerous because `all_durable` can move backwards, causing RTS to roll back data that should not be rolled back. This masks or creates false negatives in stability testing. Recently active.

---

### WT-9784 — Cache stuck logic incorrectly aborts transactions
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Last Updated:** (circa 2022, open)
- **Reason:** INCORRECT BEHAVIOR — The "cache stuck" detection incorrectly aborts transactions that should not be aborted, potentially causing spurious failures in MongoDB workloads.

---

### WT-8165 — Timestamp assertions miss invalid timestamps in specific scenario
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Last Updated:** (circa 2022, open)
- **Reason:** DATA INTEGRITY — Invalid timestamps can be written without triggering the expected assertion, meaning data can be written with incorrect temporal ordering without any error or warning.

---

### WT-7157 — wt downgrade hangs (confirmed spinning)
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Last Updated:** (circa 2021, open)
- **Reason:** HANG — Confirmed spinning/hang during downgrade. Hang without recovery meets the crash/hang criteria.

---

### WT-8644 — Preload failures leak cache blocks
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Last Updated:** (circa 2022, open)
- **Reason:** RESOURCE LEAK — Preload failures leak cache blocks, contributing to cache pressure and potential eviction failures, which can escalate to cache stuck conditions and database hangs.

---

### WT-10824 — Checksum mismatch tooling for data integrity investigation
- **Priority:** P3 | **Status:** Open | **Type:** Improvement
- **Team:** Persistence
- **Last Updated:** 2025-11-04
- **Reason:** DATA INTEGRITY TOOLING — Improved tooling to investigate checksum mismatches in production. Recently active; checksum mismatches indicate on-disk corruption. Needed for production issue investigation.

---

### WT-6627 — Unexpected WriteConflictException with single client
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Last Updated:** 2025-10-14
- **Reason:** INCORRECT BEHAVIOR — A single client should never see a WriteConflictException with itself. This indicates a concurrency bug in the MVCC logic. Recently active (2025).

---

### WT-8808 — Data validation failure in test_timestamp_abort
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Transactions
- **Last Updated:** (circa 2022, open)
- **Reason:** DATA INTEGRITY — A data validation failure in the timestamp abort test indicates that data visible after a rollback-to-stable does not match what should be visible, pointing to a correctness bug in RTS.

---

### WT-11213 — wt dump/load causes "unexpected timestamp usage" and aborts WT library
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Foundations
- **Last Updated:** 2025-03-25
- **Reason:** CRASH — Running `wt load` after `wt dump` triggers "unexpected timestamp usage" and calls `abort()` on the WiredTiger library. Any tool that causes an abort in the library is a crash risk in production tooling. Recently active.

---

### WT-11244 — Uninitialized bytes in MSAN build during bulk loading
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Foundations
- **Last Updated:** 2025-03-25
- **Reason:** MEMORY SAFETY — MemorySanitizer reports use-of-uninitialized-value during bulk loading. Bulk loading writes data to disk; uninitialized bytes in the write path can produce corrupt data files. Recently active.

---

### WT-12010 — Testy detects corruption flag in log record during verify
- **Priority:** P4 | **Status:** Open | **Type:** Bug
- **Team:** Foundations
- **Last Updated:** 2025-05-06 (updated today)
- **Reason:** DATA INTEGRITY — A real corruption flag was found in a log record during a Testy run, causing `WT_TRY_SALVAGE: database corruption detected`. Although P4, the ticket was updated today, indicating active investigation of a real corruption event.

---

### WT-4158 — Crash/recovery inconsistency with insert+truncate
- **Priority:** P4 | **Status:** Open | **Type:** Bug
- **Team:** Persistence
- **Last Updated:** (old, open)
- **Reason:** DATA INTEGRITY — Describes an inconsistency between what is written and what is recovered after a crash when insert and truncate operations are interleaved. Silent post-crash data inconsistency.

---

### WT-7418 — test/format assert on imported table: WT_ROLLBACK
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Persistence
- **Last Updated:** (circa 2021, open)
- **Reason:** INCORRECT BEHAVIOR — Assertion failure (WT_ROLLBACK) on an imported table in test/format indicates the import feature has correctness issues that could affect production use.

---

---

## MEDIUM URGENCY

Tickets with recently renewed interest, crashes in specific scenarios, hangs, performance pathologies with production impact, or P3 bugs representing important missing behavior.

---

### WT-7976 — Build failure with stability label
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Foundations
- **Last Updated:** 2025-02-28
- **Reason:** CI STABILITY — Labeled `stability`, recently active. A persistent build failure in the stability category warrants attention.

---

### WT-11266 — format-predictable-test: directories have different files (7.0, develop)
- **Priority:** P3 | **Status:** Open | **Type:** Bug
- **Team:** Foundations
- **Last Updated:** 2024-11-03
- **Reason:** TEST CORRECTNESS — The predictable test exits with "different files to compare" instead of reporting the discrepancy and completing the comparison. This can hide real failures.

---

### WT-11378 — Eviction takes more than 4 minutes for pages with many small updates
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Transactions
- **Last Updated:** 2024-07-23
- **Reason:** PERFORMANCE / PRODUCTION IMPACT — Single eviction lasting 4+ minutes is a serious latency spike that would impact MongoDB production workloads. Related to cache management correctness.

---

### WT-11179 — test/format should restart multiple times to catch bugs like WT-10551
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Last Updated:** 2024-11-19
- **Reason:** TEST COVERAGE GAP — WT-10551 (backup not capturing all data) required multiple restarts to reproduce. Without multi-restart testing, similar backup correctness bugs go undetected.

---

### WT-11200 — Create session stash history buffer to track page freeing
- **Priority:** P3 | **Status:** Open | **Type:** Improvement
- **Team:** Transactions
- **Last Updated:** 2025-03-25
- **Reason:** DEBUGGABILITY FOR PRODUCTION BUGS — Directly follows WT-10789 (a production-impacting bug involving premature memory freeing). The history buffer is needed to diagnose recurrences. Recently active.

---

### WT-11968 — Investigate if PowerPC atomics provide sufficient memory barriers
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Last Updated:** 2025-03-25
- **Reason:** MEMORY SAFETY / CORRECTNESS ON PLATFORM — Insufficient atomic memory barriers on PowerPC would lead to data races and silent corruption on that platform. Recently active.

---

### WT-11446 — Incorrect encoding for variable-length negative int
- **Priority:** P3 | **Status:** Backlog | **Type:** Bug
- **Team:** Foundations
- **Last Updated:** 2023-08-16
- **Reason:** DATA CORRECTNESS — The negative integer multi-byte encoding in `intpack_inline.h` is inconsistent with the positive encoding, using slightly more space and potentially encoding/decoding incorrectly for certain ranges.

---

### WT-11383 — Mechanism to check variable names compared to correct macro names
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Last Updated:** 2023-07-25
- **Reason:** BUG PREVENTION — Stems from a specific bug where a transaction ID was compared to a timestamp. Without a check, similar type-confusion bugs can re-enter the codebase silently.

---

### WT-9460 / WT-9461 / WT-9464 / WT-9531 / WT-9532 / WT-9574 — Documentation update sub-tasks (SPM-2942–SPM-2975)
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Mixed — see below (Step 7 reassignment)
- **Last Updated:** 2022
- **Reason:** DOCUMENTATION GAP — These tickets represent documentation work for specific features. If the features shipped but documentation was never updated, users and operators may be working from incorrect or missing documentation. Team assignment updated in Step 7 based on SPM ownership: WT-9460/9461 → Foundations (SPM-2942/2943, API/session work); WT-9464/9531 → Foundations (SPM-2944/2960, both Canceled parent projects); WT-9532 → Transactions (SPM-2961, shard merge timestamps, Canceled); WT-9574 → Transactions (SPM-2975, cache observability). Three of these (WT-9464/9531/9532) have Canceled parent SPM projects and are strong Won't Do candidates.

---

### WT-5396 — Review how WiredTiger uses WT_PUBLISH and WT_ORDERED_WRITE
- **Priority:** P3 | **Status:** Open | **Type:** Task
- **Team:** Foundations
- **Last Updated:** (old, open)
- **Reason:** MEMORY ORDERING / CORRECTNESS — WT_PUBLISH and WT_ORDERED_WRITE are memory ordering macros. An incorrect or unnecessary use of these can introduce subtle data races or false assumptions about ordering guarantees.

---

### WT-11293 — Investigate whether a read barrier is needed in hazard.c
- **Priority:** P3 | **Status:** Backlog | **Type:** Task
- **Team:** Foundations
- **Last Updated:** 2024-03-21
- **Reason:** MEMORY SAFETY — Hazard pointers are a critical memory reclamation safety mechanism. An incorrect or missing memory barrier in hazard pointer acquisition can allow use-after-free.

---

### WT-12067 — Improve/Fix CRC calculation and testing on zSeries
- **Priority:** P3 | **Status:** Backlog | **Type:** Improvement
- **Team:** Foundations
- **Last Updated:** 2023-12-05
- **Reason:** DATA INTEGRITY ON PLATFORM — CRC calculations are wrong on zSeries (big-endian), and existing CRC tests are disabled for that platform. Incorrect checksums mean silent corruption may go undetected on zSeries.

---

### WT-11503 — WT_CEIL_POS macro produces 0 instead of 1 for very small decimal values
- **Priority:** P3 | **Status:** Open | **Type:** Improvement
- **Team:** Foundations
- **Last Updated:** 2025-03-26
- **Reason:** INCORRECT BEHAVIOR — A macro used in cache/eviction math can return 0 for valid non-zero inputs, which could divide by zero or cause incorrect cache sizing calculations. Recently active.

---
