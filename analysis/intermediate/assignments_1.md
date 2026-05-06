# Team Assignments - Group 1 (WT-999 to WT-6076)

## WT-999: Test hot backup with a log path
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about hot backup behavior when log files are stored in a non-default path, which falls under the backup domain owned by Persistence.

---

## WT-1559: backup log URIs need to use log path
- **Team:** Storage Engines - Persistence
- **Reason:** This is a bug/task about backup not working correctly with log files stored in a separate path, covering both backup and logging (WAL) functionality owned by Persistence.

---

## WT-2144: Deprecate support for overflow keys
- **Team:** Storage Engines - Persistence
- **Reason:** Overflow items (large values/keys stored outside pages) are explicitly owned by Persistence; this ticket proposes deprecating overflow key support.

---

## WT-3246: Expose internal thread states to allow applications to track idleness
- **Team:** Storage Engines - Foundations
- **Reason:** This is an API/connection-level improvement to expose internal thread state information to applications, which falls under the API and sessions/connections domain owned by Foundations.

---

## WT-3519: Review uses of API_END_RET_NOTFOUND_MAP
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about reviewing API error-mapping macros used in session and connection methods, which is an API/session-level concern owned by Foundations.

---

## WT-3626: Allow updates to be restored against an empty column store page
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket concerns update/restore eviction behavior on column store pages, which is part of the cache/eviction management and B-tree reconciliation domain owned by Transactions.

---

## WT-3633: Have checkpoints be less IO hungry in low throughput workloads
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about tuning checkpoint I/O behavior to reduce impact on application latency, which is squarely in the checkpoints domain owned by Persistence.

---

## WT-3700: Test crashing during various non-CRUD operations
- **Team:** Storage Engines - Foundations
- **Reason:** This is a broad correctness testing initiative spanning crash/recovery scenarios for schema operations, backup, verify/salvage, and other operations — a cross-cutting test improvement owned by Foundations.

---

## WT-3723: Add timestamp support to wtperf
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about enhancing the wtperf performance benchmarking tool to support timestamps, which falls under performance benchmarking tools owned by Foundations.

---

## WT-3731: Avoid making a copy of table URI in every cursor
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cursor performance improvement involving dhandle reference counting and table URI management, touching both the cursor layer and dhandle cache owned by Foundations.

---

## WT-3778: Enhance test timestamp abort to support modify operations
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about expanding the timestamp_abort correctness test to include cursor modify operations, which is a correctness test framework improvement owned by Foundations.

---

## WT-3873: Document legal page state transitions
- **Team:** Storage Engines - Transactions
- **Reason:** Page state transitions are part of the in-memory B-tree format and B-tree operations domain owned by Transactions.

---

## WT-3951: Add bulk load and checkpoint abort test
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about creating a new correctness/stress test for bulk load combined with checkpoint abort, which falls under the correctness frameworks owned by Foundations.

---

## WT-3965: Make schema operations atomic
- **Team:** Storage Engines - Foundations
- **Reason:** Schema operations (create, drop, alter, rename) and their atomicity via metadata transactions are explicitly owned by Foundations.

---

## WT-3983: Transaction isolation documentation should cover phantom reads and write skew
- **Team:** Storage Engines - Transactions
- **Reason:** This is documentation about transaction isolation semantics (phantom reads, write skew) which is core transaction behavior owned by Transactions.

---

## WT-4047: Document what split generations are, and how they work
- **Team:** Storage Engines - Transactions
- **Reason:** Split generations are a mechanism for safely freeing structures replaced during page splits, which is part of B-tree operations (splits) owned by Transactions.

---

## WT-4054: Free transaction snapshot resources on session reset
- **Team:** Storage Engines - Transactions
- **Reason:** Transaction snapshot arrays are a transaction-layer resource; freeing them on session reset is a transaction memory management concern owned by Transactions.

---

## WT-4066: Improve test coverage for timestamp races
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket is about adding test coverage for races between timestamp setting and transaction snapshot acquisition, which is a transaction/timestamp correctness concern owned by Transactions.

---

## WT-4073: Provide a way to fix app_metadata inconsistency after non-exclusive alter call
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket concerns metadata consistency for schema objects (tables, indexes, colgroups) after alter operations, which is a schema operations and metadata integrity concern owned by Foundations.

---

## WT-4082: Track all memory allocations not intended for the cache
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cross-cutting infrastructure improvement to categorize and track non-cache memory allocations across WiredTiger, which is a systemic/cross-component improvement owned by Foundations.

---

## WT-4089: Inconsistency in documentation configuration output
- **Team:** Storage Engines - Foundations
- **Reason:** This is a documentation formatting/style issue in the WiredTiger docs, which is a build system and code style concern owned by Foundations.

---

## WT-4095: Review log slot switch algorithm to reduce lock contention
- **Team:** Storage Engines - Persistence
- **Reason:** The log slot switching algorithm is part of the write-ahead log (WAL/logging) system, which is owned by Persistence.

---

## WT-4109: Extend testing of write-failure scenarios
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about testing WiredTiger behavior when filesystem write operations fail, which relates to the filesystem API abstraction owned by Persistence.

---

## WT-4158: Fix concurrent behaviour of insert with truncate
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket concerns undefined behavior when a concurrent transaction inserts into a range being truncated, including crash/recovery inconsistencies — a transaction isolation and visibility concern owned by Transactions.

---

## WT-4161: Extend test/format to test write failure handling
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about extending the format correctness test framework to cover write failure scenarios, which is a correctness framework improvement owned by Foundations.

---

## WT-4165: Optimize stability of workload with many tables
- **Team:** Storage Engines - Transactions
- **Reason:** The described issue — dirty cache pushing above 20% during checkpoints with 18,000 tables — is primarily a cache/eviction and checkpoint interaction issue owned by Transactions (cache/eviction) with Persistence involvement; the cache pressure aspect makes Transactions the primary owner.

---

## WT-4173: workgen: refactor runner functions
- **Team:** Storage Engines - Foundations
- **Reason:** Workgen is a performance benchmarking tool; refactoring its runner library is a benchmarking tool improvement owned by Foundations.

---

## WT-4180: Transaction sync timeout in log flush testing
- **Team:** Storage Engines - Persistence
- **Reason:** The failure involves a transaction sync timeout during log flush testing, which is a logging/WAL behavior issue owned by Persistence.

---

## WT-4204: Add test case to verify complex metadata
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about adding tests for complex metadata (app_metadata with JSON) through the alter API, which covers metadata/schema table integrity owned by Foundations.

---

## WT-4320: Potentially subsume schema_abort test into random_directio
- **Team:** Storage Engines - Foundations
- **Reason:** This is about consolidating schema-related crash/recovery correctness tests, which falls under the correctness frameworks owned by Foundations.

---

## WT-4354: Improve fast path WT_SESSION:alter
- **Team:** Storage Engines - Foundations
- **Reason:** Optimizing the `WT_SESSION::alter` API to avoid acquiring locks when nothing changes is a schema operations and API-level improvement owned by Foundations.

---

## WT-4363: Identify and improve test coverage gaps
- **Team:** Storage Engines - Foundations
- **Reason:** This is a broad correctness and CI/CD testing improvement covering test coverage measurement and gap-filling across the codebase, owned by Foundations.

---

## WT-4365: Simplify control flow in dhandle close function
- **Team:** Storage Engines - Foundations
- **Reason:** The dhandle close function is part of data handle management / dhandle cache, which is explicitly owned by Foundations.

---

## WT-4388: Add complex table types to abort csuite tests
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket extends csuite crash/recovery tests to include column groups and indexes, which is a correctness framework improvement owned by Foundations.

---

## WT-4391: Tracking file system latency below 10ms
- **Team:** Storage Engines - Persistence
- **Reason:** Adding finer-grained latency buckets for filesystem operations falls under the filesystem API layer owned by Persistence.

---

## WT-4462: Refactor top level open_cursor code
- **Team:** Storage Engines - Foundations
- **Reason:** Refactoring `__session_open_cursor` in session_api.c is an API/session-level code quality improvement owned by Foundations (cursors/sessions).

---

## WT-4487: Use more accurate statistics for various running totals
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cross-cutting infrastructure improvement to use atomic operations for statistics counters, which is a systemic/memory model concern owned by Foundations.

---

## WT-4597: Add a static test for verifying the correctness of statistic values
- **Team:** Storage Engines - Foundations
- **Reason:** Adding tests to verify statistics values for complex tables is a correctness framework improvement owned by Foundations.

---

## WT-4622: Handle txn_state->is_allocating routines in __wt_verbose_dump_txn
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket involves the global transaction state table and the `is_allocating` flag used during transaction ID allocation, which is core transaction infrastructure owned by Transactions.

---

## WT-4656: Enhance salvage to use timestamps when determining recency
- **Team:** Storage Engines - Persistence
- **Reason:** Salvage is explicitly owned by Persistence; this enhancement extends it to use timestamps when determining the most recent versions of key/value pairs.

---

## WT-4667: Add automated testing for non-hardware CRC functionality
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about adding CI/CD test coverage for non-default build configurations (e.g., `--disable-crc32-hardware`), which is a CI/CD infrastructure concern owned by Foundations.

---

## WT-4713: Python documentation not exposed at top level
- **Team:** Storage Engines - Foundations
- **Reason:** This concerns Python language binding documentation and the Doxygen/pyfilter scripts for Python API docs, which falls under language bindings and build system owned by Foundations.

---

## WT-4802: Enable and improve random dhandle selection and eviction target calculations
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket investigates improvements to eviction target calculations and random dhandle selection in the eviction walk, which is part of cache/eviction management owned by Transactions.

---

## WT-4813: Enable cursor caching for statistics cursors
- **Team:** Storage Engines - Foundations
- **Reason:** Cursor caching is a cursor-layer performance optimization; enabling it for statistics cursors falls under the cursor (CRUD) layer owned by Foundations.

---

## WT-4880: Make Python tests work with statically linked extensions
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build system issue where statically linked (builtin) compressor extensions are not picked up by Python tests, touching both the build system and Python language bindings owned by Foundations.

---

## WT-4903: extend test/checkpoint online snapshot verification using prev
- **Team:** Storage Engines - Foundations
- **Reason:** This extends the test/checkpoint correctness test to use `cursor->prev` for online snapshot consistency checking, which is a correctness framework improvement owned by Foundations.

---

## WT-4914: Log cursor value_format change from qIIIuu to QIIIuu
- **Team:** Storage Engines - Persistence
- **Reason:** This bug is about an incorrect type declaration in the log cursor format for the txnid field, which is part of the logging/WAL system owned by Persistence.

---

## WT-4938: Error while running to install the wiredtiger Python module on Windows
- **Team:** Storage Engines - Foundations
- **Reason:** This is a Python language binding installation issue (Windows pip install failure), which falls under language bindings owned by Foundations.

---

## WT-4941: Add accessor functions for WT_CONFIG_ITEM fields
- **Team:** Storage Engines - Foundations
- **Reason:** `WT_CONFIG_ITEM` accessor functions are part of the configuration API infrastructure, which is an API/session-level concern owned by Foundations.

---

## WT-4945: Expand io_capacity configuration setting to allow number of IOs
- **Team:** Storage Engines - Persistence
- **Reason:** The `io_capacity` configuration controls I/O rate limiting at the filesystem layer, which is a filesystem API / block manager concern owned by Persistence.

---

## WT-4948: WiredTiger.backup file should be a normal WT table
- **Team:** Storage Engines - Persistence
- **Reason:** The `WiredTiger.backup` file is part of the backup system; converting it to use the block manager would put it through compression/encryption paths, which is a backup and block manager concern owned by Persistence.

---

## WT-4951: Create standalone disk validation utility
- **Team:** Storage Engines - Persistence
- **Reason:** A disk validation utility to detect data corruption susceptibility relates to verify and disk integrity concerns owned by Persistence.

---

## WT-4962: add gdb functions that mimic the debug functions
- **Team:** Storage Engines - Transactions
- **Reason:** The debug functions being mirrored (in `btree/bt_debug.c`) are for inspecting cache, update structures, and the B-tree — core B-tree and transaction data structures owned by Transactions.

---

## WT-5035: Decommission Jenkins CI system
- **Team:** Storage Engines - Foundations
- **Reason:** Decommissioning the Jenkins CI system is a CI/CD infrastructure task owned by Foundations.

---

## WT-5049: Removal of turtle file should be salvageable
- **Team:** Storage Engines - Persistence
- **Reason:** This is about the salvage operation's ability to recover from a missing `WiredTiger.turtle` file, which is a salvage concern owned by Persistence.

---

## WT-5053: Enhance salvage database to be able to use source objects
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket enhances salvage to reconstruct the turtle file and WiredTiger.wt from source files, which is a salvage improvement owned by Persistence.

---

## WT-5070: Test that using WT_CURSOR::modify works with all visibility scenarios
- **Team:** Storage Engines - Transactions
- **Reason:** Testing cursor modify operations under unusual timestamp and visibility rules is a transaction/timestamp visibility correctness concern owned by Transactions.

---

## WT-5091: Enhance the random_abort to fine control the test execution
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket enhances the `random_abort` csuite correctness test with additional command-line options, which is a correctness framework improvement owned by Foundations.

---

## WT-5103: Investigate improvements to eviction slot calculations
- **Team:** Storage Engines - Transactions
- **Reason:** Eviction slot calculations (`WT_EVICT_WALK_BASE`, `WT_EVICT_WALK_INCR`) are part of the eviction algorithm owned by Transactions.

---

## WT-5107: Update WiredTiger Python formatting/linting standard to match MongoDB
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code style / linting infrastructure improvement for Python scripts, which falls under build system and code style owned by Foundations.

---

## WT-5110: Add dsrc statistic for size of checkpoints unable to be deleted
- **Team:** Storage Engines - Persistence
- **Reason:** This statistic tracks checkpoint space retained because a backup cursor is open, which is a backup and checkpoint space management concern owned by Persistence.

---

## WT-5127: Fix a bug where code uses leaf page size, not memory page max
- **Team:** Storage Engines - Transactions
- **Reason:** The `__wt_leaf_page_can_split` function determines when a page should be split, which is a B-tree split decision in the B-tree operations domain owned by Transactions.

---

## WT-5133: Replace wt_epoch with wt_clock where we can
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cross-cutting performance improvement replacing `wt_epoch` with `wt_clock` across the codebase, which is a systemic/infrastructure improvement owned by Foundations.

---

## WT-5147: fast-path search isn't implemented for the read-committed isolation level
- **Team:** Storage Engines - Transactions
- **Reason:** Fast-path search optimization and read-committed isolation level behavior are transaction/B-tree search concerns owned by Transactions.

---

## WT-5180: Exclude .git from the evergreen artifact tar ball
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD infrastructure fix for Evergreen artifact packaging, which falls under CI/CD pipelines owned by Foundations.

---

## WT-5332: Investigate the impact of slow checkpoints using the new debug mode
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket investigates the impact of the slow-checkpoint debug mode on WiredTiger behavior, which is a checkpoint investigation owned by Persistence.

---

## WT-5390: Document wiredtiger structs memory padding / management in the developer docs
- **Team:** Storage Engines - Foundations
- **Reason:** This is developer documentation about memory padding and struct management, which is a cross-cutting/systemic infrastructure documentation concern owned by Foundations.

---

## WT-5396: Review how WiredTiger uses WT_PUBLISH and WT_ORDERED_WRITE
- **Team:** Storage Engines - Foundations
- **Reason:** `WT_PUBLISH` and `WT_ORDERED_WRITE` are memory ordering/barrier macros — a memory model concern explicitly owned by Foundations.

---

## WT-5399: Python: Fix Session.strerror()
- **Team:** Storage Engines - Foundations
- **Reason:** This is a bug in the Python/SWIG language binding where `Session.strerror()` returns an incorrect result, which is a language bindings issue owned by Foundations.

---

## WT-5430: Write out debug log records for operations that do not commit
- **Team:** Storage Engines - Persistence
- **Reason:** Debug logging is part of the logging/WAL subsystem; extending it to record uncommitted operations is a logging enhancement owned by Persistence.

---

## WT-5472: Add statistic that tracks when salvage builds big internal pages
- **Team:** Storage Engines - Persistence
- **Reason:** This adds a statistic to the salvage operation for a specific code path in `__slvg_row_build_internal`, which is a salvage improvement owned by Persistence.

---

## WT-5494: Request for example usages of wt utility in documentation
- **Team:** Storage Engines - Foundations
- **Reason:** Documentation improvements for the `wt` command-line utility fall under build system and general documentation owned by Foundations.

---

## WT-5498: Investigate ftdc stalls when trying to delete checkpoint during backup cursor execution
- **Team:** Storage Engines - Persistence
- **Reason:** This investigates stalls related to checkpoint deletion while a backup cursor is open, which is a backup and checkpoint interaction concern owned by Persistence.

---

## WT-5511: Document the usage of split generation code for concurrent access of page index
- **Team:** Storage Engines - Transactions
- **Reason:** Split generation tracking for safe concurrent page index access is part of the B-tree split/operations infrastructure owned by Transactions.

---

## WT-5514: Summarise the search changes and outline how the search works in durable history
- **Team:** Storage Engines - Transactions
- **Reason:** This documents search changes made as part of durable history (history store), covering B-tree search and transaction visibility owned by Transactions.

---

## WT-5528: Create an on-boarding document of Should-Read WT Wiki Pages
- **Team:** Storage Engines - Foundations
- **Reason:** On-boarding and team documentation is a cross-cutting infrastructure concern owned by Foundations.

---

## WT-5561: Add __wt_fsync histogram statistics
- **Team:** Storage Engines - Persistence
- **Reason:** `__wt_fsync` is a filesystem sync operation; adding histogram statistics for it falls under the filesystem API layer owned by Persistence.

---

## WT-5586: Update WT package on PyPi to include compressor libs
- **Team:** Storage Engines - Foundations
- **Reason:** This is a Python packaging and distribution improvement for the WiredTiger Python module, which falls under language bindings and build/release owned by Foundations.

---

## WT-5592: WiredTiger assumes URI arguments contain printable characters
- **Team:** Storage Engines - Foundations
- **Reason:** Validating and sanitizing URI arguments at the API level is an API/session-level concern owned by Foundations.

---

## WT-5599: Explore discarding obsolete updates when checkpointing
- **Team:** Storage Engines - Transactions
- **Reason:** Discarding obsolete updates from the update chain during checkpoint is primarily an eviction/cache management and reconciliation concern, touching transaction visibility — owned by Transactions.

---

## WT-5646: Python interface for cursors should raise an exception on cursor error
- **Team:** Storage Engines - Foundations
- **Reason:** This is a bug in the Python/SWIG cursor interface (`IterableCursor.__next__`) where non-zero/non-WT_NOTFOUND errors are silently lost, which is a language bindings issue owned by Foundations.

---

## WT-5709: In the Python test suite, explore adding a timeout on all test functions
- **Team:** Storage Engines - Foundations
- **Reason:** Adding timeouts to Python test suite functions is a correctness/CI test infrastructure improvement owned by Foundations.

---

## WT-5793: Remove WT_REC_VISIBLE_ALL flag
- **Team:** Storage Engines - Transactions
- **Reason:** `WT_REC_VISIBLE_ALL` is a reconciliation flag controlling which updates are written to disk during B-tree reconciliation, which is owned by Transactions.

---

## WT-5802: Reduce runtime of Python history store tests
- **Team:** Storage Engines - Foundations
- **Reason:** Reducing test runtime and improving test bucketing in Evergreen is a CI/CD and test infrastructure concern owned by Foundations.

---

## WT-5818: Add ability for a cursor to not participate in cursor copy debug functionality
- **Team:** Storage Engines - Foundations
- **Reason:** The cursor copy debug feature is a cursor-layer debug mechanism; adding an opt-out flag is a cursor API/session concern owned by Foundations.

---

## WT-5832: Detect potential corruption as part of recovery/rollback to stable
- **Team:** Storage Engines - Persistence
- **Reason:** Detecting data corruption during RTS (Rollback to Stable) and recovery is a rollback-to-stable and verify/salvage concern owned by Persistence.

---

## WT-5924: Integrate alphabetic Clang Tidy check into PR testing
- **Team:** Storage Engines - Foundations
- **Reason:** Integrating a Clang Tidy check for code style (alphabetic variable declaration ordering) into PR/CI pipelines is a build system and CI/CD infrastructure task owned by Foundations.

---

## WT-5942: Improve how we track which updates are restored during reconciliation
- **Team:** Storage Engines - Transactions
- **Reason:** Tracking updates to be restored at the update chain, split page, and page level during reconciliation is a B-tree reconciliation concern owned by Transactions.

---

## WT-5947: Investigate if we need to free the updates in scrub eviction
- **Team:** Storage Engines - Transactions
- **Reason:** `WT_REC_SCRUB` eviction and the decision of whether to free updates moved to data store/history store is a cache/eviction and reconciliation concern owned by Transactions.

---

## WT-5996: Review WT_SESSION_NO_LOGGING and if other flags should be retained when calling RTS
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket reviews session flag retention during RTS (Rollback to Stable), specifically the `WT_SESSION_NO_LOGGING` flag, which is an RTS and logging interaction owned by Persistence.

---

## WT-6005: Create a python test that validates the new version check performed on start
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds a Python test for the `__turtle_validate_version` function (version compatibility checking on open), which is a metadata/schema table integrity and API-level test owned by Foundations.

---

## WT-6012: Enhance test/format to support -R and -C option at the same time
- **Team:** Storage Engines - Foundations
- **Reason:** This is a correctness framework enhancement to the format test program to support combined command-line options, owned by Foundations.

---

## WT-6024: Make testing binaries relocatable across machines and folders
- **Team:** Storage Engines - Foundations
- **Reason:** Making test binaries relocatable via portable RPATH settings is a build system improvement owned by Foundations.

---

## WT-6028: Signal in the API when a wiredtiger_open call fails due to compatibility_version
- **Team:** Storage Engines - Foundations
- **Reason:** Improving the `wiredtiger_open` API to distinguish compatibility version errors from other failures is an API/connection-level concern owned by Foundations.

---

## WT-6037: Performance degradation because of open/close history store cursor to cache dhandle
- **Team:** Storage Engines - Foundations
- **Reason:** The performance regression relates to opening/closing history store cursors and dhandle caching, which is a dhandle cache and cursor management concern owned by Foundations.

---

## WT-6076: Extend format to run with 'S' modifies occasionally
- **Team:** Storage Engines - Foundations
- **Reason:** Extending test/format to use string ('S') modify operations for broader test coverage is a correctness framework improvement owned by Foundations.

---
