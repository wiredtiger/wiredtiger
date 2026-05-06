# WiredTiger Ticket Team Assignments

Analysis of 574 open WiredTiger Jira tickets with missing "Assigned Teams" field.
Assignments based on team charters for the three Storage Engines sub-teams.

## Summary

| Team | Tickets | % |
|------|---------|---|
| Storage Engines - Foundations | 360 | 62.7% |
| Storage Engines - Transactions | 129 | 22.5% |
| Storage Engines - Persistence | 85 | 14.8% |
| Unclear | 0 | 0.0% |
| **Total** | **574** | **100%** |

---


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

## WT-6100: Find a way to do "eatmydata" on Windows to speed up test suite.
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/test infrastructure improvement focused on making FlushFileBuffers a no-op on Windows during test runs, which falls under build system and correctness frameworks.

---

## WT-6112: Review the utility code and make sure it works as an external application
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about the WiredTiger utility tool using internal API flags and cleaning up API conventions, which belongs to the API/sessions domain.

---

## WT-6119: History store verification does not verify the table itself
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about extending the verify function to also verify the history store table itself, which falls under the Verify component.

---

## WT-6143: Document the metadata stored along with a key value pair on a page
- **Team:** Storage Engines - Transactions
- **Reason:** This documentation ticket describes the cell format and metadata stored with key/value pairs on pages, which is in-memory B-tree page format territory.

---

## WT-6262: Extend operation time tracking stats to report per dhandle
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket extends statistics tracking to the dhandle level, which relates to data handle management and per-table stats infrastructure.

---

## WT-6304: add option to redirect WiredTiger verbose messages to the WiredTiger log
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket adds an option to redirect verbose messages to the WAL/WiredTiger log, which is a logging/WAL infrastructure concern.

---

## WT-6316: Need test for backup, versions and logpath
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about adding a test for backup behavior combined with version checking and log path configurations, covering both backup and logging components.

---

## WT-6321: Upgrade Evergreen Windows distribution to vs2019
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD infrastructure improvement for Evergreen pipelines, upgrading the Windows build/test distribution.

---

## WT-6391: Improve set_timestamp API to ensure consistent stable and oldest usage.
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket improves the timestamp API enforcement for stable and oldest timestamps, which is core transaction timestamp management.

---

## WT-6420: Stop restricting dirty cache usage with small caches
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket addresses dirty cache size limits and eviction configuration, which is part of cache/eviction management.

---

## WT-6431: Ensure rollback to stable handles corrupted files as expected
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about RTS gracefully handling corrupted files, which is a Rollback to Stable concern.

---

## WT-6437: Add statistics tracking history store insert types
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket adds statistics for history store insert types (prepared updates, out-of-order timestamps, etc.), which tracks transaction-layer history store operations.

---

## WT-6459: Remove the extra memory copy in __wt_hs_find_upd
- **Team:** Storage Engines - Transactions
- **Reason:** This is an optimization in the history store update lookup path, which is part of transaction visibility and the history store layer.

---

## WT-6489: Refactor __wt_hs_insert_updates
- **Team:** Storage Engines - Transactions
- **Reason:** This refactoring ticket targets __wt_hs_insert_updates, which is the reconciliation-time function that moves updates into the history store.

---

## WT-6500: History store tombstones with transaction id 0 can theoretically cause use-after-free
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket describes a potential use-after-free caused by history store tombstones with txn id=0 bypassing pinned_id protection, which is a transaction visibility and history store correctness issue.

---

## WT-6516: Fix conditional detecting wasted reconciliation calls
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket improves a complex conditional in the reconciliation code that reaches into internal reconciliation data structures, which is a B-tree reconciliation concern.

---

## WT-6531: Refactor incremental backup config parsing functions
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket refactors duplicated config parsing code in the incremental backup cursor and checkpoint metadata loading functions, which is a backup/incremental backup concern.

---

## WT-6536: slowdown during run of test_wt2853_perf
- **Team:** Storage Engines - Transactions
- **Reason:** The performance slowdown involves eviction behavior during concurrent reads and writes with WT indices, pointing to cache/eviction management issues.

---

## WT-6541: Explore removing evict priority
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket investigates removing the evict_priority mechanism that makes metadata pages stick in cache, which is squarely in cache/eviction management.

---

## WT-6545: Ensure Truncate operation does not fast truncate pages with active prepared updates.
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket ensures fast truncate respects active prepared transactions, combining transaction state (prepared updates) with B-tree page operations.

---

## WT-6565: Onpage value may be duplicated on the update chain for in-memory database
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket addresses a memory inefficiency where reconciliation duplicates the on-disk value on the in-memory update chain, which is a reconciliation and in-memory B-tree concern.

---

## WT-6574: Allow writing modifies to the history store in some edge cases
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket revisits the decision to disable writing modify operations to the history store in edge cases like out-of-order timestamps and prepared updates, which is a transaction history store concern.

---

## WT-6590: Allow eviction of clean pages during reconciliation
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket investigates allowing clean page eviction while a session is already performing reconciliation, which involves both cache/eviction management and B-tree reconciliation.

---

## WT-6601: Clang Format doesn't handle TAILQ macros well
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket addresses code style/formatting tooling issues with clang-format and TAILQ macros, which is a build system and code style concern.

---

## WT-6614: verify tests exist for all parts of the API configurations
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket proposes ensuring test coverage for all API configuration options, which is a correctness framework and API testing concern.

---

## WT-6627: Unexpected WriteConflictException during insert benchmark with transactions
- **Team:** Storage Engines - Transactions
- **Reason:** The issue stems from forced eviction of oversized pages causing WT_ROLLBACK during inserts, which is an eviction/cache management and transaction interaction issue.

---

## WT-6631: Break up __rec_append_orig_value into two functions to simplify code
- **Team:** Storage Engines - Transactions
- **Reason:** This refactoring targets the reconciliation code that decides when to append the on-page value to the update chain, which is a B-tree reconciliation concern.

---

## WT-6638: Add testing to ensure we do not regress the database size
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket proposes a standalone WT test to detect database size on disk regressions, which is a correctness/performance benchmarking and test framework concern.

---

## WT-6646: Implement ENOSPC fault injection for WiredTiger
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket builds fault injection infrastructure for out-of-space errors in the filesystem abstraction layer (fail_fs extension), which is a filesystem API concern.

---

## WT-6647: Retro Action Item: Document the Code review and coding guidelines for testing for each change
- **Team:** Storage Engines - Foundations
- **Reason:** This is a documentation/process ticket about code review and testing guidelines, which is a cross-cutting engineering process concern under Foundations.

---

## WT-6651: Write test to verify ACID guarantees after ENOSPC failure
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket writes a test verifying ACID correctness after ENOSPC errors and recovery, which combines filesystem error handling with logging/WAL durability guarantees.

---

## WT-6699: Create Evergreen task for modified LSWA workload
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds a new Evergreen CI task for a performance workload, which is a CI/CD infrastructure and performance benchmarking concern.

---

## WT-6744: Avoid duplication in test/format failure CONFIG set
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the test/format failure CONFIG management to avoid duplicating entries, which is a correctness framework and test infrastructure concern.

---

## WT-6757: Add tests for edge cases involving last block of incremental backup
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket adds test cases for incremental backup block tracking edge cases when the last block of a file changes size, which is a backup concern.

---

## WT-6758: Documentation: add rows to doc "pointer" page for MongoDB releases
- **Team:** Storage Engines - Foundations
- **Reason:** This is a documentation ticket about updating the WiredTiger documentation website to reference MongoDB release versions, which is a release management/documentation concern.

---

## WT-6759: Create automatic check for non-atomic struct assignments
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket creates tooling to automatically detect non-atomic struct assignments related to memory model correctness, which is a memory models/atomic operations concern.

---

## WT-6760: Investigate compiler-based tools for syntax & usage checks
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket investigates using libclang/compiler-based tools to replace shell-script-based code analysis, which is a build system/code style tooling concern.

---

## WT-6777: Add incremental backup performance focused test
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket adds a performance test for incremental backup comparing it against full backup under various workloads, which is a backup performance testing concern.

---

## WT-6787: Add more request mixes to "remove eMRCf" workload
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket extends a performance benchmark workload with additional operation mixes, which is a performance benchmarking concern.

---

## WT-6795: Remove random_directio debugging from WT once solved
- **Team:** Storage Engines - Persistence
- **Reason:** The debugging code to remove is in os_posix/os_fs.c (the filesystem abstraction layer), making this a filesystem API/persistence concern.

---

## WT-6807: Windows doesn't move existing files out of the way when creating a new file with the same name
- **Team:** Storage Engines - Persistence
- **Reason:** This bug involves file rename/creation behavior in the filesystem abstraction layer on Windows, which is a filesystem API concern.

---

## WT-6810: Add data source statistics for some if not all connection level statistics
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket extends connection-level statistics down to the per-data-source level, which relates to data handle management and statistics infrastructure.

---

## WT-6814: Retro Action Item:Write a wiki page on upgrade/downgrade
- **Team:** Storage Engines - Foundations
- **Reason:** This is a documentation ticket about upgrade/downgrade procedures, which is a release management concern.

---

## WT-6837: Don't insert globally hidden modifies into the history store.
- **Team:** Storage Engines - Transactions
- **Reason:** This optimization prevents inserting globally hidden modify records into the history store during reconciliation, which is a transaction visibility and history store concern.

---

## WT-6865: Improve timestamp usage assertion code
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket improves the timestamp abort checking logic at the dhandle/table level during transaction commit, which is a transaction timestamp management concern.

---

## WT-6878: Improve configuration string handling in dhandle open
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket clarifies the dual configuration string handling in __wt_conn_dhandle_open, which is a data handle management and configuration API concern.

---

## WT-6918: lldb cannot attach to processes in MacOS - Hang analyzer
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about fixing the hang analyzer CI script for macOS, which is a CI/CD infrastructure concern.

---

## WT-6919: Windows cannot find the debug symbols - Hang analyzer.
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket fixes debug symbol resolution in the hang analyzer for Windows, which is a CI/CD infrastructure concern.

---

## WT-6920: Identify and fix references to non-existent functions in documentation
- **Team:** Storage Engines - Foundations
- **Reason:** This is a documentation maintenance ticket about fixing broken API references in the WiredTiger docs, which is a release management/documentation concern.

---

## WT-6930: Improve the test/format timestamp usage to better match MDB server usage
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves timestamp usage in test/format to better match MongoDB server patterns, which is a correctness framework and stress test improvement.

---

## WT-6940: Add ability to provide fail points separate to main API
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket builds infrastructure to define fault injection points separately from the main API config, which is a correctness framework and test infrastructure concern.

---

## WT-6941: Implement code to parse fail points into internal data structures
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket implements session-scoped data structures for managing fault point state, which is part of the fault injection/stress testing infrastructure under correctness frameworks.

---

## WT-6942: Implement and test fail points which forces eviction of pages on release from a cursor
- **Team:** Storage Engines - Transactions
- **Reason:** While part of the fail points infrastructure series, this specific ticket adds an eviction fault point triggered on cursor release, which directly exercises cache/eviction management.

---

## WT-6943: Implement and test fail points causes sporadic failures in reconciliation at some interesting point
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket adds fail points specifically targeting reconciliation failure scenarios, which is a B-tree reconciliation concern within the stress testing framework.

---

## WT-6944: Design: Tune fail point functionality to ensure failures are as obvious as possible
- **Team:** Storage Engines - Foundations
- **Reason:** This design ticket is about making the fail point infrastructure more debuggable and observable (logging, consistency), which is a cross-cutting correctness framework concern.

---

## WT-6945: Implement: Tune fail point functionality to ensure failures are as obvious as possible
- **Team:** Storage Engines - Foundations
- **Reason:** This implementation ticket follows from WT-6944 and builds out the fail point framework logging/observability, which is a correctness framework concern.

---

## WT-6954: Generate RELEASE_INFO file with the git hash where the build is initated
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds a RELEASE_INFO file with the git hash at build time, which is a build system and release management concern.

---

## WT-6955: Test restart performance with large history store due to a pinned stable timestamp
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket tests RTS performance when recovering with a large history store accumulated due to a pinned stable timestamp, which is a Rollback to Stable performance concern.

---

## WT-6977: Retro Action Item : Write about "Converting WiredTiger into C++ project"
- **Team:** Storage Engines - Foundations
- **Reason:** This documentation ticket is about exploring conversion of WiredTiger to C++, which is a build system and cross-cutting systemic improvement concern.

---

## WT-6985: Verify the current key history store key order for every data store key order check
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket adds history store key ordering verification during verify operations in DIAGNOSTIC mode, which is a Verify component concern.

---

## WT-6987: Create test(s) to verify that ENOSPC errors are always reported
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket creates tests ensuring ENOSPC errors are consistently reported via error codes or log messages, which relates to filesystem API error handling and logging.

---

## WT-6988: Replace python test suite eviction loops with debug eviction cursor
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves Python test suite determinism by replacing polling eviction loops with the debug release_evict cursor, which is a correctness framework and test infrastructure improvement.

---

## WT-7016: Add new verbose messages to find out the usage of history store
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket adds verbose logging to the history store access paths, which helps diagnose transaction history store usage and bugs.

---

## WT-7017: Document and share "Convert WiredTiger into C++ project"
- **Team:** Storage Engines - Foundations
- **Reason:** This documentation ticket shares findings on converting WiredTiger to C++, which is a build system and engineering process concern.

---

## WT-7018: Add "Write gen" page in architecture guide
- **Team:** Storage Engines - Transactions
- **Reason:** Write generation is a concept related to in-memory B-tree page state tracking and cache management; documenting it belongs with the Transactions team.

---

## WT-7021: Review the design of the global operation timeout
- **Team:** Storage Engines - Foundations
- **Reason:** The operation timeout mechanism spans API calls (sessions, cursors, connection) and was designed to address cache-stuck scenarios; reviewing its design is a cross-cutting API/sessions concern.

---

## WT-7037: Investigate more compact representation for small history store records
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket investigates changes to the history store on-disk data format to reduce storage for small modify records, which is a transaction history store format concern.

---

## WT-7042: large_scale_long_lived and large_scale_model genny workloads potentially faulty
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket investigates and fixes potentially broken performance benchmark workloads, which is a performance benchmarking concern.

---

## WT-7048: Review diagnostic assertions and add informational log messages
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket reviews WT_ASSERT usage across the codebase and adds informational log messages where useful, which is a cross-cutting code quality and correctness concern.

---

## WT-7052: Investigate WT cache eviction improvements based on newer cache replacement algorithms
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket investigates applying modern cache replacement algorithms (ARC, LIRS, Multi Queue) to WiredTiger's eviction system, which is a cache/eviction management concern.

---

## WT-7061: Write "split" internal doc, to be added to Architecture Guide
- **Team:** Storage Engines - Transactions
- **Reason:** This documentation ticket describes the page splitting algorithms and protection mechanisms for B-tree internal pages, which is a B-tree operations concern.

---

## WT-7082: Log a message or create a statistic to record a failure case when modifying the update chain
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket adds logging/statistics for a failure case in __wt_txn_modify when update chain modification fails after insertion, which is a transaction layer concern.

---

## WT-7096: Improve the mechanism that collects cache usage stats for the history store
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket improves how eviction collects history store cache usage statistics, which involves both cache/eviction management and the history store.

---

## WT-7098: Improve autonomy in evergreen to run tests that use randomness
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the CI/CD Evergreen test suite to automatically discover and run tests using the random seed feature, which is a CI/CD infrastructure concern.

---

## WT-7115: Consider always running prototypes.py as part of make
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket considers integrating prototypes.py into the build process to keep extern.h up to date, which is a build system concern.

---

## WT-7142: Add comments explaining mixed mode testing to test_hs18
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket adds explanatory comments to mixed-mode transaction tests in the history store test suite, which relates to transaction timestamp testing.

---

## WT-7157: Investigate 'wt downgrade' hanging when reconfiguring to incompatible version
- **Team:** Storage Engines - Persistence
- **Reason:** The hang occurs in __log_slot_switch after a log version incompatibility error, which is a logging/WAL concern.

---

## WT-7170: History Store truncates when stable timestamp is not set
- **Team:** Storage Engines - Persistence
- **Reason:** The RTS sweep phase truncates the entire history store when stable timestamp is not set; this is a Rollback to Stable behavioral issue.

---

## WT-7194: Create test for drop and cursor->close interaction
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket creates a test for cursor close behavior on a dropped dhandle, combining cursor management with schema drop operations and dhandle lifecycle.

---

## WT-7203: Add WT diagnostic mode test for conflicting session use by a thread
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds diagnostic session-to-thread validation to detect unsafe multi-session usage from a single thread, which is an API/sessions correctness concern.

---

## WT-7212: Improve handling of mixed mode and out-of-order operations in history store reconciliation code
- **Team:** Storage Engines - Transactions
- **Reason:** This refactoring unifies the handling of mixed-mode and out-of-order timestamp operations during history store reconciliation, which is a B-tree reconciliation and transaction history store concern.

---

## WT-7213: Evergreen PR testing compilation missing '-Werror'
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds -Werror to PR testing compilation jobs in Evergreen, which is a CI/CD infrastructure and build system concern.

---

## WT-7247: Separate session frame management from api macros
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket investigates separating session frame management from API entry/exit macros so internal cursors don't need to go through the user API path, which is an API/sessions architecture concern.

---

## WT-7248: Stricter assert to ensure we don't return api call with open hs cursors
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket strengthens API entry/exit assertions related to open history store cursors, which is an API/sessions correctness concern.

---

## WT-7251: Add more testing for snapshot based visibility of out-of-order updates
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket adds tests for snapshot-based transaction visibility with out-of-order timestamp fix-up logic, which is a transaction visibility and history store correctness concern.

---

## WT-7259: Add statistics from WT extensions
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket explores adding statistics infrastructure to WiredTiger extensions (e.g., for tiered storage cloud plugins), which is a cross-cutting API/statistics infrastructure concern.

---

## WT-7283: Document definitions and use cases for "mixed mode" "out of order" and "ghost" timestamps
- **Team:** Storage Engines - Transactions
- **Reason:** This documentation ticket defines mixed-mode, out-of-order, and ghost timestamp concepts and their expected WT behavior, which is a transaction timestamp documentation concern.

---

## WT-7306: Add compatibility tests for Windows platform
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds compatibility/upgrade-downgrade tests to Windows in the CI pipeline, which is a CI/CD infrastructure and correctness framework concern.

---

## WT-7310: dupekey error on uncommited write should return WT_ROLLBACK not WT_DUPLICATE_KEY
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket corrects the error code returned when a duplicate key conflict involves an uncommitted transaction, which is a transaction isolation and cursor CRUD error handling concern.

---

## WT-7347: Review to remove the compare in __curhs_search_near
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket reviews whether a comparison in the history store cursor search_near implementation can be removed, which is a transaction history store cursor optimization.

---

## WT-7362: Allow batching multiple table alterations for the same config change
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket proposes batching multiple WT_SESSION::alter() calls to reduce fsync overhead, which is a schema operations (alter) and API performance concern.

---

## WT-7408: API to return row and byte counts for objects and cursor ranges
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds a new WT_SESSION.range_stat() API for estimating row and byte counts, storing aggregate counts in internal page address cookies during reconciliation — this spans cursor API, schema, and B-tree operations.

---

## WT-7418: test/format assert gets hit verifying imported table
- **Team:** Storage Engines - Foundations
- **Reason:** This bug involves test/format hitting an assertion during import table verification, which relates to schema import operations and the correctness framework.

---

## WT-7443: Add error message when bulk cursor can't get exclusive access to dhandle
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the error message when a bulk load cursor fails to get exclusive dhandle access, which is a cursor API and dhandle management concern.

---

## WT-7482: Architecture Guide updates for PM-2293
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket updates the Architecture Guide documentation for a project, which is a release management and documentation concern.

---

## WT-7495: Cursor update can use pinned page search to update the key
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket proposes using pinned page search for cursor update operations to avoid full tree traversal, which is a B-tree operations and cursor performance concern.

---

## WT-7503: Change default compressor for WT HS to Zstandard
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket changes the default compression algorithm for the history store, which is a persistence-layer concern involving on-disk block storage and compaction-related configuration.

---

## WT-7505: Use Python hooks to improve test coverage for in-memory config
- **Team:** Storage Engines - Foundations
- **Reason:** This is a testing/correctness framework improvement using Python test hooks to expand in-memory configuration coverage, which falls under the correctness frameworks and language bindings owned by Foundations.

---

## WT-7518: Update WT_DATA_HANDLE to support different types of backing storage for Btrees
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket modifies the data handle (dhandle) management infrastructure to support tiered storage URIs alongside file-based Btrees, which is part of dhandle cache and schema operations owned by Foundations.

---

## WT-7527: Perform fine-tuning on reverse modifies for HS records
- **Team:** Storage Engines - Transactions
- **Reason:** Reverse modifies for history store records are written during B-tree reconciliation, and this ticket tunes the thresholds that govern when reconciliation switches between full updates and reverse deltas.

---

## WT-7558: 5% performance regression in retryable writes workloads with 8 threads on linux-1-node-replSet
- **Team:** Storage Engines - Foundations
- **Reason:** This is a performance benchmarking/investigation ticket focused on analyzing a regression in benchmark workloads, which falls under performance benchmarking tools and cross-cutting improvements owned by Foundations.

---

## WT-7568: Use project-level API token for git.get_project in Evergreen
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD infrastructure improvement for Evergreen pipelines, which is directly owned by Foundations.

---

## WT-7576: Remove --zstd option once zstd compressor is installed on PPC and ZSeries machines
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD and build system task to clean up a temporary workaround in the test suite for compression library availability, falling under CI/CD infrastructure owned by Foundations.

---

## WT-7597: Expand support for WiredTiger C/C++ tests on Windows
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cross-platform build system and correctness framework task to make C/C++ test suites compile and run on Windows, which falls under the build system and correctness frameworks owned by Foundations.

---

## WT-7612: Fix operation tracking after reconfiguration and add more tests
- **Team:** Storage Engines - Foundations
- **Reason:** Operation tracking is an API-level feature accessible via the reconfiguration API (sessions/connections/configuration), and fixing it along with adding tests falls under API and configuration owned by Foundations.

---

## WT-7617: Improve diagnosability of Python test hangs in Evergreen
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD and correctness framework improvement to help diagnose Python test hangs in Evergreen pipelines, which is owned by Foundations.

---

## WT-7622: Add CMake + icecream support on our dev servers
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build system improvement to add distributed build support with icecream/icecc to the CMake build system, which is directly owned by Foundations.

---

## WT-7650: Investigate test/format failing on existing databases with prefix enabled
- **Team:** Storage Engines - Foundations
- **Reason:** This is an investigation into failures in the format test correctness framework with prefix testing enabled, which falls under correctness frameworks (format test) owned by Foundations.

---

## WT-7688: Identifying and handling corrupted files in WiredTiger
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket addresses how block-manager corruption is identified and propagated through the system, involving the block manager and salvage/recovery layers which are owned by Persistence.

---

## WT-7693: Fix tiered storage disconnect between WT_BUCKET_STORAGE and customize_file_system
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about the filesystem abstraction layer for tiered storage, specifically reconciling the `WT_BUCKET_STORAGE` structure with the local store `customize_file_system`, which is part of the filesystem API owned by Persistence.

---

## WT-7734: Add dhandle flag to indicate dhandles that are both btree and object
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds a flag to the dhandle infrastructure to distinguish btree-object dhandles, which is part of data handle management owned by Foundations.

---

## WT-7735: Support tiered tables in wt_block_checkpoint_last
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket extends checkpoint scanning to support tiered tables in the block manager's `__wt_block_checkpoint_last` function, which falls under checkpoints and block manager owned by Persistence.

---

## WT-7800: Windows Evergreen Windows tests don't test extension libraries
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD infrastructure bug where extension libraries are not built and tested on Windows in Evergreen, falling under CI/CD infrastructure and build system owned by Foundations.

---

## WT-7862: Reduce unnecessary verbose RTS logs and enable them by default
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket improves verbose logging for Rollback to Stable (RTS), which is a persistence-layer component owned by Persistence.

---

## WT-7879: Investigate potential improvements of using atomics for cache configuration statistics
- **Team:** Storage Engines - Transactions
- **Reason:** Cache configuration statistics (cache_bytes_max) relate to the cache/eviction management subsystem, and investigating atomic operations for correctness in this area falls under cache/eviction management owned by Transactions.

---

## WT-7884: test_cursor_random failed due to key not set for insert operation
- **Team:** Storage Engines - Foundations
- **Reason:** This is a test build failure in a cursor-related test (`test_cursor_random`), which falls under cursors and correctness frameworks owned by Foundations.

---

## WT-7919: Write "Reconciliation" subpage for Architecture Guide
- **Team:** Storage Engines - Transactions
- **Reason:** Reconciliation (writing in-memory pages to disk) is part of B-tree reconciliation owned by Transactions, and this documentation task supports that subsystem.

---

## WT-7927: incr_backup test doesn't test variable- or fixed-length column store access methods
- **Team:** Storage Engines - Persistence
- **Reason:** This is a test coverage gap for the incremental backup test suite, and backup is owned by Persistence; the column store aspect is secondary to the backup testing focus.

---

## WT-7946: Create a Wiki page on flamescope
- **Team:** Storage Engines - Foundations
- **Reason:** Flamescope is a performance profiling/benchmarking tool, and creating documentation for it falls under performance benchmarking tools owned by Foundations.

---

## WT-7966: No need to handle lower isolation levels in reconciliation
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket removes dead code handling lower transaction isolation levels in reconciliation, which is directly in the B-tree reconciliation and transactions area owned by Transactions.

---

## WT-7969: Recovery failed trying to allocate a very large amount of memory
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket involves a failure in `__wt_txn_recover` during recovery when an unreasonable amount of memory is allocated, which is part of the logging/WAL and recovery subsystem owned by Persistence.

---

## WT-7976: Commit timestamp should be greater than latest active read timestamp
- **Team:** Storage Engines - Transactions
- **Reason:** This is a build failure related to timestamp validation in RTS (rollback to stable) during a transaction commit timestamp check, which is part of the transactions and RTS subsystems owned by Transactions.

---

## WT-7990: Rethink data handle statistics
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket proposes restructuring per-data-handle statistics to reduce memory usage, which is part of data handle management owned by Foundations.

---

## WT-7991: improve row/byte-count information in split-heavy workloads
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket improves row/byte-count tracking across B-tree page splits, which falls under B-tree operations (page splits, merges) and the WT_SESSION API, owned by Transactions.

---

## WT-8002: Brainstorm ideas to fix inconsistency in timestamp format between API and error output
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket addresses timestamp formatting inconsistency in the API and error messages, which is part of the transactions and timestamp management area owned by Transactions.

---

## WT-8008: Investigate long stalls in 5.0 compared to 4.4 with many collection test
- **Team:** Storage Engines - Transactions
- **Reason:** The investigation involves stalls with many collections likely related to checkpoint and eviction/cache pressure, which are part of cache/eviction management and checkpoints; the eviction/cache aspect places this primarily with Transactions.

---

## WT-8028: The many-collection-test does not run when a task is not configured
- **Team:** Storage Engines - Foundations
- **Reason:** This is a bug fix in the many-collection-test script, which is a performance/correctness testing framework tool owned by Foundations.

---

## WT-8031: Fix many-dhandles-stress.py for range partition
- **Team:** Storage Engines - Foundations
- **Reason:** This is a fix to a workgen/benchmark runner script (`many-dhandle-stress.py`), which falls under performance benchmarking tools owned by Foundations.

---

## WT-8037: Review coverage-report test and seek coverage improvement
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket reviews and improves the Evergreen coverage-report task, which is part of CI/CD infrastructure and correctness frameworks owned by Foundations.

---

## WT-8040: disallow direct modification of WT managed files
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket proposes preventing direct API/utility modification of internally-managed Btree files (bloom filters, index files), which falls under schema operations and API access control owned by Foundations.

---

## WT-8049: Bug in dumping stdout/stderr on error in unit testing on os x - cmake
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD/build infrastructure bug where stdout/stderr dumps don't work correctly in OS X CMake unit testing, falling under CI/CD infrastructure owned by Foundations.

---

## WT-8064: Investigate massive improvement in YCSB workload
- **Team:** Storage Engines - Foundations
- **Reason:** This is a performance benchmarking investigation into unexpected improvements in YCSB workload runs, which falls under performance benchmarking tools owned by Foundations.

---

## WT-8082: Architecture Guide updates for PM-2503
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort; without specific component context, this falls under Foundations as the team managing cross-cutting and release documentation.

---

## WT-8083: Architecture Guide updates for PM-2504
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8084: Architecture Guide updates for PM-2505
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8085: Architecture Guide updates for PM-2506
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8087: Architecture Guide updates for PM-2507
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8088: Architecture Guide updates for PM-2508
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8089: Architecture Guide updates for PM-2509
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8090: Architecture Guide updates for PM-2510
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8106: Fix prefix search near entries traversal statistics
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket fixes statistics for prefix search-near cursor operations, which falls under cursors (the CRUD layer) and their associated statistics, owned by Foundations.

---

## WT-8107: Separate next skip statistics from HS
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves cursor traversal statistics by separating history store increments from data file increments, which is part of cursor statistics owned by Foundations.

---

## WT-8145: Build guidelines in Wiki around descriptive commit messages
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code quality and release management process documentation task (commit message guidelines), which falls under release management and build system owned by Foundations.

---

## WT-8155: Statistic around count and duration of the files being checkpointed
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket adds statistics to track the number of files and duration within a checkpoint, which is directly part of the checkpoints subsystem owned by Persistence.

---

## WT-8165: Commit timestamp assertions didn't catch invalid timestamps in specific scenario
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket identifies a bug in commit timestamp validation assertions, which is part of the transactions and timestamp management area owned by Transactions.

---

## WT-8177: Verify lock protection around data structures
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket proposes formalizing lock verification around data structure accesses (using LLVM Thread Safety Analysis or dynamic annotations), which is a cross-cutting code quality and memory model improvement owned by Foundations.

---

## WT-8207: Add assert for excessive amounts of rollbacks in CPP framework
- **Team:** Storage Engines - Foundations
- **Reason:** This improves the cppsuite correctness testing framework by adding rollback tracking assertions, which falls under correctness frameworks owned by Foundations.

---

## WT-8215: Architecture Guide updates for PM-2564
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8229: Improve the logging under WT_VERB_TRANSACTION tag
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket improves verbose logging for the transactions subsystem, specifically for rollback and write conflict scenarios, which is owned by Transactions.

---

## WT-8231: Add dist script support for CMake formatting
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds CMake source formatting to the dist scripts (code style enforcement), which is part of the build system and code style tooling owned by Foundations.

---

## WT-8247: Add the missing compiler warnings for CPP files related to the cppsuite
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code quality and build system improvement to add compiler warnings for cppsuite C++ files, falling under build system and compile owned by Foundations.

---

## WT-8262: Make it default for most tests to generate statistics. Default stats to JSON
- **Team:** Storage Engines - Foundations
- **Reason:** This is a correctness/testing framework improvement to make statistics collection default and JSON-formatted across test suites, owned by Foundations.

---

## WT-8263: Enable compiler warnings for CPP files related to workgen
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build system and code quality task to enable compiler warnings for workgen C++ files, falling under build system and compile owned by Foundations.

---

## WT-8267: add table add and drop to format tester
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds table add/drop/rename and sweep server testing to the format tester, which is a correctness framework (format test) improvement owned by Foundations.

---

## WT-8276: Add cppsuite tests to the code coverage-report
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket extends the Evergreen coverage-report task with cppsuite tests, which is part of CI/CD infrastructure and correctness frameworks owned by Foundations.

---

## WT-8277: Change salvage to resolve prepared records
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket modifies salvage to commit prepared values found on salvaged pages, which is part of the salvage subsystem owned by Persistence.

---

## WT-8278: Change salvage to remove history store records
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket modifies salvage to discard potentially stale history store records for salvaged objects, which is part of the salvage subsystem owned by Persistence.

---

## WT-8279: Change salvage to merge history store records after salvage completes
- **Team:** Storage Engines - Persistence
- **Reason:** This is a follow-on to WT-8278 proposing an additional salvage pass to merge/compare history store records, which is part of the salvage subsystem owned by Persistence.

---

## WT-8305: Update eviction to check for weak hazard pointers and invalidate them when it attempts to evict a page
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket modifies eviction to handle weak hazard pointers before evicting a page, which is part of the cache/eviction management and B-tree operations (hazard pointers) owned by Transactions.

---

## WT-8307: Investigate management of hazard pointer array resizing
- **Team:** Storage Engines - Transactions
- **Reason:** Hazard pointer management (array resizing) is part of B-tree operations and eviction infrastructure owned by Transactions.

---

## WT-8308: Placeholder: Create follow on tickets for implementation
- **Team:** Storage Engines - Transactions
- **Reason:** This is a placeholder ticket in the hazard pointer / weak pointer series (WT-8305-8311), which falls under B-tree operations and eviction owned by Transactions.

---

## WT-8309: Add relevant statistics for hazard point resolution
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket adds statistics for weak/strong hazard pointer resolution and tracks uncommitted update resolution during commit/rollback, which is part of the transactions and eviction subsystems owned by Transactions.

---

## WT-8310: Investigate optimising hazard pointer storage for multiple keys on a single page updated by the same transaction
- **Team:** Storage Engines - Transactions
- **Reason:** This is an optimization investigation for hazard pointer storage related to transactions updating multiple keys on a single page, owned by Transactions.

---

## WT-8311: Validate impact on in memory storage engine
- **Team:** Storage Engines - Transactions
- **Reason:** This validation task is part of the hazard pointer/weak pointer series, examining the impact on the in-memory storage engine of eviction and transaction changes, owned by Transactions.

---

## WT-8334: Architecture Guide updates for PM-2631
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8432: Add version information to the WT checkpoint metadata
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket adds WiredTiger version metadata to checkpoint metadata to help with upgrade/downgrade analysis, which is part of the checkpoints and metadata subsystems owned by Persistence.

---

## WT-8445: Add VLCS/FLCS cases for test_checkpoint/recovery-test.sh
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket adds column store (VLCS/FLCS) test cases to the checkpoint/recovery test script, which is part of the checkpoints subsystem owned by Persistence.

---

## WT-8453: Enable cursor caching for cursors used to resolve uncommitted updates
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket is about cursor caching for cursors used during uncommitted update resolution at commit/rollback, which is part of the transactions subsystem owned by Transactions.

---

## WT-8458: Support JSON-encoded message strings for 'WT_CONNECTION::debug_info' messages
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the API-level `WT_CONNECTION::debug_info` interface to support JSON output formatting, which is part of the API (connections, configuration) and message handling owned by Foundations.

---

## WT-8469: Handle resolved updates getting evicted before commit/rollback finishes
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket addresses a race condition where resolved updates can be evicted before commit/rollback finishes, which is squarely in the transactions and eviction/cache management area owned by Transactions.

---

## WT-8471: Don't rollback resolve-search in case of being the oldest transaction pinning cache
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket prevents `__wt_txn_is_blocking` from rolling back transactions performing slow-path update resolution, which is part of transaction management and cache/eviction interaction owned by Transactions.

---

## WT-8492: Add a debug option to let the reconcile page to retain the time window of an update
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket adds a debug configuration option to control whether reconciliation retains time window values on page cells, which is part of B-tree reconciliation owned by Transactions.

---

## WT-8524: Create a python script that can diagnose the structure of btree from a coredump
- **Team:** Storage Engines - Transactions
- **Reason:** This diagnostic tool is specifically for inspecting in-memory B-tree structure (update lists, insert lists, pages) from a coredump, which is part of the in-memory B-tree format owned by Transactions.

---

## WT-8531: Add functionality to wiredtiger open verify metadata configuration that returns an error if the metadata is inconsistent
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds metadata consistency verification to the connection open configuration, which is part of metadata/schema table integrity and API configuration owned by Foundations.

---

## WT-8538: Reduce the amount of duplicated artifacts in Evergreen jobs
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD infrastructure improvement to reduce duplicated artifact uploads in Evergreen tasks, which is owned by Foundations.

---

## WT-8573: Update Architecture guide for configuration precompiling
- **Team:** Storage Engines - Foundations
- **Reason:** This documentation task updates the architecture guide for the configuration precompiling feature, which is part of the API and configuration subsystem documented and maintained by Foundations.

---

## WT-8582: Expand extent lists to collect GC information
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket modifies checkpoint extent lists (block allocation tracking structures) to support garbage collection information, which is part of the block manager and checkpoints subsystems owned by Persistence.

---

## WT-8612: Consider merging import compatibility test script into the main compatibility test script
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD and test infrastructure improvement to merge compatibility test scripts, which falls under CI/CD infrastructure and correctness frameworks owned by Foundations.

---

## WT-8628: Make a decision on test suites running with diag/non-diag builds
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD and build system policy decision about running test suites in diagnostic versus non-diagnostic modes, owned by Foundations.

---

## WT-8644: Preload failures leak cache blocks
- **Team:** Storage Engines - Persistence
- **Reason:** This bug involves cache block leaks when btree open preload fails, which touches the block manager and block cache layer owned by Persistence.

---

## WT-8681: Remove dead code handling WT versions earlier than 3.2.0
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket removes dead code around old log version constants (`WT_LOG_V3_VERSION`) that are no longer relevant, which is part of the logging/WAL subsystem owned by Persistence.

---

## WT-8685: Investigate how to have all the plots from the cpp suite tests on Atlas
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD and performance benchmarking tooling improvement to aggregate cppsuite test plots on a single Atlas page, owned by Foundations.

---

## WT-8729: the block cache code doesn't support object create
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket addresses a deficiency in the block cache layer where it bypasses object creation, which is part of the block cache and block manager owned by Persistence.

---

## WT-8738: Architecture Guide updates for PM-2710
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8739: Architecture Guide updates for PM-2711
- **Team:** Storage Engines - Foundations
- **Reason:** Architecture guide maintenance is a cross-cutting documentation effort managed by Foundations.

---

## WT-8744: Clean up handling of performance stats in cppsuite tests
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket cleans up performance statistics handling in the cppsuite correctness testing framework, which falls under correctness frameworks owned by Foundations.

---

## WT-8763: Logging and extension API improvements for storage sources
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket adds a separate verbose logging category for storage source extensions (tiered storage filesystem API), which is part of the filesystem API and tiered storage owned by Persistence.

---

## WT-8779: Investigate bounds for compression/decompression statistics
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket investigates anomalous compression ratio statistics in the block-level compression layer, which is part of the block manager and filesystem layer owned by Persistence.

---

## WT-8793: enhance logging-based testing
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket enhances logging-based testing (including backup and recovery tests) to better cover mixed logged/non-logged table scenarios, which is part of the logging/WAL subsystem owned by Persistence.

---

## WT-8800: Upgrade 3rdparty python test support libraries
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket updates third-party Python test support libraries used by the test suite, which falls under the correctness frameworks and build system owned by Foundations.

---

## WT-8808: Data validation failure in test_timestamp_abort
- **Team:** Storage Engines - Transactions
- **Reason:** This bug involves data validation failures in `test_timestamp_abort` after crash and recovery, relating to transaction timestamps and logging durability, with the primary concern being transaction/timestamp correctness owned by Transactions.

---

## WT-8810: enhance static test suite checkpoint tests
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket enhances checkpoint Python tests to run in both logged and non-logged modes, which is part of the checkpoints subsystem testing owned by Persistence.

---

## WT-8811: test_log04 enhancement
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about enhancing a Python test (test_log04.py) that smoke-tests logging with timestamp configurations, specifically adding a restart/recovery scenario to verify timestamp retention across log flush and reopen — directly a logging/WAL concern.

---

## WT-8813: Improve access to methods requiring an exclusive handle
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket concerns session/handle exclusivity for schema operations (related to WT-8695 and WT-7750) and data handle management — both are Foundations responsibilities covering API sessions and dhandle management.

---

## WT-8834: Automatically update parameter values in WT doxygen pages
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build/documentation infrastructure improvement to auto-generate config parameter values in docs from api_data.py, which falls under the build system and release management concerns of Foundations.

---

## WT-8881: It is possible to commit with a durable timestamp earlier than that of data read by the same transaction
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket is about transaction commit/durable timestamp consistency and tracking read timestamps within a transaction to enforce ordering — a core transaction ACID and timestamp visibility concern.

---

## WT-8916: Enable S3 extension build and test on the Windows
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about the build system, CI/CD, and cross-platform compilation for the S3 extension on Windows, which falls under Foundations' build system and CI/CD infrastructure responsibilities.

---

## WT-8937: Allow print_python_stack_trace.py to print traces for failing python tests on Windows
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD and testing infrastructure improvement for the Evergreen pipeline on Windows, which is owned by Foundations.

---

## WT-8974: Investigate naming/functionality of __wt_txn_publish_durable_timestamp
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket investigates the naming and behavior of a transaction timestamp publishing function — directly concerns transaction timestamp semantics and durable timestamp handling.

---

## WT-8976: Allow print_python_stack_trace.py to print traces for failing python tests on macOS
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD testing infrastructure improvement for Evergreen on macOS, falling under Foundations' responsibility for CI/CD pipelines and correctness frameworks.

---

## WT-8977: Tiered Storage python tests shouldn't check contents of dir_store cache
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about fixing tiered storage test correctness — removing improper coupling between WiredTiger API-level tests and storage_source cache internals — which is a test correctness and API boundary concern under Foundations.

---

## WT-9023: Create a cpp test for prepared updates
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket is about creating a stress test specifically for prepared transactions under eviction pressure, validating that uncommitted prepared updates are correctly handled — a prepared transactions and cache/eviction correctness concern.

---

## WT-9042: commit/durability timestamps can race, perform potentially unnecessary checks
- **Team:** Storage Engines - Transactions
- **Reason:** This is a bug about races in the commit/durability timestamp validation code and set_timestamp, which involves transaction timestamp semantics and global transaction state management.

---

## WT-9043: commit/prepare timestamp checks against read timestamps are only done in #diagnostic builds
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket is about improving commit and prepare timestamp validation against active read timestamps — a transaction correctness and timestamp ordering concern.

---

## WT-9066: format uses all_durable to set the stable timestamp
- **Team:** Storage Engines - Foundations
- **Reason:** This is a bug in the test/format correctness framework regarding how the stable timestamp is set, which is part of Foundations' correctness frameworks (format test) responsibility.

---

## WT-9113: More efficient cell encoding when adjacent keys differ by incrementing last byte by 1
- **Team:** Storage Engines - Transactions
- **Reason:** This improvement concerns the in-memory B-tree cell encoding format for adjacent keys on pages, which falls under the in-memory B-tree format and page data structures owned by Transactions.

---

## WT-9145: Add donor_stable_timestamp in WT_SESSION::create(import=())
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket involves running RTS (Rollback to Stable) on an imported table using a donor stable timestamp, which is a combined import/RTS operation — RTS is owned by Persistence.

---

## WT-9148: Investigate the use of HWASAN instead of ASAN
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD and developer tooling improvement to investigate hardware-assisted address sanitizer for testing, which falls under Foundations' CI/CD infrastructure and build system responsibilities.

---

## WT-9170: Shutdown RTS skips trees that have never been checkpointed
- **Team:** Storage Engines - Persistence
- **Reason:** This bug is about shutdown-time RTS incorrectly skipping trees that have never been checkpointed — a Rollback to Stable (RTS) behavior issue owned by Persistence.

---

## WT-9172: remove force configuration for WT_CONNECTION.set_timestamp API
- **Team:** Storage Engines - Transactions
- **Reason:** This is about removing an undocumented `force` configuration from the `WT_CONNECTION::set_timestamp` API, which is a transaction timestamp API cleanup concern.

---

## WT-9178: Remove has_XXX booleans from the global transaction state
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket proposes eliminating redundant boolean flags from the `WT_TXN_GLOBAL` structure in favor of sentinel timestamp values, which is a transaction global state and performance improvement.

---

## WT-9182: Explore what should be the correct way to calculate upd_memsize in the durable history era
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket investigates the calculation of update chain memory size used in reconciliation page-split decisions, which directly involves B-tree reconciliation and update chain management.

---

## WT-9187: Create consistent per-session I/O statistics
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket requests new per-session I/O statistics at the API level, which falls under the sessions and API domain of Foundations.

---

## WT-9193: test_gc02 WT_ROLLBACK: conflict between concurrent operations
- **Team:** Storage Engines - Transactions
- **Reason:** This is a build failure in test_gc02 involving `WT_ROLLBACK` errors from concurrent operations, which is a transaction conflict and concurrency issue.

---

## WT-9198: Improve the way we update the stable timestamp in the cpp suite
- **Team:** Storage Engines - Foundations
- **Reason:** This improvement is about making the cppsuite correctness framework update the stable timestamp more carefully to avoid setting it beyond running transactions' commit timestamps — a test framework improvement owned by Foundations.

---

## WT-9245: Too many logs from compatibility_test_for_releases.sh
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD and testing infrastructure cleanup ticket about reducing noisy recovery debug logs in the compatibility test script, under Foundations' release management and CI/CD responsibilities.

---

## WT-9269: failed: test_config11 assertion error on macos-1014
- **Team:** Storage Engines - Transactions
- **Reason:** This build failure involves a cache usage assertion in test_config11 (checking current_cache_usage > max_cache_size / 2), which concerns cache/eviction management behavior.

---

## WT-9270: Ideas to improve the code style/flexibility of the cpp suite
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket collects improvements to the cppsuite correctness framework code style, which is part of Foundations' responsibility for correctness frameworks and stress tests.

---

## WT-9285: tree walk code locks deleted WT_REFs twice
- **Team:** Storage Engines - Transactions
- **Reason:** This is a B-tree operation improvement — removing redundant double-locking of fast-truncate WT_REF structures during tree walks, which falls under B-tree operations and hazard pointers.

---

## WT-9286: Enhance existing GDB functions to be consistent
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves GDB debugging scripts in the WiredTiger repository, which is a developer tooling and build system concern owned by Foundations.

---

## WT-9288: Include external functions in GDB auto loading
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about improving developer productivity by auto-loading external GDB functions, which is a tooling and build infrastructure concern under Foundations.

---

## WT-9294: Understand performance of cursor creation
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket investigates cursor creation performance and config string parsing, which is a cursor API and session management performance concern owned by Foundations.

---

## WT-9330: Add observability on the last thread that accessed a session
- **Team:** Storage Engines - Foundations
- **Reason:** This improvement adds session-to-thread mapping observability for diagnostics, which is a session API and supportability concern under Foundations.

---

## WT-9333: Clean up overloaded functions in the CppSuite
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the cppsuite correctness framework by cleaning up overloaded default operation implementations, which is part of Foundations' correctness frameworks responsibility.

---

## WT-9346: format "cp: Argument list too long" failure
- **Team:** Storage Engines - Foundations
- **Reason:** This is a bug in the test/format correctness framework where copying log files for salvage replay fails when there are too many files, a format test infrastructure issue owned by Foundations.

---

## WT-9352: Improve eviction performance during RTS
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket addresses eviction performance specifically during RTS execution — while eviction is generally a Transactions concern, this improvement is about RTS (Rollback to Stable) tuning its cache behavior, and RTS is owned by Persistence.

---

## WT-9375: Update Windows Evergreen testing to generate dump files
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket is about CI/CD Evergreen testing infrastructure on Windows to generate mini-dumps on crashes, which is a CI/CD pipeline concern owned by Foundations.

---

## WT-9386: Review and update the namespaces and naming conventions in the cppsuite
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves namespace conventions in the cppsuite correctness framework, which falls under Foundations' responsibility for correctness frameworks and code style.

---

## WT-9387: Rename classes, methods, instance variables using OOP conventions
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code quality and style improvement for the cppsuite framework — renaming classes, methods, and variables to follow Google C++ style guide, which is a Foundations code style concern.

---

## WT-9390: Code quality improvements in the cppsuite: Part two
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket makes code quality improvements to the cppsuite correctness framework (early exits, macro review, type deduction), which is owned by Foundations.

---

## WT-9391: Replace WiredTiger macros with C++ functions in cppsuite
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket removes tight coupling between cppsuite test code and WiredTiger internal macros, which is a correctness framework code quality improvement owned by Foundations.

---

## WT-9399: Code quality improvements in the cppsuite: Part three
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket makes further code quality improvements to the cppsuite correctness framework (logging, checkpoint thread defaults), which is owned by Foundations.

---

## WT-9403: Add more format stress tests to run for less time in Evergreen
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds shorter-duration format stress test variants to the Evergreen CI pipeline, which falls under Foundations' correctness frameworks (format test) and CI/CD infrastructure.

---

## WT-9414: Add information about ref state transitions to developer docs
- **Team:** Storage Engines - Transactions
- **Reason:** This documentation ticket captures WT_REF state transitions and allowed transitions — directly related to the B-tree page lifecycle and ref management owned by Transactions.

---

## WT-9419: Replace the WiredTiger PRNG with an xoshiro variant
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket replaces the internal PRNG implementation in WiredTiger with a better algorithm (xoshiro128**), which is a cross-cutting code quality and memory model improvement owned by Foundations.

---

## WT-9443: Implement a basic repeatable reads checker in the CppSuite
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds a snapshot isolation validation feature to the cppsuite test framework, which is a correctness framework improvement owned by Foundations.

---

## WT-9444: Enhance flags.py to generate the flags variable declaration
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the flags.py code generation script to control variable declarations, which is a build system and code generation tool improvement owned by Foundations.

---

## WT-9449: Add a stage to the "upload artifact" function in evergreen.yml to fail the test if the artifact is too big
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the Evergreen CI pipeline by adding artifact size checks, which is a CI/CD infrastructure concern owned by Foundations.

---

## WT-9451: Test to demonstrate append only workloads
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket creates a cppsuite stress test simulating append-only workloads (similar to MongoDB oplog), which falls under Foundations' correctness frameworks and stress tests.

---

## WT-9460: Documentation updates for PM-2942
- **Team:** Storage Engines - Foundations
- **Reason:** This is a general documentation update ticket (API Guide, Programming Guide, Architecture Guide) without a specific component, which falls under Foundations' cross-cutting documentation and release management responsibilities.

---

## WT-9461: Documentation updates for PM-2943
- **Team:** Storage Engines - Foundations
- **Reason:** This is a general documentation update ticket (API Guide, Programming Guide, Architecture Guide) without a specific component, which falls under Foundations' documentation and release management responsibilities.

---

## WT-9464: Documentation updates for PM-2947
- **Team:** Storage Engines - Foundations
- **Reason:** This is a general documentation update ticket (API Guide, Programming Guide, Architecture Guide) without a specific component, which falls under Foundations' documentation and release management responsibilities.

---

## WT-9469: CppSuite: Tune search_near_01 stress to make it more stressful on the new variant
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket tunes a cppsuite stress test workload for search_near operations, which falls under Foundations' responsibility for correctness frameworks and stress tests.

---

## WT-9470: CppSuite: Tune search_near_02 stress to make it more stressful on the new variant
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket tunes a cppsuite stress test workload for search_near operations, which falls under Foundations' responsibility for correctness frameworks and stress tests.

---

## WT-9471: CppSuite: Tune search_near_03 stress to make it more stressful on the new variant
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket tunes a cppsuite stress test workload for search_near operations, which falls under Foundations' responsibility for correctness frameworks and stress tests.

---

## WT-9478: Extension libraries should be installed in a (versioned) subdir
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the CMake build system to install extension plugin libraries into versioned subdirectories, which is a build system and release management concern owned by Foundations.

---

## WT-9482: Document how page splits work in WiredTiger
- **Team:** Storage Engines - Transactions
- **Reason:** This documentation ticket describes how B-tree page splits work in WiredTiger — page splits are a B-tree operation owned by Transactions.

---

## WT-9496: Generate documentation front page with correct current and previous versions
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket automates the documentation release workflow to correctly update version numbers, which is a release management and build system concern owned by Foundations.

---

## WT-9498: Move the documentation build into the CMake build directory
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket moves the WiredTiger documentation build into the standard CMake build directory, which is a build system improvement owned by Foundations.

---

## WT-9499: Identify storage HW corruption from extensions
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about identifying and logging hardware corruption errors (checksum errors) from compression and encryption extensions — which concerns the filesystem API and block-level data integrity owned by Persistence.

---

## WT-9517: Randomise the collections a thread is assigned to in the insert operation of the cppsuite
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves randomness in the cppsuite correctness framework's default insert operation, which is a stress test framework improvement owned by Foundations.

---

## WT-9519: Use random cursors in the update operation of the cppsuite
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the cppsuite framework's update operation to use random cursors instead of key count generation, which is a correctness framework improvement owned by Foundations.

---

## WT-9520: Randomise read logic in the cppsuite
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the cppsuite framework's read operation by randomizing cursor positioning, which is a correctness framework improvement owned by Foundations.

---

## WT-9523: Decouple the cppsuite code related to the timestamp manager
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket refactors the timestamp manager component in the cppsuite test framework to be less coupled, which is a correctness framework code quality improvement owned by Foundations.

---

## WT-9524: Revisit the purpose of the workload manager of the cpp suite
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket evaluates and potentially refactors the cppsuite workload manager component, which is a correctness framework architecture concern owned by Foundations.

---

## WT-9531: Documentation updates for PM-2958
- **Team:** Storage Engines - Foundations
- **Reason:** This is a general documentation update ticket (API Guide, Programming Guide, Architecture Guide) without a specific component, which falls under Foundations' documentation and release management responsibilities.

---

## WT-9532: Documentation updates for PM-2959
- **Team:** Storage Engines - Foundations
- **Reason:** This is a general documentation update ticket (API Guide, Programming Guide, Architecture Guide) without a specific component, which falls under Foundations' documentation and release management responsibilities.

---

## WT-9540: Add API to get the durable timestamp of an associated write
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket requests a new cursor API to expose the durable timestamp of positioned values or the max durable timestamp within a transaction — a transaction timestamp visibility API concern.

---

## WT-9543: Rename the database operations of the cppsuite
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket renames cppsuite operation functions to better reflect their transaction-lifecycle semantics, which is a correctness framework code quality improvement owned by Foundations.

---

## WT-9548: Better RAII in unit tests
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves RAII patterns in WiredTiger unit tests to ensure proper resource cleanup, which is a correctness framework code quality improvement owned by Foundations.

---

## WT-9560: Remove forward compatibility checking on open
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket removes the forward compatibility version check in `wiredtiger_open` and clarifies the versioning/compatibility scheme, which is a connections/API and release management concern owned by Foundations.

---

## WT-9565: Review API coverage in test-format
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket reviews and extends test/format coverage for WiredTiger API features, which is a correctness framework (format test) improvement owned by Foundations.

---

## WT-9574: Documentation updates for PM-2975
- **Team:** Storage Engines - Foundations
- **Reason:** This is a general documentation update ticket (API Guide, Programming Guide, Architecture Guide) without a specific component, which falls under Foundations' documentation and release management responsibilities.

---

## WT-9585: Clean up unused imports and main function in python tests
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket cleans up Python test code style (unused imports, main function consistency), which is a code quality improvement for the correctness frameworks owned by Foundations.

---

## WT-9586: Python test rollback failures with retries should be more obvious in the output log
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the observability of rollback failures in Python test output, which is a CI/CD and correctness framework usability improvement owned by Foundations.

---

## WT-9597: Teach cmake/ninja about api_config
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the CMake/ninja build system to automatically regenerate API config files when api_config.py changes, which is a build system improvement owned by Foundations.

---

## WT-9598: Teach cmake/ninja about s_stats
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the CMake/ninja build system to automatically re-run s_stat when stats definitions change, which is a build system improvement owned by Foundations.

---

## WT-9613: Make alter transactional with updates to other tables
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket makes the `alter` schema operation transactional with respect to other table updates, which concerns schema operations (alter, metadata integrity) and transactions — primarily a schema/metadata concern owned by Foundations.

---

## WT-9615: Create data structures for the fail points
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket creates the basic WT_FAIL_POINT data structures for a diagnostic fail point infrastructure, which is a cross-cutting correctness/testing framework concern owned by Foundations.

---

## WT-9616: Create a header file where the fail point data structures should live
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket creates the build infrastructure (header file, CMake integration) for the fail point system, which is a build system and correctness framework concern owned by Foundations.

---

## WT-9617: Add the fail point apis with empty implementation
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds the fail point API skeleton implementation (diagnostic vs. production build variants), which is a correctness framework and API infrastructure concern owned by Foundations.

---

## WT-9618: Define fail point apis skeleton in api_data.py
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds fail point API definitions to api_data.py and integrates them into the s_all build process, which is a build system and API infrastructure concern owned by Foundations.

---

## WT-9619: Write a python script to generate the mapping for each defined fail point to a unique number
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket creates a code generation script (run in s_all) to map fail points to unique numbers, which is a build system and code generation tool improvement owned by Foundations.

---

## WT-9620: Create a c implementation file where the evaluation functions and init functions live
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket creates the C implementation file for fail point evaluation and initialization functions with CMake build integration, which is a build system and correctness framework concern owned by Foundations.

---

## WT-9658: Add visible statistics for s3_store module
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds statistics tracking for S3 storage source requests (visible via statistics cursors), which is an API and statistics infrastructure concern crossing tiered storage and Foundations' sessions/API domain.

---

## WT-9665: Project suggestion: Implement a WiredTiger b-tree visualizer
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket proposes a B-tree visualization tool for debugging and education, which primarily concerns the B-tree in-memory format and page structures owned by Transactions.

---

## WT-9668: Improve overlap detection in WT_MODIFY paths
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket improves the fast-path overlap detection in WT_MODIFY application to avoid unnecessary materialization of intermediate modifications — a concern of in-memory update chain and B-tree data structure handling owned by Transactions.

---

## WT-9671: Investigate extremely varied checkpoint cleanup statistic in CppSuite test hs_cleanup
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket investigates variance in the `cc_pages_removed` (checkpoint cleanup pages removed) statistic in the hs_cleanup test, which concerns checkpoint cleanup and history store behavior owned by Transactions.

---

## WT-9699: Spike: Investigate a solution to better pass keys and recno's within WiredTiger
- **Team:** Storage Engines - Transactions
- **Reason:** This investigation ticket examines the inconsistent use of key/recno fields in cursors and CBT structures during B-tree operations, which is an in-memory B-tree and cursor data structure concern owned by Transactions.

---

## WT-9708: WT_REF::flags should never change from WT_REF_FLAG_INTERNAL to WT_REF_FLAG_LEAF or vice versa
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket ensures WT_REF flag immutability between internal and leaf page types, which concerns B-tree page structures, WT_REF state management, and race condition prevention owned by Transactions.

---

## WT-9719: CppSuite tests that call try_rollback aren't honoring the configured op_count
- **Team:** Storage Engines - Foundations
- **Reason:** This bug in the cppsuite test framework causes transactions to roll back too early, reducing workload stressfulness — a correctness framework behavior fix owned by Foundations.

---

## WT-9731: Add test case encouraging race conditions on collection creation
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds a stress test to expose races between collection creation and concurrent checkpoint operations, which is a correctness framework (stress test) improvement owned by Foundations.

---

## WT-9754: Add testing coverage for newer versions of GCC and Clang
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds CI/CD Evergreen testing coverage for newer GCC and Clang compiler versions, which is a build system and CI/CD infrastructure concern owned by Foundations.

---

## WT-9780: Rationalize and tidy ref locking
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket rationalizes WT_REF lock/unlock macro usage and adds a WT_REF_TRYLOCK macro with proper memory barriers — directly a B-tree page ref locking and memory model concern owned by Transactions.

---

## WT-9784: WiredTiger cache stuck logic abort transactions (sometimes) that are not blocking the eviction
- **Team:** Storage Engines - Transactions
- **Reason:** This improvement addresses the eviction cache-stuck logic that incorrectly rolls back transactions that are not actually blocking eviction — a cache/eviction management and transaction interaction concern owned by Transactions.

---

## WT-9798: Fill in function TODO comments
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket fills in missing function header comments across the codebase (generated by function.py in WT-8274), which is a code quality and documentation improvement spanning all teams but best owned by Foundations as a cross-cutting improvement.

---

## WT-9800: Enhance bulk load to support multiple threads
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket enhances the bulk load cursor API to support multiple concurrent threads, which is a cursor/API capability improvement owned by Foundations.

---

## WT-9808: Fix suite_subprocess.runWt for tiered storage
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket fixes the test suite subprocess infrastructure for tiered storage tests (runWt compatibility), which is a correctness framework and test infrastructure concern owned by Foundations.

---

## WT-9810: Create a test application that stresses truncate and checkpoint
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket requests a new correctness/stress test (CPP suite) covering truncate and checkpoint interactions, which falls under correctness frameworks and stress tests owned by Foundations.

---

## WT-9858: Add custom data source test to WiredTiger
- **Team:** Storage Engines - Foundations
- **Reason:** This is about adding a csuite correctness test for custom data source cursors, which falls under correctness frameworks and cursor/API testing owned by Foundations.

---

## WT-9859: Deduplicate the page skip code for deleted pages
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket is about deduplicating and cleaning up page skip logic during B-tree walk (cursor_next/prev and wt_delete_page_skip), which involves B-tree operations and in-memory page structures owned by Transactions.

---

## WT-9875: Index cursor CRUD functions do not check if the key operation is set
- **Team:** Storage Engines - Foundations
- **Reason:** This is a bug in index cursor CRUD operations (cur_index.c) where key-set checks are missing, directly involving the cursor layer between MongoDB and the B-tree, owned by Foundations.

---

## WT-9880: Create a hierarchy for incompatible settings in test/format
- **Team:** Storage Engines - Foundations
- **Reason:** This is a test/format improvement task to document and enforce incompatible configuration settings, which falls under the correctness frameworks (format test) owned by Foundations.

---

## WT-9883: test/format table_ops positioned variable doesn't reflect the cursor position properly
- **Team:** Storage Engines - Foundations
- **Reason:** This is a bug in the test/format correctness framework involving cursor positioning state management, which falls under correctness frameworks owned by Foundations.

---

## WT-9884: Remove the default (read) timestamp used in session.query_timestamp()
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket is about changing the timestamp API behavior for session.query_timestamp(), which is part of the transactions/timestamp subsystem owned by Transactions.

---

## WT-9887: Error path testing for statistics handler on open/close
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket requests a C-suite test for error paths in the event handler API during wiredtiger_open/close, which involves API correctness testing owned by Foundations.

---

## WT-9929: Investigate the generation of traces when the IOPS are getting slow
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cross-cutting supportability/tracing investigation about slow I/O detection, which spans multiple components and fits under cross-cutting systemic improvements owned by Foundations.

---

## WT-9941: Spike to improve unit test coverage
- **Team:** Storage Engines - Foundations
- **Reason:** This is a general unit test coverage improvement initiative with no specific component, which falls under correctness frameworks and testing infrastructure owned by Foundations.

---

## WT-9949: Set core file pattern on static hosts at beginning of Evergreen tasks
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD infrastructure improvement for Evergreen static hosts to ensure core files land in the correct location, owned by Foundations.

---

## WT-9951: Add flexibility to the format-failure-configs-test task
- **Team:** Storage Engines - Foundations
- **Reason:** This is an Evergreen pipeline/CI improvement for the format-failure-configs-test task, which falls under CI/CD infrastructure and correctness frameworks owned by Foundations.

---

## WT-9952: Assess correct usage of setting multiple commit timestamps
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket addresses enforcing constraints on setting multiple commit timestamps in transactions, which is core transaction timestamp logic owned by Transactions.

---

## WT-9962: Add contributors information to the WiredTiger repository
- **Team:** Storage Engines - Foundations
- **Reason:** This is about improving open-source contributor documentation and README files, which falls under build system/release management and code style owned by Foundations.

---

## WT-9972: Understand why s-clang_tidy is no longer used
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build system/lint tooling investigation about whether s_clang-tidy should be integrated into the build pipeline, owned by Foundations.

---

## WT-9976: Update clang_format to enforce with C++ coding guidelines
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build system and code style enforcement task for C++ formatting, which falls under build system, lint, and code style owned by Foundations.

---

## WT-9977: Update Workgen with the new C++ coding guidelines
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code style update to the Workgen performance benchmarking tool to comply with C++ guidelines, owned by Foundations under benchmarking tools and code style.

---

## WT-9978: Update the S3 extension with the new C++ coding guidelines
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code style update to the S3 storage extension for C++ guidelines compliance, falling under build system and code style owned by Foundations.

---

## WT-9979: Update the timestamp simulator with the new C++ coding guidelines
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code style update to the timestamp simulator tool for C++ guidelines compliance, falling under code style and build system owned by Foundations.

---

## WT-9980: Update the cppsuite with the new C++ coding guidelines
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code style update to the cppsuite stress test framework for C++ guidelines compliance, owned by Foundations under correctness frameworks and code style.

---

## WT-9981: Update the cpp unit tests with the new C++ coding guidelines
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code style update to the C++ unit tests for compliance with C++ coding guidelines, owned by Foundations under correctness frameworks and code style.

---

## WT-9986: Fix JSON cursor bug triggered by allocator changes
- **Team:** Storage Engines - Foundations
- **Reason:** This is a bug fix in the JSON cursor code where memory reallocated without clearing causes incorrect behavior, involving cursor layer bug fixes owned by Foundations.

---

## WT-10006: Catch2 fails to build and raises "raising cygheap base mismatch detected" on Windows
- **Team:** Storage Engines - Foundations
- **Reason:** This is a Windows build system failure related to the Catch2 test framework, which falls under build system and CI/CD infrastructure owned by Foundations.

---

## WT-10028: Allow changing block allocation algorithm with WT_SESSION::alter
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about extending the block manager's block allocation algorithm to be changeable at runtime via WT_SESSION::alter, which is a block manager feature owned by Persistence.

---

## WT-10034: Ensure wt can be built on all the available distros
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build system improvement to ensure WiredTiger compiles on all supported Linux distributions, owned by Foundations under build system and compile.

---

## WT-10045: Update WT_ASSERT to take a failure_reason string
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cross-cutting code quality improvement to the WT_ASSERT macro used throughout the codebase, falling under cross-cutting systemic improvements and code style owned by Foundations.

---

## WT-10048: Add operation tracking support for truncate operation in cpp test framework
- **Team:** Storage Engines - Foundations
- **Reason:** This is an enhancement to the CPP stress test framework to track and validate truncate operations, which falls under correctness frameworks owned by Foundations.

---

## WT-10079: Automate the Python compatibility test update step for cutting WT releases
- **Team:** Storage Engines - Foundations
- **Reason:** This is a release management automation task for updating Python compatibility tests during open-source release cuts, owned by Foundations under release management.

---

## WT-10099: Establish performance metrics to monitor workload rates over time
- **Team:** Storage Engines - Foundations
- **Reason:** This is a performance benchmarking initiative to establish metrics and monitoring for workload rates, which falls under performance benchmarking tools owned by Foundations.

---

## WT-10100: Update the WiredTiger test triage wiki
- **Team:** Storage Engines - Foundations
- **Reason:** This is a documentation update for the test triage wiki page related to the test monitoring framework, owned by Foundations under CI/CD and correctness frameworks.

---

## WT-10121: Improve the testing around standalone and non-standalone
- **Team:** Storage Engines - Foundations
- **Reason:** This is a test coverage improvement task for standalone vs. non-standalone configurations, falling under CI/CD infrastructure and correctness frameworks owned by Foundations.

---

## WT-10130: Review wtperf_run.sh for removal
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cleanup task for legacy wtperf benchmark runner scripts, which falls under performance benchmarking tools (wtperf) owned by Foundations.

---

## WT-10145: Enable 'page_stats_2022' flag in standalone builds
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build configuration task to enable a statistics feature flag in standalone builds, falling under build system and cross-cutting improvements owned by Foundations.

---

## WT-10154: Improve public wiki description of good Jira ticket content
- **Team:** Storage Engines - Foundations
- **Reason:** This is a process/documentation improvement for Jira ticket quality guidance, which is a general engineering process task owned by Foundations under cross-cutting improvements.

---

## WT-10156: Upgrade/downgrade testing for record count
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket adds upgrade/downgrade compatibility testing for the record count feature, which falls under correctness frameworks and metadata/schema integrity owned by Foundations.

---

## WT-10158: Add test for record count with only updates
- **Team:** Storage Engines - Foundations
- **Reason:** This is a test addition for the record count feature covering update-only scenarios, which falls under correctness frameworks owned by Foundations.

---

## WT-10177: Automate updates to documentation landing page to support release
- **Team:** Storage Engines - Foundations
- **Reason:** This is a release management automation task for updating the documentation landing page during releases, owned by Foundations under release management.

---

## WT-10182: Add configuration to s3_store to turn off file caching
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about adding configuration to disable file-based caching in the S3 storage source extension, which relates to the filesystem API and block cache layer owned by Persistence.

---

## WT-10200: Consider removing deleted ref cleanup during checkpoint
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket analyzes the safety of freeing deleted WT_REF structures during checkpoint splits, which involves B-tree split/merge operations and checkpoint interactions owned by Transactions.

---

## WT-10208: Consider ways to free statistics array for dormant data handles
- **Team:** Storage Engines - Foundations
- **Reason:** This is an improvement to free statistics memory from dormant data handles during dhandle sweep, which directly involves data handle management (dhandle cache) owned by Foundations.

---

## WT-10210: Create a way to remove obsolete config fields from WT metadata
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket addresses removing obsolete configuration fields from WT metadata to support agile development and upgrade/downgrade, which involves metadata/schema table integrity owned by Foundations.

---

## WT-10222: Track and Evaluate Pull Request building time
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD infrastructure and developer productivity improvement to track PR build times, owned by Foundations under CI/CD infrastructure and Evergreen pipelines.

---

## WT-10224: Create a common testing runner/framework
- **Team:** Storage Engines - Foundations
- **Reason:** This is an infrastructure improvement to create a unified test runner across all WT testing frameworks, owned by Foundations under correctness frameworks and CI/CD.

---

## WT-10227: Unnecessary deleted page instantiations
- **Team:** Storage Engines - Transactions
- **Reason:** This is an optimization to avoid instantiating deleted pages during cursor search by detecting deletion and returning WT_NOTFOUND early, which involves B-tree operations and page management owned by Transactions.

---

## WT-10228: Terminology reform for "proxy cell"
- **Team:** Storage Engines - Transactions
- **Reason:** This is a code clarity improvement renaming "proxy cells" (used in fast-truncate) to "deleted cells" in rec_child.c, which involves in-memory B-tree format and reconciliation terminology owned by Transactions.

---

## WT-10244: Unresolved issue in many-dhandle-stress.py
- **Team:** Storage Engines - Foundations
- **Reason:** This is a technical debt item in the Workgen stress test (many-dhandle-stress.py) related to range_partition support, falling under correctness frameworks and performance benchmarking tools owned by Foundations.

---

## WT-10252: Define a Workgen operation that can insert/update random k/v pairs of random sizes
- **Team:** Storage Engines - Foundations
- **Reason:** This is an enhancement to the Workgen performance benchmarking tool to support random-sized key/value pairs in operations, owned by Foundations under performance benchmarking tools.

---

## WT-10280: More detailed statistics for RTS
- **Team:** Storage Engines - Persistence
- **Reason:** This is an improvement to add more detailed statistics and logging for Rollback to Stable (RTS) operations, which is owned by Persistence under RTS.

---

## WT-10282: "Debug" optimisation level not applied to MSAN builds
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build system bug where debug optimization flags are not correctly applied to MSAN builds, falling under build system and compile owned by Foundations.

---

## WT-10308: Missing test cases in packing-test.c
- **Team:** Storage Engines - Foundations
- **Reason:** This is a task to add missing test cases for WT_ITEM packing in the packing-test.c csuite test, falling under correctness frameworks owned by Foundations.

---

## WT-10313: Create auto test test/format config script
- **Team:** Storage Engines - Foundations
- **Reason:** This is a scripting/automation task for the test/format correctness framework to find the best reproducer config from BFG failures, owned by Foundations under correctness frameworks.

---

## WT-10322: Investigate refactoring common functionality in WT storage source extensions
- **Team:** Storage Engines - Persistence
- **Reason:** This is an investigation into refactoring shared code across cloud storage source extensions (S3, Azure, GCP), which involves the filesystem API and storage source extensions owned by Persistence.

---

## WT-10337: Add basic cache read tracing
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket adds tracing/verbose logging for every page read into cache to support cache modeling analysis, which involves cache/eviction management and page read operations owned by Transactions.

---

## WT-10339: Improve tests, benchmarks to emulate session pooling
- **Team:** Storage Engines - Foundations
- **Reason:** This is an improvement to wtperf/workgen benchmarks to emulate MongoDB-style session pooling, falling under performance benchmarking tools and sessions/API owned by Foundations.

---

## WT-10388: Investigate tools to check shell code portability
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build system and code style investigation into tools (like shellcheck) for ensuring shell script portability, owned by Foundations under build system and lint.

---

## WT-10396: Use stat cursor instead of a separate api to retrieve the record count
- **Team:** Storage Engines - Foundations
- **Reason:** This is a design decision to use the existing statistics cursor API rather than a new API for record count retrieval, which involves the API layer and cursors owned by Foundations.

---

## WT-10427: Investigate the cause of cursor_copy causing failures in test/format
- **Team:** Storage Engines - Foundations
- **Reason:** This is a test/format correctness framework investigation into cursor_copy debug mode configuration causing failures, owned by Foundations under correctness frameworks.

---

## WT-10454: Review FIXMEs and their associated tickets
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cross-cutting code quality task to review and validate all FIXME comments in the codebase, falling under cross-cutting systemic improvements and code style owned by Foundations.

---

## WT-10455: Cleanup TODOs in the WiredTiger codebase
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cross-cutting code quality task to remove or replace TODO comments with proper FIXME/ticket references throughout the codebase, owned by Foundations under code style.

---

## WT-10457: Modify data format to support statistics cursor for byte and record counts
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket involves data format changes to persist record count and byte size statistics in files, which relates to on-disk block/file format owned by Persistence.

---

## WT-10470: Review benchmarks and hardware used for automated performance testing
- **Team:** Storage Engines - Foundations
- **Reason:** This is a review of the hardware and workloads used in automated performance testing via Evergreen, owned by Foundations under performance benchmarking tools and CI/CD infrastructure.

---

## WT-10477: Include page_del_committed in visibility check for page_del structures
- **Team:** Storage Engines - Transactions
- **Reason:** This is a visibility check improvement for page_del structures involving transaction visibility logic for fast-truncated pages, owned by Transactions.

---

## WT-10481: Change WT_STAT_NONE to use max uint64 and change stats to use uint64_t
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cross-cutting code quality change to the statistics type definitions used throughout the codebase, falling under cross-cutting systemic improvements owned by Foundations.

---

## WT-10484: Verify WiredTiger versions 11.0 and 10.0 are running on Windows
- **Team:** Storage Engines - Foundations
- **Reason:** This is a release management and build system task to verify WiredTiger installation on Windows via PyPI, owned by Foundations under release management and build system.

---

## WT-10554: Make Windows build process/documentation better
- **Team:** Storage Engines - Foundations
- **Reason:** This is a documentation and build system improvement for the Windows build process, owned by Foundations under build system and compile.

---

## WT-10612: Add a new WT_TIME_POINT structure to hold transaction id and both commit and durable timestamps
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket creates a new WT_TIME_POINT structure to simplify passing transaction visibility parameters, which is a core transaction data structure improvement owned by Transactions.

---

## WT-10634: Documentation and test changes corresponding to bulk operations
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket involves creating tests and updating documentation around bulk cursor behavior with single-file checkpoints, which relates to checkpoint and cursor behaviors owned by Persistence.

---

## WT-10639: Investigate the tests left behind in random_directio
- **Team:** Storage Engines - Foundations
- **Reason:** This is a technical debt investigation into commented-out schema stress test scenarios in the random_directio test, falling under correctness frameworks and schema operations owned by Foundations.

---

## WT-10641: Explore adding statistics for pages requested and read in cache by application threads
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket proposes adding per-application-thread cache hit/miss statistics for pages read into cache, which involves cache/eviction statistics owned by Transactions.

---

## WT-10651: Investigate methods to install google cloud dependencies on evergreen machines
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD and build system investigation for installing Google Cloud dependencies on Evergreen hosts, owned by Foundations under CI/CD infrastructure and build system.

---

## WT-10668: Investigate what diagnostic correctness checking could be added to the skip list and other lock free data structures
- **Team:** Storage Engines - Foundations
- **Reason:** This is a diagnostic correctness investigation for lock-free data structures (skip list), which relates to memory models and atomic operations owned by Foundations.

---

## WT-10669: Review WT perf tests to ensure they cover MongoDB like use cases with appropriate concurrency
- **Team:** Storage Engines - Foundations
- **Reason:** This is a review of WT performance tests to ensure they represent MongoDB-like workloads with appropriate concurrency, owned by Foundations under performance benchmarking tools.

---

## WT-10675: Add open_session config to set session name
- **Team:** Storage Engines - Foundations
- **Reason:** This is an API improvement to WT_CONNECTION->open_session to allow applications to set a session name via configuration, which falls under API (sessions, connections, configuration) owned by Foundations.

---

## WT-10694: Compile third party lib on Windows
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build system task to ensure third-party libraries (sodium, memkind) compile correctly on Windows, owned by Foundations under build system and compile.

---

## WT-10696: GDB fails to load source files when compiling with gcc in mongodbtoolchain v4
- **Team:** Storage Engines - Foundations
- **Reason:** This is a build system and developer productivity issue with GCC debug info in the mongodbtoolchain v4, owned by Foundations under build system and CI/CD infrastructure.

---

## WT-10718: Investigate the conditions to open a checkpoint cursor
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket investigates conditions around opening checkpoint cursors, specifically whether oldest/stable timestamp comparisons are needed, which involves checkpoint cursor logic owned by Persistence.

---

## WT-10768: Create a dedicated command for wt util to explore a file
- **Team:** Storage Engines - Foundations
- **Reason:** This is an improvement to the wt utility tool to add a dedicated `live` command for interactive file exploration, which falls under tooling and correctness/debugging tools owned by Foundations.

---

## WT-10769: The -E option of format.sh is not working as expected
- **Team:** Storage Engines - Foundations
- **Reason:** This is a bug in the format.sh test script where the -E (skip errors) option is broken due to removed functionality, owned by Foundations under correctness frameworks (format test).

---

## WT-10782: Create a script to check for trailing whitespaces in python and evergreen files
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code style and CI tooling improvement to check trailing whitespace in Python and Evergreen files, owned by Foundations under build system, lint, and code style.

---

## WT-10788: Evaluate whether to consider internal pages for dirtied by a transaction
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket evaluates whether internal pages should be counted when tracking transaction bytes added to cache, which involves cache/eviction and B-tree page management owned by Transactions.

---

## WT-10794: Save WT files in cloud storage as part of test artifacts
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD infrastructure improvement for capturing tiered storage test artifacts in Evergreen failures, owned by Foundations under CI/CD infrastructure.

---

## WT-10795: Add fflush calls in random_abort and other parent/child programs
- **Team:** Storage Engines - Foundations
- **Reason:** This is a correctness improvement to test programs (random_abort and similar) to flush buffered stdout output, falling under correctness frameworks (csuite) owned by Foundations.

---

## WT-10801: Update skip list comment to include speculation in description
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code comment improvement in row_srch.c to clarify that speculative CPU execution (not just compiler reordering) is involved, which relates to memory models and atomic operations owned by Foundations.

---

## WT-10824: Create a tool to automatically parse and categorize checksum mismatch failures
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket is about building a diagnostic tool to analyze checksum mismatch failures in on-disk data, which involves block manager and on-disk data integrity owned by Persistence.

---

## WT-10828: Add workgen and ext/storage_source to s_string
- **Team:** Storage Engines - Foundations
- **Reason:** This is a code style tooling task to extend the s_string style checker to cover workgen and storage source extension files, owned by Foundations under build system and lint.

---

## WT-10829: Redact AccountKey when printing out configuration passed into WiredTiger
- **Team:** Storage Engines - Foundations
- **Reason:** This is a security/code quality improvement to redact the auth_token field from configuration error messages in the API layer, owned by Foundations under API and configuration.

---

## WT-10832: Investigate reconciliation split logic not creating reasonably sized pages
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket investigates the reconciliation split logic that determines how data is laid out on leaf pages, which is a core B-tree reconciliation concern owned by Transactions.

---

## WT-10833: Implement a mechanism to combine small on-disk pages together
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket is about implementing a mechanism to identify and merge small leaf pages to create more efficient B-tree structures, which involves B-tree operations and reconciliation owned by Transactions.

---

## WT-10839: Add cursor reset to commit in cppsuite
- **Team:** Storage Engines - Foundations
- **Reason:** This is a cppsuite stress test framework improvement related to cursor reset behavior after transaction commit, falling under correctness frameworks owned by Foundations.

---

## WT-10842: Improve the HS validation by checking hs_counter
- **Team:** Storage Engines - Transactions
- **Reason:** This is an improvement to history store (HS) validation to also check the hs_counter field for key ordering, which involves transaction visibility and history store integrity owned by Transactions.

---

## WT-10843: Improved support for transient tables
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket explores adding a table-like interface with no durability guarantees for temporary/transient data, which involves schema operations and the table/cursor API owned by Foundations.

---

## WT-10844: Try to combine __wt_hs_verify_one and __hs_verify_id
- **Team:** Storage Engines - Transactions
- **Reason:** This is a code quality refactoring of the history store verification functions to reduce duplication, which involves history store internals owned by Transactions.

---

## WT-10845: Add statistics that give insight to cached disk image size
- **Team:** Storage Engines - Transactions
- **Reason:** This ticket adds statistics to provide visibility into disk image sizes for pages in the cache, which involves cache/eviction management and statistics owned by Transactions.

---

## WT-10850: s3 subsystem not printing error messages by default
- **Team:** Storage Engines - Persistence
- **Reason:** This is a bug in the S3 storage source extension where error messages are not printed unless verbose logging is explicitly configured, which involves the filesystem API and storage source extensions owned by Persistence.

---

## WT-10853: Avoid complete stdout/stderr dump of massive files for Python tests
- **Team:** Storage Engines - Foundations
- **Reason:** This is a CI/CD and Evergreen pipeline improvement to limit stdout dump size from Python test failures to improve diagnostic usability, owned by Foundations under CI/CD infrastructure.

---

## WT-10855: Lock free lists using CAS and generations
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket describes a lock-free linked list mechanism using CAS operations and WT generations for managing data structure disposal, which involves memory models and atomic operations owned by Foundations.

---

## WT-10865: Enhance s_string to check for spelling errors in python comments
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket improves the `s_string` code style/lint tool, which is part of the build system and code style infrastructure owned by Foundations.

---

## WT-10886: Update testy logging and link logs to dashboard
- **Team:** Storage Engines - Foundations
- **Reason:** Improving the testy testing framework's logging and CI dashboard integration falls under CI/CD infrastructure and correctness frameworks owned by Foundations.

---

## WT-10891: Running wt utility outside of the test/format directory fails unintuitively
- **Team:** Storage Engines - Foundations
- **Reason:** This involves fixing a relative path issue in test/format, bench/wtperf, and cppsuite tools, all of which are part of the correctness frameworks and performance benchmarking tools owned by Foundations.

---

## WT-10896: Test dist/s_docs with doxygen 1.9.3 (or drop support?)
- **Team:** Storage Engines - Foundations
- **Reason:** This is about Evergreen testing of the documentation generation script (`s_docs`), which is part of CI/CD infrastructure and build system maintenance owned by Foundations.

---

## WT-10926: Review all the disabled code without a FIXME
- **Team:** Storage Engines - Foundations
- **Reason:** Reviewing and cleaning up disabled `#if 0` code is a cross-cutting code quality and build system task that spans multiple components, owned by Foundations.

---

## WT-10936: Make test/checkpoint predictable for column store
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing test/checkpoint (a correctness framework test) to support column store is part of the correctness frameworks and test infrastructure owned by Foundations.

---

## WT-10955: Make many-collection-test easier to debug
- **Team:** Storage Engines - Foundations
- **Reason:** Improving the debuggability of the many-collection-test is a developer productivity and correctness framework improvement owned by Foundations.

---

## WT-10956: Investigate performance change in test/format after mirror branch (zseries)
- **Team:** Storage Engines - Foundations
- **Reason:** Investigating a performance regression in test/format across platforms is a performance benchmarking and correctness framework task owned by Foundations.

---

## WT-10982: s-all should not run on multiple variants
- **Team:** Storage Engines - Foundations
- **Reason:** Optimizing the `s-all` Evergreen task to run only on the necessary variant is CI/CD infrastructure work owned by Foundations.

---

## WT-10991: Add "general" handler callbacks to Python SWIG interface
- **Team:** Storage Engines - Foundations
- **Reason:** Extending the Python SWIG interface with new callbacks is a language bindings task owned by Foundations.

---

## WT-10993: Don't use internal WiredTiger structures in the cache_resize.cpp test
- **Team:** Storage Engines - Foundations
- **Reason:** Refactoring a cppsuite test to avoid using internal structures is a correctness framework improvement owned by Foundations.

---

## WT-11004: Prevent tiered objects from being overwritten in s3, gcp, azure
- **Team:** Storage Engines - Persistence
- **Reason:** Ensuring write-once-read-many semantics for tiered storage cloud objects is a filesystem/block management concern for tiered/object storage, owned by Persistence.

---

## WT-11013: Clean up obsolete config items in api_data.py
- **Team:** Storage Engines - Foundations
- **Reason:** Cleaning up obsolete configuration items in `api_data.py` is an API configuration management task owned by Foundations.

---

## WT-11014: Rename the test files and src folder rollback_to_stable to rts
- **Team:** Storage Engines - Persistence
- **Reason:** Renaming RTS source and test files is a code organization task for the Rollback to Stable feature owned by Persistence.

---

## WT-11032: Adjust test/format operation percentages after turning on predictable replay
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing test/format configuration handling for predictable replay is a correctness framework improvement owned by Foundations.

---

## WT-11033: Allow test/format to do modify operations with predictable replay
- **Team:** Storage Engines - Foundations
- **Reason:** Extending predictable replay in test/format to support modify operations is a correctness framework enhancement owned by Foundations.

---

## WT-11037: Evaluate enabling per file stats for history store
- **Team:** Storage Engines - Transactions
- **Reason:** Per-file statistics for the history store relate to transaction/cache management instrumentation, which is owned by the Transactions team.

---

## WT-11038: Expose WiredTiger #defines in python tests
- **Team:** Storage Engines - Foundations
- **Reason:** Exposing internal WiredTiger `#defines` through the Python/SWIG layer for use in tests is a language bindings task owned by Foundations.

---

## WT-11058: format.sh unused verbose function and outdated usage function
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing an outdated shell script in the test/format framework is a correctness framework maintenance task owned by Foundations.

---

## WT-11059: No-op logging for complex tables needed
- **Team:** Storage Engines - Persistence
- **Reason:** This ticket involves fixing WAL (write-ahead log) record writing for truncate operations on complex tables, which is part of the logging/WAL subsystem owned by Persistence.

---

## WT-11061: Fix formatting of block comment describing __wt_session_lock_dhandle()
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing a block comment about dhandle locking is a code style/readability task; the `__wt_session_lock_dhandle` function is part of the data handle management area owned by Foundations.

---

## WT-11093: Memory leaks in error paths realloc failure
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing memory leak bugs in the `util_load.c` and `util_load_json.c` utility tools is a code quality fix in the WiredTiger utilities, owned by Foundations.

---

## WT-11096: Improve logs related to sweep server
- **Team:** Storage Engines - Foundations
- **Reason:** The sweep server manages dhandle lifecycle (closing and expiring data handles), which is part of data handle management owned by Foundations.

---

## WT-11097: Layering violation and potential dead code in wt_gen_drain
- **Team:** Storage Engines - Foundations
- **Reason:** The `wt_gen_drain` generation tracking code is a cross-cutting memory/synchronization primitive; however it directly interacts with eviction generations. Given the layering violation involves the generation framework (not eviction-specific logic), this is best assigned to Foundations as a cross-cutting systemic improvement.

---

## WT-11100: Resolve confusion about exclusive lock requirement in __wt_conn_dhandle_close
- **Team:** Storage Engines - Foundations
- **Reason:** This ticket concerns sweep server locking of data handles during close, which is part of the data handle management area owned by Foundations.

---

## WT-11103: Evaluate effectively of running 2 sets of unit tests in PR builds
- **Team:** Storage Engines - Foundations
- **Reason:** Evaluating PR build test configurations is a CI/CD infrastructure task owned by Foundations.

---

## WT-11104: Assess the history store cursor's visibility semantics
- **Team:** Storage Engines - Transactions
- **Reason:** The history store cursor's visibility and isolation semantics are a transaction/cursor visibility concern owned by Transactions.

---

## WT-11107: Verify steps that cause OOO keys during insertion and deletion races
- **Team:** Storage Engines - Transactions
- **Reason:** Investigating out-of-order key races during concurrent insertions and deletions involves B-tree page operations and hazard pointer logic owned by Transactions.

---

## WT-11110: Fix s_style in finding all paired typos
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing the `s_style` code style tool is part of the build system and lint infrastructure owned by Foundations.

---

## WT-11111: Replace interrogating /proc/cpuinfo by invoking nproc in evergreen tasks
- **Team:** Storage Engines - Foundations
- **Reason:** Improving Evergreen task configuration to use `nproc` instead of `/proc/cpuinfo` is a CI/CD infrastructure improvement owned by Foundations.

---

## WT-11139: Enhance gdb dump script to support dumping on-disk page contents
- **Team:** Storage Engines - Foundations
- **Reason:** Enhancing the GDB debugging script is a developer productivity/debugging tool improvement owned by Foundations.

---

## WT-11149: Spike: Investigate improving consistency of lock usage with the txn_global structure
- **Team:** Storage Engines - Transactions
- **Reason:** Investigating lock consistency for the `txn_global` structure is a transaction subsystem code quality task owned by Transactions.

---

## WT-11174: Investigate using thread.join during wiredtiger shutdown
- **Team:** Storage Engines - Foundations
- **Reason:** Investigating replacement of memory barriers with thread join patterns during `__wt_connection_close` is a cross-cutting memory model and connection API improvement owned by Foundations.

---

## WT-11179: Extend to format: Test runs and repeatedly shuts down verifying everything it can each time
- **Team:** Storage Engines - Foundations
- **Reason:** Extending test/format to support multiple restarts for correctness checking is a correctness framework improvement owned by Foundations.

---

## WT-11185: Prototype tiered storage compaction
- **Team:** Storage Engines - Persistence
- **Reason:** Prototyping compaction for tiered storage (tracking discarded blocks and rewriting objects) is a compaction/block management task owned by Persistence.

---

## WT-11191: Take the latest artifacts when setting up the spawn host
- **Team:** Storage Engines - Foundations
- **Reason:** Improving the spawn host setup script for Evergreen debugging is a CI/CD infrastructure task owned by Foundations.

---

## WT-11200: Create a session stash history buffer to track how and when a page gets freed
- **Team:** Storage Engines - Transactions
- **Reason:** Tracking split generation stash frees in a session history buffer is a B-tree page/split debugging improvement owned by Transactions.

---

## WT-11213: Unexpected timestamp usage using dump/load wt commands
- **Team:** Storage Engines - Transactions
- **Reason:** The error involves timestamp usage enforcement during `wt load`, which is a transaction timestamp validation concern owned by Transactions.

---

## WT-11214: Improve code coverage related to logging and timestamped txn in compatibility testing
- **Team:** Storage Engines - Foundations
- **Reason:** Randomizing logging and timestamp configurations in compatibility testing is a CI/CD/correctness framework improvement owned by Foundations.

---

## WT-11215: test/format: report aggregated configuration probabilities
- **Team:** Storage Engines - Foundations
- **Reason:** Reporting configuration option probabilities in test/format is a correctness framework diagnostic improvement owned by Foundations.

---

## WT-11216: Move away from autoconf in compatibility testing for more branches
- **Team:** Storage Engines - Foundations
- **Reason:** Migrating compatibility testing from autoconf to CMake is a build system improvement owned by Foundations.

---

## WT-11228: Usage messages in some csuite tests incorrect
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing incorrect usage messages in csuite test programs is a correctness framework maintenance task owned by Foundations.

---

## WT-11231: Create a script to detect when triage team should be notified of WT stat changes
- **Team:** Storage Engines - Foundations
- **Reason:** Creating an Evergreen script to monitor stat changes in commits is a CI/CD infrastructure and process automation task owned by Foundations.

---

## WT-11243: Rename the inmem field in test_util.h to avoid confusion
- **Team:** Storage Engines - Foundations
- **Reason:** Renaming a field in test utilities for clarity is a correctness framework code quality task owned by Foundations.

---

## WT-11244: Uninitialized bytes in __interceptor_pwrite during bulk loading in MSAN build
- **Team:** Storage Engines - Transactions
- **Reason:** The MSAN-reported uninitialized memory originates from bulk insert through reconciliation (`__wt_bulk_insert_var` -> `__wt_rec_split`), which is in the B-tree reconciliation path owned by Transactions.

---

## WT-11251: Avoid hardcoded values for failpoints
- **Team:** Storage Engines - Foundations
- **Reason:** Improving failpoint probability configuration is a testing infrastructure and correctness framework improvement owned by Foundations.

---

## WT-11264: Investigate sanitizer code path completeness
- **Team:** Storage Engines - Foundations
- **Reason:** Investigating memory sanitizer code path coverage in PR builds is a CI/CD and correctness framework task owned by Foundations.

---

## WT-11266: Directories have different files to compare in format-predictable-test (7.0, develop)
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing the `wt_cmp_dir` comparison script used in the format predictable test is a correctness framework bug owned by Foundations.

---

## WT-11291: Review all external libraries WiredTiger depends on for MSan compatibility
- **Team:** Storage Engines - Foundations
- **Reason:** Reviewing and providing MSan-instrumented external libraries is a build system and CI/CD infrastructure task owned by Foundations.

---

## WT-11293: Investigate whether a read barrier is needed in hazard.c
- **Team:** Storage Engines - Transactions
- **Reason:** Investigating read barrier correctness in hazard pointer code is a memory model and B-tree safety concern closely tied to the hazard pointer mechanism owned by Transactions.

---

## WT-11304: Investigate: Determine if variables contained within a lock are used without the lock being taken in some contexts
- **Team:** Storage Engines - Foundations
- **Reason:** Investigating compile-time or script-based detection of missing lock acquisitions is a cross-cutting code quality and memory model investigation owned by Foundations.

---

## WT-11375: Allow the S3 extension to use AWS sso
- **Team:** Storage Engines - Persistence
- **Reason:** Improving the S3 tiered storage extension's authentication mechanism is a tiered storage/filesystem API task owned by Persistence.

---

## WT-11376: Allow the Azure extension to use Azure AD
- **Team:** Storage Engines - Persistence
- **Reason:** Improving the Azure tiered storage extension's authentication to use Azure AD is a tiered storage/filesystem API task owned by Persistence.

---

## WT-11377: Allow the GCP extension to use Application Default Credentials (ADC)
- **Team:** Storage Engines - Persistence
- **Reason:** Improving the GCP tiered storage extension's authentication to use ADC is a tiered storage/filesystem API task owned by Persistence.

---

## WT-11378: Investigate perf impact of eviction algorithm for pages with a lot of small updates but not big enough to trigger forced eviction
- **Team:** Storage Engines - Transactions
- **Reason:** Investigating and improving the eviction algorithm for pages with many small updates is a cache/eviction management task owned by Transactions.

---

## WT-11379: Add support for newer GCC and Clang versions
- **Team:** Storage Engines - Foundations
- **Reason:** Adding support for newer compiler versions in Evergreen compile tasks is a build system and CI/CD infrastructure task owned by Foundations.

---

## WT-11383: Implement mechanism to check variable names are compared to correct macro names in WiredTiger codebase
- **Team:** Storage Engines - Foundations
- **Reason:** Implementing a static check or script to prevent incorrect type comparisons (e.g., txn ID vs. timestamp) is a build system/code quality tooling task owned by Foundations.

---

## WT-11384: Create a perf test to assess r/w latency while stressing the maximum page size at eviction
- **Team:** Storage Engines - Transactions
- **Reason:** Creating a performance test to stress maximum page size during eviction directly targets cache/eviction behavior owned by Transactions.

---

## WT-11385: Investigate how a page with a few entries can be created despite of the existence of pages with lots of entries
- **Team:** Storage Engines - Transactions
- **Reason:** Investigating how reconciliation creates a page with too few entries is a B-tree reconciliation and page split issue owned by Transactions.

---

## WT-11388: Investigate volatility in the overflow-130k Btree Throughput performance charts
- **Team:** Storage Engines - Foundations
- **Reason:** Investigating performance chart volatility for the overflow-130k wtperf test configuration is a performance benchmarking tools task owned by Foundations.

---

## WT-11393: Move connection locks under a separate structure
- **Team:** Storage Engines - Foundations
- **Reason:** Refactoring connection-level locks into a sub-structure is an API/connection management code quality task owned by Foundations.

---

## WT-11394: Investigate utilizing pthread mutex correctness attributes in WiredTiger
- **Team:** Storage Engines - Foundations
- **Reason:** Investigating pthread mutex error-checking attributes is a cross-cutting memory model and synchronization code quality task owned by Foundations.

---

## WT-11403: Module to induce cache pressure along a workgen workload
- **Team:** Storage Engines - Transactions
- **Reason:** Creating a workgen module to induce cache pressure is a performance benchmarking tool directly tied to cache/eviction testing owned by Transactions.

---

## WT-11404: Do not create tiered table's local file until first write
- **Team:** Storage Engines - Persistence
- **Reason:** Deferring creation of tiered table local files until first write is a tiered storage/filesystem API improvement owned by Persistence.

---

## WT-11422: Update the cppsuite to be able to generate modify operations
- **Team:** Storage Engines - Foundations
- **Reason:** Extending the cppsuite correctness test framework to support modify operations is a correctness framework improvement owned by Foundations.

---

## WT-11446: Incorrect encoding for variable length negative int
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing variable-length integer encoding in `intpack_inline.h` is a low-level data format/utility code quality fix that is cross-cutting and owned by Foundations.

---

## WT-11485: Review WT's usage of casting
- **Team:** Storage Engines - Foundations
- **Reason:** Reviewing and establishing a strategy for C casting (including `WT_CELL_UNPACK` and API-level casts) is a cross-cutting code style and build system task owned by Foundations.

---

## WT-11502: Migrate upload_stats_atlas.py to wiredtiger repo
- **Team:** Storage Engines - Foundations
- **Reason:** Deciding on and migrating a stats upload script is a CI/CD infrastructure and performance benchmarking tooling task owned by Foundations.

---

## WT-11503: Improve the precision of WT_CEIL_POS Macro for decimal values
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing a numeric precision bug in the `WT_CEIL_POS` macro is a cross-cutting utility/code quality fix owned by Foundations.

---

## WT-11513: Create a single function for workgen workloads
- **Team:** Storage Engines - Foundations
- **Reason:** Consolidating workgen workload Evergreen functions is a CI/CD infrastructure and performance benchmarking tools task owned by Foundations.

---

## WT-11552: Use system clock to measure test duration in wtperf
- **Team:** Storage Engines - Foundations
- **Reason:** Improving wtperf timing accuracy using the system clock is a performance benchmarking tools enhancement owned by Foundations.

---

## WT-11719: C/C++ Style Guide Proposal: Terminating Multi-line Preprocessor Macros with Single-line Comments
- **Team:** Storage Engines - Foundations
- **Reason:** Proposing a C/C++ style guide change for macro termination is a code style and build system tooling task owned by Foundations.

---

## WT-11734: Improve op_rate functionality in cppsuite
- **Team:** Storage Engines - Foundations
- **Reason:** Improving the cppsuite `op_rate` thread management to allow clean shutdown is a correctness framework improvement owned by Foundations.

---

## WT-11742: Find a better way to interact with stats using the Metrics Monitor in the cppsuite
- **Team:** Storage Engines - Foundations
- **Reason:** Improving stats interaction in the cppsuite Metrics Monitor is a correctness framework infrastructure task owned by Foundations.

---

## WT-11748: C Style Guide Proposal: Declaration of Variables at Point of Use
- **Team:** Storage Engines - Foundations
- **Reason:** Proposing a C style guide change for variable declaration placement is a code style task owned by Foundations.

---

## WT-11750: Remove cast from SKIP_FIRST and SKIP_LAST
- **Team:** Storage Engines - Transactions
- **Reason:** Removing the cast buried inside the `WT_SKIP_FIRST` and `WT_SKIP_LAST` macros is a code quality fix for in-memory B-tree skip list data structures owned by Transactions.

---

## WT-11768: Explore what we should do as our condition variable implementation diverges from the standard definition
- **Team:** Storage Engines - Foundations
- **Reason:** Investigating WiredTiger's custom condition variable semantics versus the standard definition is a cross-cutting memory model and synchronization primitive concern owned by Foundations.

---

## WT-11780: Clarify the isolation levels wiredtiger doc with a diagram
- **Team:** Storage Engines - Transactions
- **Reason:** Clarifying isolation level documentation (including snapshot isolation and write skew) is a transaction semantics documentation task owned by Transactions.

---

## WT-11790: C and C++ Style Guide Proposal: Use Braces for Multi-line if/for/while Statements
- **Team:** Storage Engines - Foundations
- **Reason:** Proposing a code style guide update for brace usage is a build system and code style task owned by Foundations.

---

## WT-11796: Create a nice-looking and comprehensive README.md for WT on GitHub
- **Team:** Storage Engines - Foundations
- **Reason:** Creating a comprehensive README for the WiredTiger GitHub repository is a release management and documentation task owned by Foundations.

---

## WT-11848: Identify list of noisy perf tests for investigation
- **Team:** Storage Engines - Foundations
- **Reason:** Identifying and triaging noisy performance tests in wtperf/cppsuite is a performance benchmarking tools and CI/CD stability task owned by Foundations.

---

## WT-11940: Review the use of __wt_yield (sched_yield)
- **Team:** Storage Engines - Foundations
- **Reason:** Reviewing the use of `sched_yield` across the codebase is a cross-cutting memory model and threading concern owned by Foundations.

---

## WT-11968: Investigate if PowerPC atomic primitives provide sufficient memory barriers
- **Team:** Storage Engines - Foundations
- **Reason:** Investigating whether PowerPC atomic operations provide the required memory barrier guarantees is a memory model/atomic operations task owned by Foundations.

---

## WT-11973: Review the full barrier in __wt_sleep
- **Team:** Storage Engines - Foundations
- **Reason:** Reviewing and refactoring the full memory barrier inside `__wt_sleep` is a memory model/atomic operations task owned by Foundations.

---

## WT-11974: Take advantage of the fact that __wt_random is equivalent to flipping a coin 32 times
- **Team:** Storage Engines - Foundations
- **Reason:** Optimizing `__wt_random` usage using count-leading-zeros for better performance is a cross-cutting utility optimization; the skip list depth calculation it targets is a B-tree data structure, but the optimization itself is a general utility improvement best owned by Foundations.

---

## WT-11975: Remove full barriers in thread creation and join code
- **Team:** Storage Engines - Foundations
- **Reason:** Removing unnecessary memory barriers from thread creation and join code is a memory model/atomic operations improvement owned by Foundations.

---

## WT-12010: Testy detects corruption flag in a log record during verify
- **Team:** Storage Engines - Persistence
- **Reason:** A testy test detecting WAL log record corruption and recovery failure (WT_TRY_SALVAGE) involves the logging/WAL and verify subsystems owned by Persistence.

---

## WT-12035: Atomic flag set/clear should use dedicated RMW ops rather than CAS loop
- **Team:** Storage Engines - Foundations
- **Reason:** Replacing CAS loops with direct atomic read-modify-write operations in `hardware.h` is a memory model/atomic operations improvement owned by Foundations.

---

## WT-12037: Slow file opens on Windows
- **Team:** Storage Engines - Persistence
- **Reason:** Slow file open operations on Windows under load involve the filesystem API and OS file handling abstraction layer owned by Persistence.

---

## WT-12045: Disable hyperthreading for x86 perf runs
- **Team:** Storage Engines - Foundations
- **Reason:** Configuring Evergreen perf test hosts to disable hyperthreading is a CI/CD infrastructure and performance benchmarking tooling task owned by Foundations.

---

## WT-12067: Improve/Fix CRC calculation and testing on zSeries
- **Team:** Storage Engines - Foundations
- **Reason:** Fixing CRC32 hardware acceleration and endianness issues on zSeries is a cross-cutting, platform-specific build/correctness task owned by Foundations.

---

## WT-12291: __wt_file_zero doesn't need to allocate and zero a buffer
- **Team:** Storage Engines - Persistence
- **Reason:** Optimizing `__wt_file_zero` to use a static zero buffer instead of allocating memory is a filesystem API / block manager improvement owned by Persistence.

---

## WT-12293: Optimize our crc32 implementation for x86
- **Team:** Storage Engines - Foundations
- **Reason:** Investigating and implementing parallel CRC32 streams for x86 performance is a cross-cutting platform optimization and build system task owned by Foundations.

---

## WT-12294: Implement generic validation of WT locking hierarchy
- **Team:** Storage Engines - Foundations
- **Reason:** Implementing a generic locking hierarchy validator in diagnostic mode is a cross-cutting code quality and synchronization infrastructure improvement owned by Foundations.

---
