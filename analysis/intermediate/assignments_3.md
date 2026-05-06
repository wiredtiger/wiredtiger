# Team Assignments - Group 3 (WT-7503 to WT-8810)

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
