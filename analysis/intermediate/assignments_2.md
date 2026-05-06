# Team Assignments - Group 2 (WT-6100 to WT-7495)

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
