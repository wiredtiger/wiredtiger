# Team Assignments - Group 4 (WT-8811 to WT-9808)

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
