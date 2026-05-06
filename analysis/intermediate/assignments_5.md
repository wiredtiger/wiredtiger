# Team Assignments - Group 5 (WT-9810 to WT-10855)

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
