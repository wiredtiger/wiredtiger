# Team Assignments - Group 6 (WT-10865 to WT-12294)

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
