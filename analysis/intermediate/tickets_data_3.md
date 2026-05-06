# WiredTiger Tickets Data - Group 3 (WT-7503 to WT-8810)

## WT-7503: Change default compressor for WT HS to Zstandard

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** Vamsi Boyapati
- **Reporter:** Brian Lane
- **Created:** 2021-05-07T01:12:41.000+0000
- **Updated:** 2022-12-19T05:21:48.000+0000

**Description:**
Change the default compressor for the HS to Zstandard from Snappy.  You could also use this ticket to create some tests around upgrade/downgrade for this change.  If so, reflect that in the estimate.

---

## WT-7505: Use Python hooks to improve test coverage for in-memory config

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2021-05-07T15:15:20.000+0000
- **Updated:** 2022-04-05T01:07:06.000+0000

**Description:**
WT Python tests only provide minimal testing for in-memory configurations. 

We should use the Python test hooks from WT-7329 to automatically run as many of the python tests as possible in that config.

(Suggested by [~keith.bostic])

---

## WT-7518: Update WT_DATA_HANDLE to support different types of backing storage for Btrees

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** tiered-storage, tiered-storage-misc
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2021-05-11T19:00:44.000+0000
- **Updated:** 2025-12-03T06:07:46.596+0000

**Description:**
Currently a dhandle of type `WT_DHANDLE_TYPE_BTREE` is assumed live in a single file. I.e., it has a URI of the form, `file:name.wt`.  

Tiered storage introduces Btrees that use `tiered:` URIs, which describe the combination of local files and objects that hold the BTree.

The goal of this ticket is to update the WiredTiger code so that a single dhandle can represent either form of BTree.  This will ensure that various bits of code that make decisions based on `dhandle->type == WT_DHANDLE_TYPE_BTREE` will continue to work without needing to know that there are two different ways the BTree might be representing on the underlying storage.

This will consist of the following pieces of work:
 1. Introduce new dhandle flags to indicate whether a btree is stored in a single file or a tiered set of files/objects
 2. Eliminate the `WT_DHANDLE_TYPE_TIERED` and `WT_DHANDLE_TYPE_TIERED_TREE` types in favor of appropriate settings of the above flags.
 3. Examine existing code that performs checks for `dhandle->type == WT_DHANDLE_TYPE_BTREE` and ensure that the above changes won't break anything.
 4. Examine existing code that performs checks for `WT_PREFIX_MATCH(uri, "file:")` and update as needed to make sure it will behave correctly for tiered Btrees.

---

## WT-7527: Perform fine-tuning on reverse modifies for HS records

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2021-05-12T05:14:27.000+0000
- **Updated:** 2022-04-05T01:25:33.000+0000

**Description:**
This ticket follows the work done in WT-7106. A new variable `WT_MAX_CONSECUTIVE_REVERSE_MODIFY` has been defined to limit the number of consecutive reverse modifies during reconciliation before performing a full update.

Instead of having a hardcoded value, we could have a dynamic approach. Find below a few ideas from the WT-7106 discussion:

- If a reverse delta uses 1% of the space a full record would, then we might store 100 reverse deltas per full update. If a reverse delta uses 50% or more of the space, we might store 10.

- Store a reverse modify if it will save at least N bytes (or N% of the record size) where N increases with the number of consecutive reverse modifies we've stored.

- This limit could also be based on per-table data instead of database wide.

---

## WT-7558: 5% performance regression in retryable writes workloads with 8 threads on linux-1-node-replSet

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haseeb Bokhari
- **Created:** 2021-05-19T00:34:11.000+0000
- **Updated:** 2022-04-05T01:25:28.000+0000

**Description:**
This is a break-off ticket from BF-10453. We have noticed a 5% regression in MongoDB branch compared to v4.4 branch for retryable_writes_workloads benchmarks. The aim of this ticket is to:

* Verify the regression still exists
* Next step would be to analyze the t2 data between v4.4 and master branches and confirm if the regression is related to WiredTiger.
* Create a WT ticket for possible improvement.

---

## WT-7568: Use project-level API token for git.get_project in Evergreen

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2021-05-21T00:27:48.000+0000
- **Updated:** 2022-04-05T01:21:21.000+0000

**Description:**
Right now the WiredTiger Evergreen projects use the Evergreen default API token to clone Github repositories, which contributes to the overall usage. Recently we're hitting the limit for our Evergreen default Github token. To help alleviating this issue, we can set up a separate project-level Github API token and use it solely for WiredTiger Evergreen projects.

---

## WT-7576: Remove --zstd option once zstd compressor is installed on PPC and ZSeries machines

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Monica Ng
- **Created:** 2021-05-24T04:45:20.000+0000
- **Updated:** 2022-04-05T01:03:10.000+0000

**Description:**
As part of the project to change our default compressor from snappy to zstd, we introduced a Python test to our test suite which compresses a table using zstd and re-configures its compression level after restart. However, we could not run this along with the rest of the test suite without causing major Evergreen testing fallout as the zstd compressor was not available on PPC and ZSeries machines.

As a temporary fix, in WT-7542 we added a --zstd option so that zstd tests will only be run on machines that do have the zstd compressor. We now want to remove this command option so that the Python test test_compress02.py is integrated with the rest of the unit_test.

---

## WT-7597: Expand support for WiredTiger C/C++ tests on Windows

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alison Felizzi
- **Created:** 2021-05-27T23:01:13.000+0000
- **Updated:** 2022-04-05T01:01:06.000+0000

**Description:**
This task involves auditing the C/C++ tests that are only supported on POSIX and trying to update them to work on Windows. It would be nice to increase our testing coverage on Windows.

**Context:**

Whilst developing CMake support for WiredTiger on Windows (WT-7535), we discovered a number of the C/C++ testing suite programs that aren't supported on Windows. This is either due to the tests assuming the existence of specific POSIX utilities e.g bash/sh commands, use non-native directory paths e.g. (forward vs backward slash) or they try to include specific Linux/Darwin system headers.

Note some tests may be easy to update (e.g replacing hard-coded bash/sh commands with platform-independent `test_util` library calls). However I expect some may not be possible to get running on Windows or would require significant re-work outside the scope of this ticket (e.g a test that is heavily depend on `pthread` functionality).

Below is a summary of the tests that currently don't compile/run on Windows:
```
test/format
test/readonly
test/salvage
test/syscall (I wouldn't expect its feasible to make this work on Windows)
test/thread
test/cppsuite (C++ compilation currently not tested on Windows)
test/csuite/incr_backup
test/csuite/random_abort
test/csuite/random_directio
test/csuite/rwlock
test/csuite/schema_abort
test/csuite/timestamp_abort
test/csuite/truncated_log
test/csuite/wt1965_col_efficiency
test/csuite/wt2403_lsm_workload
test/csuite/wt2535_insert_race
test/csuite/wt2853_perf
test/csuite/wt2909_checkpoint_integrity
test/csuite/wt3120_filesys
test/csuite/wt3338_partial_update
test/csuite/wt4105_large_doc_small_upd
test/csuite/wt4156_metadata_salvage
test/csuite/wt4333_handle_locks
test/csuite/wt4803_history_store_abort
test/csuite/wt6185_modify_ts
test/csuite/wt6616_checkpoint_oldest_ts
```

**Definition of Done:**
 * Have some of our POSIX-only C/C++ tests updated to compile and run on Windows. Identify the tests that are not possible to run on Windows.
 * Add to evergreen windows variant.

---

## WT-7612: Fix operation tracking after reconfiguration and add more tests

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-d
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2021-05-31T05:34:12.000+0000
- **Updated:** 2025-03-18T02:35:50.000+0000

**Description:**
When investigating SERVER-56842, I found a segmentation fault related to the use of the operation tracking feature.

It can be easily reproduced by enabling the feature through the reconfiguration API. It seems that there is no test dealing with the operation tracking feature. If this feature is still used/maintained, tests should be added too if possible.

---

## WT-7617: Improve diagnosability of Python test hangs in Evergreen

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2021-05-31T06:29:50.000+0000
- **Updated:** 2022-04-05T01:02:45.000+0000

**Description:**
When a Python test hangs in Evergreen we don't get very good information in the test log files. It generally looks something like a context-canceled message with no definitive statement about which test case is hanging. It's difficult to determine exactly which test hangs, since there are generally multiple tests running in parallel.

There also seems to be an issue which is that our hang analyzer script isn't finding debug symbols, so doesn't show useful stack traces.

We should enhance our testing to make such failures easier to diagnose.

---

## WT-7622: Add CMake + icecream support on our dev servers

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** neweng, streamline-standalone-wt
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alison Felizzi
- **Created:** 2021-05-31T23:22:35.000+0000
- **Updated:** 2022-04-05T01:06:56.000+0000

**Description:**
Add CMake support to compile WiredTiger with Icecream (icecc) on our development servers.

**Context:**

We have icecream distributed build support setup on our development servers. It would be worth investigating the potential build time improvements when compiling WiredTiger with our distributed icecream build farm.

This task would involves updating and testing the WiredTiger CMake build system to compile the repository with the icecream compiler. This task would most likely also involve writing a separate CMake toolchain file to source the installed icecc toolchain.

---

## WT-7650: Investigate test/format failing on existing databases with prefix enabled

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Monica Ng
- **Created:** 2021-06-07T00:59:47.000+0000
- **Updated:** 2022-04-05T00:45:56.000+0000

**Description:**
In WT-7579 we saw some failures in the backwards compatibility testing, where snapshot isolation search mismatches occurred when prefix testing was enabled. For now, prefix testing has been disabled in backwards compatibility testing, but we want to do some further investigation on why test/format is failing.

---

## WT-7688: Identifying and handling corrupted files in WiredTiger

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2021-06-15T18:20:18.000+0000
- **Updated:** 2022-04-05T01:07:42.000+0000

**Description:**
This ticket discusses the fragility of the current approach to handling corrupted files in WiredTiger, specifically around the `WT_CONNECTION.WT_CONN_DATA_CORRUPTION` flag and `WT_SESSION.WT_SESSION_QUIET_CORRUPT_FILE` flag usage. The current mechanism requires that `ret == WT_ERROR` before checking the corrupted flag in RTS code, which creates a tight coupling that could lead to bugs.

The proposed solution involves:
* Adding a session flag that gets set in the case of block-manager corruption, and querying that flag if a block manager call returns an error.
* At a higher level, setting the `WT_CONN_DATA_CORRUPTION` flag, but the block manager shouldn't be doing that work.
* Specifically, RTS would report the corruption and not set the `WT_CONN_DATA_CORRUPTION` flag.

This helps move toward gracefully handling the corruption of a specific collection in the future.

---

## WT-7693: Fix tiered storage disconnect between WT_BUCKET_STORAGE and customize_file_system

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-d, tiered-storage-misc
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2021-06-16T14:33:59.000+0000
- **Updated:** 2026-01-02T04:48:21.177+0000

**Description:**
There is a bit of a disconnect between the `WT_BUCKET_STORAGE` structure and the local store `customize_file_system`. They are both acting as a source of information and, in particular, the file system is missing the bucket prefix. They also duplicate a lot of information.

This ticket should figure out if we can have one data structure and one copy of the information.

---

## WT-7734: Add dhandle flag to indicate dhandles that are both btree and object

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** tiered-storage-misc
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2021-06-25T20:13:51.000+0000
- **Updated:** 2022-04-05T01:01:58.000+0000

**Description:**
We need a flag to identify a dhandle for an object. Currently we need the following check (e.g., in `conn_dhandle.c`):
```c
if (dhandle->type == WT_DHANDLE_TYPE_BTREE && WT_SUFFIX_MATCH(dhandle->name, ".wtobj"))
```
Alternatively (and perhaps better in the long term) can we eliminate dhandles on individual storage objects?

---

## WT-7735: Support tiered tables in wt_block_checkpoint_last

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** tiered-storage, tiered-storage-misc
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2021-06-25T21:00:28.000+0000
- **Updated:** 2026-01-05T02:21:06.605+0000

**Description:**
`__wt_block_checkpoint_last` is used (if needed) to find the most recent valid checkpoint during table import. It scans the entire file looking for checkpoint roots and returns the most recent one.

This needs to be extended to support tiered tables. This means scanning objects that may be part of the table. We should be able to take advantage of the write-once nature of objects and scan them in reverse order, ending the search once we find an object with a checkpoint root.

---

## WT-7800: Windows Evergreen Windows tests don't test extension libraries

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-d
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2021-07-07T20:43:34.000+0000
- **Updated:** 2026-03-04T05:52:26.267+0000

**Description:**
In our Windows tests in Evergreen it looks like we don't build the extension libraries in the `ext/` directory and therefore don't execute the python tests that would use them.

The libraries for `compressors/nop` or `encrypt/rotn` are not found in the build tree. The test output includes lines showing that `test_cursor08` couldn't find the `compressors/nop` and is therefore being skipped.

Assuming we want these tests to run on Windows, we might also want to change the tests to fail rather than being skipped if they can't find an extension library that is included in the WT source tree.

---

## WT-7862: Reduce unnecessary verbose RTS logs and enable them by default

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2021-07-16T04:11:30.000+0000
- **Updated:** 2022-04-05T01:15:33.000+0000

**Description:**
Without RTS verbose logging availability, it may be difficult to identify the problematic code when a record gets missed or lost. It is possible that either the data doesn't get written to the checkpoint or it may be removed by RTS.

With the availability of verbose logging, it would be easy to identify the problematic code. To do that, reduce the number of RTS verbose messages that are not giving information of each individual record and enable them by default.

The proposed change is no longer required when WT starts supporting different levels of verbose options.

---

## WT-7879: Investigate potential improvements of using atomics for cache configuration statistics

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Monica Ng
- **Created:** 2021-07-20T02:09:47.000+0000
- **Updated:** 2022-04-05T00:44:47.000+0000

**Description:**
Statistics are not designed to be 100% accurate all the time as implementing locks on reads would often be too expensive given the use case.

A user has recently reported that the cache_bytes_max statistic occasionally returns an incorrect zero value. This is plausible as it is possible two cursors may race when collecting this metric. The stat value is being reset before being assigned with the new value via the `WT_STAT_SET` macro.

We would like to investigate whether it is plausible to guarantee correct values without negatively impacting performance. A potential approach could be to avoid resetting cache configurations each time by utilizing atomics to swap in newer statistic values.

---

## WT-7884: test_cursor_random failed due to key not set for insert operation

- **Status:** Backlog
- **Type:** Build Failure
- **Priority:** Major - P3
- **Labels:** tf
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Xgen-Evergreen-User
- **Created:** 2021-07-21T23:22:53.000+0000
- **Updated:** 2024-06-03T15:27:12.000+0000

**Description:**
unit-test-with-compile failed on OS X 10.14. Host: macos-1014-79.macstadium.build.10gen.cc. Project: WiredTiger (develop). Commit: WT-7732 Add a timeout configuration for flush_tier.

---

## WT-7919: Write "Reconciliation" subpage for Architecture Guide

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** arch-guide
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-08-04T00:03:49.000+0000
- **Updated:** 2022-04-05T01:29:44.000+0000

**Description:**
No description

---

## WT-7927: incr_backup test doesn't test variable- or fixed-length column store access methods

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** Column Store
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2021-08-05T23:57:49.000+0000
- **Updated:** 2025-08-20T21:58:25.055+0000

**Description:**
The incr_backup test does not appear to test either variable-length or fixed-length column store.

---

## WT-7946: Create a Wiki page on flamescope

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-08-10T06:17:38.000+0000
- **Updated:** 2022-04-05T01:05:46.000+0000

**Description:**
No description

---

## WT-7966: No need to handle lower isolation levels in reconciliation

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2021-08-17T08:32:34.000+0000
- **Updated:** 2022-04-05T00:58:44.000+0000

**Description:**
Since we have disabled write operations for lower isolation levels, there is no need to handle them specially in reconciliation. The following code that handles the rare case when applications run at low isolation levels (where eviction may see a committed update followed by uncommitted updates) can be removed.

---

## WT-7969: Recovery failed trying to allocate a very large amount of memory

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2021-08-17T23:06:23.000+0000
- **Updated:** 2022-04-05T01:30:28.000+0000

**Description:**
A customer has reported a failure where `__wt_txn_recover` allocated an unreasonable amount of memory, 17GB, on a host with a total of 15GB of memory. This allocation fails and as a result WiredTiger cannot recover.

In the reported scenario MongoDB would then attempt to restart and immediately hits the same issue with the same amount of memory being allocated. The fix in this situation was to resync the problematic node.

---

## WT-7976: Commit timestamp should be greater than latest active read timestamp

- **Status:** Open
- **Type:** Build Failure
- **Priority:** Major - P3
- **Labels:** stability, tf
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Xgen-Evergreen-User
- **Created:** 2021-08-20T00:27:05.000+0000
- **Updated:** 2025-02-28T02:36:03.000+0000

**Description:**
make-check-test failed on Ubuntu 18.04 CMake. Host: ec2-54-235-25-214.compute-1.amazonaws.com. Project: WiredTiger (develop). Commit: WT-7909 Use a new method to check concurrent user transactions before starting rollback to stable.

---

## WT-7990: Rethink data handle statistics

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2021-08-23T15:10:00.000+0000
- **Updated:** 2022-04-05T01:06:06.000+0000

**Description:**
One of the main memory expenses for data handles is statistics. In `dist/stat_data.py` under the `dsrc_stats` heading, are roughly 100 stats at 8 bytes each. Of the 100 or so stats, 70 look to be btree related, 10 for LSM, 23 for cursor ops. Tiered storage may need its own set of statistics. When considering having many thousands of MongoDB collections, each with multiple indices, these numbers can add up.

Possible restructuring ideas include:
- Store stats not directly in the data_handle, but in the "associated" data structure (btree, LSM struct, etc.)
- Have an array of stats represented by a small bitmap and a pointer to an array, where the bitmap says which groups of stats are represented (LSM, ColumnStore, Tiered, Compression) and the array is sized accordingly.

---

## WT-7991: improve row/byte-count information in split-heavy workloads

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2021-08-23T18:03:50.000+0000
- **Updated:** 2022-04-05T00:46:40.000+0000

**Description:**
A potential feature in progress is a new WT_SESSION API that returns row- and byte-count information for the object as a whole as well as cursor ranges in the object.

A weakness in the cursor range implementation is some number of "failures" where cursor range requests return WT_NOTFOUND when they can't return useful information because the tree is dynamic enough there's no available row/byte-count information for the subtree.

We may want to fix this by tracking row/byte-count information across splits by adding 16B per WT_REF and aggregating row/byte-count information through splits.

---

## WT-8002: Brainstorm ideas to fix inconsistency in timestamp format between API and error output

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2021-08-25T07:38:39.000+0000
- **Updated:** 2022-05-09T18:18:04.000+0000

**Description:**
WiredTiger API accepts timestamps as hexadecimal strings without "0x", but reports back timestamps as decimals in errors or verbose messages. This causes confusion at times.

For example, when setting the stable timestamp to 184791 (0x184791 = 1591185), error messages print timestamps in decimal which can be confusing to end-users who set them in hex. This inconsistency is a by-product of historical API evolution and is reported in WT-7968.

The ticket aims to brainstorm ideas to mitigate this inconsistency.

---

## WT-8008: Investigate long stalls in 5.0 compared to 4.4 with many collection test

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2021-08-27T07:27:50.000+0000
- **Updated:** 2022-04-05T01:08:32.000+0000

**Description:**
Many-collection-test was run with 250k collections on 5.0 and 4.4 to write a report on improvements made (WT-7614). We found that 5.0 showed extended stalls that did not necessarily relate to the checkpoint (though there might be a relationship since we see longer checkpoints during the same time). These were not observed with 4.4. We should investigate the reasons behind the stalls on 5.0, and possibly fix them.

---

## WT-8028: The many-collection-test does not run when a task is not configured

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-d
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2021-09-01T02:28:39.000+0000
- **Updated:** 2022-07-17T12:17:10.000+0000

**Description:**
When trying to run many-collection-test on an existing database, the script fails because it does not allow the task input argument to be undefined, and it exits if a variable is undefined.

Another issue is when the replica set was being initialized by the test while it was already set up. The condition when `setup_mongodb()` is called needs to be fixed - it should be called when `populate` is set to `True`, not when `oplog` is set to `True`.

**Definition of done:** Make sure the test can run with and without specifying the task name.

---

## WT-8031: Fix many-dhandles-stress.py for range partition

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2021-09-01T18:21:41.000+0000
- **Updated:** 2022-04-05T01:17:05.000+0000

**Description:**
In `bench/workgen/runner/many-dhandle-stress.py` there is a comment that says the `range_partition` was updated to False because workgen has some issues with `range_partition true`, and to revert it back after WT-7332.

The referenced ticket WT-7332 is complete and closed so whatever changes needed to be reverted can now be done. We should also consider how to find these needed edits at the time so they don't get lost and forgotten.

---

## WT-8037: Review coverage-report test and seek coverage improvement

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2021-09-02T01:43:40.000+0000
- **Updated:** 2022-04-05T01:12:41.000+0000

**Description:**
The contents of the `coverage-report` Evergreen task were ported from the previous Jenkins job. It had not been reviewed and extended for quite a while. Some recently added features or new defaults (e.g. the zstd compression algorithm) are not covered by this test.

We should review the `coverage-report` task and come out with a list of testing areas that we could use to extend the task to achieve more up-to-date coverage.

---

## WT-8040: disallow direct modification of WT managed files

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-d
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2021-09-02T18:40:12.000+0000
- **Updated:** 2022-07-15T15:23:48.000+0000

**Description:**
Certain Btrees are indirectly created and managed by WT, and they should not be (easily) modified using the API and/or `wt` utility. For example, `.bf` files (bloom filters created by LSM) and `.wti` files (index files) can be created. Once that is done, `wt dump` and/or `wt load` can be invoked to view and/or modify the contents of the file, or opening a cursor on these files in a program to modify them. These files can also be dropped.

We might consider ways to prevent direct modifications of these files. Some files like `WiredTiger.wt` appear to already be protected in this way.

---

## WT-8049: Bug in dumping stdout/stderr on error in unit testing on os x - cmake

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, dev-prod, group-d
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2021-09-06T00:21:59.000+0000
- **Updated:** 2022-07-15T15:21:29.000+0000

**Description:**
A failure was found in unit testing on OS X - Cmake build where the attempt to show contents of stdout/stderr for triage purposes failed. The `dump stderr/stdout` step appeared to run but didn't actually output anything useful, requiring manual artifact download to get needed information for triaging.

---

## WT-8064: Investigate massive improvement in YCSB workload

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2021-09-08T05:10:12.000+0000
- **Updated:** 2022-04-05T01:03:46.000+0000

**Description:**
Investigate the following improvements unexpectedly obtained in YCSB runs. The improvement is massive with large documents, but also significant with small documents. The goal is to at least be aware of what has caused this improvement.

These improvements were seen in develop, master and 5.0 builds for mongodb-perf-ycsb-compare-releases jobs.

---

## WT-8082: Architecture Guide updates for PM-2503

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-09-14T01:04:55.000+0000
- **Updated:** 2022-04-05T00:54:33.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8083: Architecture Guide updates for PM-2504

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-09-14T01:11:43.000+0000
- **Updated:** 2022-04-05T00:52:25.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8084: Architecture Guide updates for PM-2505

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-09-14T01:19:35.000+0000
- **Updated:** 2022-04-05T01:27:50.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8085: Architecture Guide updates for PM-2506

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-09-14T01:28:54.000+0000
- **Updated:** 2022-04-05T01:24:51.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8087: Architecture Guide updates for PM-2507

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-09-14T01:33:19.000+0000
- **Updated:** 2022-04-05T01:03:50.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8088: Architecture Guide updates for PM-2508

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-09-14T01:38:01.000+0000
- **Updated:** 2022-04-05T01:17:18.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8089: Architecture Guide updates for PM-2509

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-09-14T01:41:53.000+0000
- **Updated:** 2022-04-05T00:59:33.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8090: Architecture Guide updates for PM-2510

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-09-14T01:44:48.000+0000
- **Updated:** 2022-04-05T01:27:03.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8106: Fix prefix search near entries traversal statistics

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-d
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2021-09-16T06:13:06.000+0000
- **Updated:** 2022-07-15T15:12:16.000+0000

**Description:**
The python test `search_near01.py` uses `cursor_next_skip_lt_100` statistic to verify that prefix search is early exiting which overall traverses less entries. This statistic fails when keys in the btree are not evicted.

The issue is that when performing a prefix early exit and returning WT_NOTFOUND directly, the statistics of entries skipped are not being incremented. This ticket will fix the statistics to better reflect entries traversed and update `search_near01` to use more accurate statistics.

---

## WT-8107: Separate next skip statistics from HS

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2021-09-16T07:40:05.000+0000
- **Updated:** 2022-04-05T00:41:52.000+0000

**Description:**
The `cursor_next_skip_lt_100` and `cursor_next_skip_ge_100` statistics combine the increments of both history store and WT data files. When performing a search_near() for a particular key, the visibility check also triggers a search near inside the HS, which increments these same statistics.

The idea is to add another statistic to increment statistics separately from the HS. This would allow better diagnosis of whether next calls are from a data file or the HS file.

---

## WT-8145: Build guidelines in Wiki around descriptive commit messages

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-09-21T06:07:05.000+0000
- **Updated:** 2022-04-05T01:18:55.000+0000

**Description:**
Guidelines in Wiki around descriptive commit messages to help build common understanding and expectations in the WT team.

---

## WT-8155: Statistic around count and duration of the files being checkpointed

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** wt-ideas
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2021-09-23T02:55:37.000+0000
- **Updated:** 2022-04-05T01:10:23.000+0000

**Description:**
Add a statistic around the number of files being checkpointed. The statistic should reset to 0 at the checkpoint start, then be incremented as a new file gets picked up for checkpointing. It will help obtain the following information:
* How many files are a part of a checkpoint
* Exactly when a file gets picked up for the checkpoint, and hence the duration a single file is being checkpointed.

---

## WT-8165: Commit timestamp assertions didn't catch invalid timestamps in specific scenario

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-d, stability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2021-09-27T01:19:43.000+0000
- **Updated:** 2022-07-15T15:05:03.000+0000

**Description:**
In the scenario where we commit an update to a key without a timestamp and then set the transaction timestamp to `2` and then add another update to the same key and then commit the transaction with timestamp `4`, we will end up with an update chain which goes `2 -> 4` and it doesn't create an error which it should given `write_timestamp_usage` is enabled.

Work to do:
* Reproduce the issue
* Identify whether a fix is appropriate or if WT-8169 will prevent the issue from occurring in the future.

---

## WT-8177: Verify lock protection around data structures

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2021-09-29T20:07:46.000+0000
- **Updated:** 2022-04-05T01:14:22.000+0000

**Description:**
It would be good to formalize how data structures are accessed in WiredTiger. In particular, for data structures that are protected by locks, we should have a way to verify that we are holding (and releasing) locks around our accesses. We might consider either a static and dynamic approach or both.

A static approach could use LLVM's Thread Safety Analysis. A dynamic approach might involve adding a facility in a diagnostic build with session flags for various locks and annotations at every read/write of a data structure's field.

---

## WT-8207: Add assert for excessive amounts of rollbacks in CPP framework

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2021-10-06T04:13:17.000+0000
- **Updated:** 2022-04-05T01:04:40.000+0000

**Description:**
Inside the cppsuite framework, there are numerous ways where we can rollback the current active transaction. It is possible that a particular test can be stuck when rollbacks are continuously called.

This ticket aims to provide a better method of asserting when a test continuously performs transaction rollbacks by moving the rollback tracking functionality inside the CPP framework (most likely thread_context.cxx) with an internal counter tracking the number of rollbacks.

---

## WT-8215: Architecture Guide updates for PM-2564

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-10-08T04:32:32.000+0000
- **Updated:** 2022-11-09T00:58:58.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8229: Improve the logging under WT_VERB_TRANSACTION tag

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haseeb Bokhari
- **Created:** 2021-10-13T00:47:07.000+0000
- **Updated:** 2022-04-05T00:59:23.000+0000

**Description:**
While debugging a recent BF, verbose logging for transactions was enabled and it was noticed that very few messages are printed under `WT_VERB_TRANSACTION` tag. Most of the messages are related to prepared transactions only. The aim of this ticket would be to review how we can improve the amount of logs printed under `WT_VERB_TRANSACTION` tag, especially for rollback and write conflict scenarios.

---

## WT-8231: Add dist script support for CMake formatting

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alison Felizzi
- **Created:** 2021-10-13T06:29:24.000+0000
- **Updated:** 2023-11-23T22:29:11.000+0000

**Description:**
No different to how we enforce styling over our C sources, we should additionally be ensuring our CMake sources meet an expected format. This ticket will introduce a new dist script (that can be run under `dist/s_all`), to parse our CMake list files and format them nicely.

**Definition of Done:**
 * Determine a tool to use to format CMake sources (suggested: cmake-format)
 * Define a style configuration
 * Implement a 'dist' script that invokes the CMake format tool
 * Perform an initial format of the CMake sources

---

## WT-8247: Add the missing compiler warnings for CPP files related to the cppsuite

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, cppsuite, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2021-10-20T00:48:34.000+0000
- **Updated:** 2022-08-12T03:03:02.000+0000

**Description:**
In WT-8196, we added a few compiler warnings for the CPP files. This ticket should investigate if we should add the following flags:
```
-Waggregate-return
-Wall
-Wextra
-Wshadow
-Wsign-conversion
```

**Definition of done:** CMake and autoconf files are updated accordingly. Errors should be fixed as well. Be mindful that it will impact both the test/cppsuite and bench/workgen files. The workgen files should be addressed as part of WT-8263.

---

## WT-8262: Make it default for most tests to generate statistics. Default stats to JSON

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod, wt-ideas
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2021-10-21T05:48:56.000+0000
- **Updated:** 2022-04-05T01:14:02.000+0000

**Description:**
Environment variables need to be frequently set to enable stats collection with python, perf and other testing. We could potentially make collection of statistics ON by default. We should also make the output for statistics JSON by default since we primarily use json format to visualize the statistics.

If it is not doable to a larger extent, we could at least make an option available for python tests to generate stats, e.g. adding an option "--enable-statistics" to the runner script.

---

## WT-8263: Enable compiler warnings for CPP files related to workgen

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2021-10-21T11:22:17.000+0000
- **Updated:** 2022-08-29T21:45:11.000+0000

**Description:**
In WT-8196, we are overwriting `AM_CXXFLAGS` to an empty list, hence no flags are taken into account. This ticket should remove that workaround and fix each warning triggered by the flags defined in `AM_CXXFLAGS`.

**Definition of done:** The cpp files in bench/workgen compile with the flags defined by `AM_CXXFLAGS` and no warnings are generated during the compilation.

---

## WT-8267: add table add and drop to format tester

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2021-10-23T21:11:03.000+0000
- **Updated:** 2024-03-12T04:25:49.000+0000

**Description:**
A goal in WT-3445 was to add support for table add, drop and rename operations, as well as sweep server testing, to the format tester. That work was not done, and this ticket is being created to track that remaining work.

Now that format supports multiple files, it should be possible to add the additional add/drop/rename test support. One solution might be to create a "table active" array to parallel the array of TABLE references, which would flag if a particular table is available to worker threads.

---

## WT-8276: Add cppsuite tests to the code coverage-report

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2021-10-26T00:13:39.000+0000
- **Updated:** 2022-11-15T22:39:57.000+0000

**Description:**
It will be useful to extend the coverage report we have for WiredTiger to include the cppsuite. It could be "make check" tests on the cppsuite, or some more of the existing tests.

---

## WT-8277: Change salvage to resolve prepared records.

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-d
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2021-10-26T00:25:26.000+0000
- **Updated:** 2025-03-18T02:50:23.000+0000

**Description:**
Prepared values can be found on salvaged pages, and they can exist forever as there is no mechanism for removing prepared values other than a future transaction being resolved. In the case of salvage, there may not be any such future transaction.

In keeping with the principle that it's better for salvage to keep data than throw it away, salvaged prepared values should be committed. The change will be to convert prepared values to ordinary updates when a salvaged page is read into memory.

To make this change, we need to enter the salvage write generation into per-table metadata at salvage time. If that write generation is present, it can be compared to the page's current write generation when a page is read in; pages from before the salvage run should have any prepared values committed.

---

## WT-8278: Change salvage to remove history store records

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-d
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2021-10-26T00:28:25.000+0000
- **Updated:** 2025-03-18T02:48:33.000+0000

**Description:**
After salvage completes, there may be history store records that are incorrect with respect to the salvaged data, and subsequent behavior may be undefined.

Change salvage to discard all history store records for the salvaged object.

---

## WT-8279: Change salvage to merge history store records after salvage completes

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2021-10-26T00:31:53.000+0000
- **Updated:** 2022-04-05T00:55:16.000+0000

**Description:**
After salvage completes, there may be history store records that are incorrect with respect to the salvaged data, and subsequent behavior may be undefined.

A relatively easy solution is to discard all history store records for the salvaged object, as described in WT-8278.

A potential improvement is to add an additional pass to salvage, where records for the object in the history store are compared against the records in the salvaged object, as this may allow the retrieval of data which salvage would have otherwise discarded.

---

## WT-8305: Update eviction to check for weak hazard pointers and invalidate them when it attempts to evict a page

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-10-29T00:36:23.000+0000
- **Updated:** 2022-04-05T00:50:28.000+0000

**Description:**
When eviction is run on a page it first checks if the page is evict-able by scanning for `active` hazard pointers on the page. With the new `weak` pointer interface eviction should either save a list of the relevant weak pointers on the page, or re-scan for them, then just before it evicts the page it should downgrade all the hazard pointers from `weak` to `invalid`.

Scope:
* Determine if a saved list of weak pointers will be used or if an additional scan is required
* Determine a location for the downgrade to occur
* Implement the functionality
* Write a test if possible

---

## WT-8307: Investigate management of hazard pointer array resizing

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-10-29T00:37:05.000+0000
- **Updated:** 2022-04-05T01:07:37.000+0000

**Description:**
No description

---

## WT-8308: Placeholder: Create follow on tickets for implementation

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-10-29T00:37:26.000+0000
- **Updated:** 2022-04-05T01:10:06.000+0000

**Description:**
No description

---

## WT-8309: Add relevant statistics for hazard point resolution

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-10-29T00:37:41.000+0000
- **Updated:** 2022-04-05T01:11:52.000+0000

**Description:**
We will add new statistics to track the following values:
 * maximum weak hazard pointers in use across all active sessions searched
 * time spent resolving uncommitted updates in `txn_commit`
 * time spent resolving uncommitted updates in `txn_rollback`
 * uncommitted update commit modified committed update in the history store
 * uncommitted update rollback modified committed update in the history store

We will add a Python test to ensure these statistics function as expected.

---

## WT-8310: Investigate optimising hazard pointer storage for multiple keys on a single page updated by the same transaction

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-10-29T00:38:56.000+0000
- **Updated:** 2022-04-05T01:19:37.000+0000

**Description:**
No description

---

## WT-8311: Validate impact on in memory storage engine

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-10-29T00:39:12.000+0000
- **Updated:** 2022-04-05T01:26:24.000+0000

**Description:**
No description

---

## WT-8334: Architecture Guide updates for PM-2631

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-11-02T22:55:18.000+0000
- **Updated:** 2022-04-05T01:26:39.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8432: Add version information to the WT checkpoint metadata

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** post-mortem
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Brian Lane
- **Created:** 2021-11-22T01:23:51.000+0000
- **Updated:** 2022-04-05T00:42:01.000+0000

**Description:**
With some of the recent issues related to upgrade/downgrade, it would be beneficial if we stored the WT version information as metadata somewhere.

---

## WT-8445: Add VLCS/FLCS cases for test_checkpoint/recovery-test.sh

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** neweng
- **Components:** Column Store
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** David Holland
- **Created:** 2021-11-24T02:12:03.000+0000
- **Updated:** 2025-08-20T21:57:25.667+0000

**Description:**
Currently test_checkpoint/recovery-test.sh only does row-store. Running this test for VLCS and FLCS is probably worthwhile. This mostly involves adding cases for it in evergreen.yml.

There's one wrinkle: while `-t r` (for rows) is passed as an argument to the script, it's also hardcoded into the second invocation, so a little tidying/adjustment of the argument handling will be needed.

---

## WT-8453: Enable cursor caching for cursors used to resolve uncommitted updates

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2021-11-25T00:41:34.000+0000
- **Updated:** 2022-04-05T01:03:25.000+0000

**Description:**
The change for WT-8306 introduces resolving uncommitted updates if they are not in memory already. A cursor is used to find these updates and later when the cursor is closed we decide not to cache it. This is because we have deterministic python tests that expect a certain number of cursors to be cached and reopened.

We would like to cache the cursors since it is good for performance. This ticket will explore how to get the tests passing with cached cursors and then enable the caching of these cursors.

---

## WT-8458: Support JSON-encoded message strings for 'WT_CONNECTION::debug_info' messages

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alison Felizzi
- **Created:** 2021-11-25T05:11:24.000+0000
- **Updated:** 2022-04-05T01:00:45.000+0000

**Description:**
We have a set of verbose message functions that dump various states of WiredTiger, typically invoked via the `WT_CONNECTION::debug_info` interface. These functions use the `wt_msg` function which directly calls the `WT_EVENT_HANDLER:handle_message` interface function, avoiding any of the styling/formatting enforced by other internal message interfaces.

This is problematic as it doesn't adhere to the new JSON configuration option `json_output`. Non-JSON strings can still possibly be passed via the `WT_EVENT_HANDLER:handle_message` function when the connection is configured with `json_output=[message]`.

**Definition of Done:** Investigate and implement a solution to create JSON output in the verbose dump methods when `json_output=[message]` is enabled.

---

## WT-8469: Handle resolved updates getting evicted before commit/rollback finishes

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2021-11-29T00:40:47.000+0000
- **Updated:** 2022-04-05T00:41:33.000+0000

**Description:**
The update resolution as part of commit/rollback does a fast path or a slow path. In both cases, since the strong pointer is released when we start to process the next update, there is nothing preventing the already processed updates from being evicted.

This ticket will focus on designing and making changes for an approach to resolve this issue.

---

## WT-8471: Don't rollback resolve-search in case of being the oldest transaction pinning cache

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2021-11-29T02:08:19.000+0000
- **Updated:** 2022-04-05T01:19:56.000+0000

**Description:**
`__wt_txn_is_blocking` can roll back a transaction if it is the oldest running and holding back eviction. The search operation for slow path update resolution can hence be rolled back if this were to happen. This should not be allowed as we resolve at the time of commit/rollback, which would eventually lead to unpinning the transaction.

We have already made sure we do not roll back prepared transactions. We will have to also make sure we do not roll back the search we are doing as part of resolving updates.

---

## WT-8492: Add a debug option to let the reconcile page to retain the time window of an update

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2021-12-03T06:49:03.000+0000
- **Updated:** 2022-04-05T01:21:12.000+0000

**Description:**
There are a couple of issues recently raised due to the presence of time windows values on the value cell of a page. Currently, it is not possible to easily control to let the reconcile operation retain the time window values.

Scope:
* Create a new debug option to let the reconcile write the time window values.
* Add a test to demonstrate this option.

---

## WT-8524: Create a python script that can diagnose the structure of btree from a coredump

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2021-12-09T05:01:48.000+0000
- **Updated:** 2022-04-05T01:17:50.000+0000

**Description:**
When diagnosing an issue from a coredump, WT developers would sometimes need to look at the structure of the btree associated with the table to understand the root problem. Currently the process is through a local python script attached as `wt_debug_script.py`, and sourcing the python script inside a gdb (attached to the core).

**Aim:** Create a python script that has the ability to print out all the update list, insert list and all the in-memory pages from a coredump.

---

## WT-8531: Add functionality to wiredtiger open verify metadata configuration that returns an error if the metadata is inconsistent

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2021-12-09T23:08:38.000+0000
- **Updated:** 2022-04-05T00:53:48.000+0000

**Description:**
This ticket is created as further work from WT-8149. `wt4156_metadata_salvage` was failing due to it finding a key in the metadata for `table:blah.wt` and then attempting to open the table, which internally opens the underlying file `file:blah.wt`. However salvage didn't recover the file component of the metadata, just the table.

We should add functionality to the metadata verification configuration option which tells WiredTiger to verify the metadata file for consistency on open and returns an error to the user if the metadata file is inconsistent.

Scope:
* Determine what is "consistent" metadata
* Implement a consistency checker
* Tie it to the verify metadata configuration and create a new error message
* Test

---

## WT-8538: Reduce the amount of duplicated artifacts in Evergreen jobs

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2021-12-10T01:01:33.000+0000
- **Updated:** 2023-03-07T22:46:51.000+0000

**Description:**
Every Evergreen task uploads the full wiredtiger source directory as an artifact, including compiled binaries, but because these tests are using the binaries built in the compile stage we already have a copy of them saved in the compile task's artifact page. This results in needless duplication of artifact files.

If we instead only uploaded the files produced by the tests (for example `WT_TEST` folders) and provided a symlink to the original compile artifacts, we could save approximately 880MB and 2 minutes of CPU time per task, which across 100 tasks comes out to 80GB and 3.5 hours of CPU per patch build.

**Definition of Done:** Individual tests no longer upload an entire .tgz of the wiredtiger folder, and only upload the delta between their wiredtiger folder and the original `compile` folder.

---

## WT-8573: Update Architecture guide for configuration precompiling

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2021-12-15T16:25:24.000+0000
- **Updated:** 2022-04-05T00:49:12.000+0000

**Description:**
Add an architecture guide subsection for WT-8571 (if and when it is completed). Material can be taken from the design document if that is still correct when the implementation is complete.

---

## WT-8582: Expand extent lists to collect GC information

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** Not Applicable
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2021-12-17T22:09:38.000+0000
- **Updated:** 2024-10-10T11:21:34.000+0000

**Description:**
Each checkpoint has a set of extent lists, which track the blocks used by the checkpoint, and blocks available/discarded in the file. To support GC, we'll need more information about other objects (and blocks within them) referred to, directly or indirectly, by the checkpoint.

One likely solution would be to create an additional extent list that travels with every checkpoint, with a set of triplets: objectid/offset/size to indicate which pieces are in use for past objects.

This ticket would seem to require a checkpoint format change, so the ramifications for upgrade/downgrade compatibility need to be examined. This ticket's work should include a way to debug/dump the new extent list.

---

## WT-8612: Consider merging import compatibility test script into the main compatibility test script

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2021-12-29T00:52:32.000+0000
- **Updated:** 2022-04-05T00:44:32.000+0000

**Description:**
The import compatibility script (`test/evergreen/import_compatibility_test.sh`) was created a while ago by emulating the main compatibility script (`test/evergreen/compatibility_test_for_releases.sh`), and customized to support import specific testing needs. As the whole test procedure is quite similar between the 2 compatibility scripts, there are benefits of merging them together to reduce duplicated code and improve maintainability.

**Acceptance Criteria:** Using a single script to run both import and other existing compatibility tests. No testing coverage should be sacrificed.

---

## WT-8628: Make a decision on test suites running with diag/non-diag builds

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2022-01-04T23:36:14.000+0000
- **Updated:** 2022-04-05T01:09:51.000+0000

**Description:**
A question was raised: is it our intent that the test suite run in both diagnostic and non-diagnostic modes?

If we only run in diagnostic mode, we can simplify the test suite and remove the checks for a diagnostic build. If we want to run in both modes, then evergreen.yml should clearly run on at least one common platform in non-diagnostic mode.

Diagnostic builds are significantly different from non-diagnostic builds, and running in both modes would give better test coverage.

**Acceptance Criteria:** Make a call on whether test suites should always run with diagnostics, without, both, or randomly choose.

---

## WT-8644: Preload failures leak cache blocks

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-c
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2022-01-07T15:29:32.000+0000
- **Updated:** 2022-07-28T06:04:12.000+0000

**Description:**
If the btree open preload fails, cache blocks will be reported as leaked on close.

---

## WT-8681: Remove dead code handling WT versions earlier than 3.2.0

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2022-01-14T01:41:15.000+0000
- **Updated:** 2023-03-07T22:47:20.000+0000

**Description:**
Discovered during refactor work in WT-8673. WT-5630 adds a `#define WT_MIN_STARTUP_VERSION` which prevents WiredTiger from starting on versions older than 3.2.0, which should make code checking for older versions (such as `WT_LOG_V3_VERSION`) no longer required.

Check that values such as `WT_LOG_V3_VERSION` are now redundant and remove any dead code that follows from their removal.

---

## WT-8685: Investigate how to have all the plots from the cpp suite tests on Atlas

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-01-16T12:33:02.000+0000
- **Updated:** 2022-11-02T00:44:53.000+0000

**Description:**
After WT-8624, each cpp test from the cpp suite can generate data that is the source of plots visible in Evergreen. However, this is not convenient as one needs to open each test on Evergreen to analyze them. The goal here is to have one page that contains all the plots so we can have a look at all of them at once.

**Acceptance Criteria:** All plots are displayed in the best manner on one page, and each test output updates the plots on the new page.

---

## WT-8729: the block cache code doesn't support object create

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-01-26T00:25:38.000+0000
- **Updated:** 2022-04-05T01:01:32.000+0000

**Description:**
The block-cache layer doesn't support object creation; the btree layer calls directly into the block layer to create objects.

This is incorrect, and the block-cache layer needs to be in the loop, assuming we can create objects in other than the local store.

---

## WT-8738: Architecture Guide updates for PM-2710

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2022-01-27T04:25:38.000+0000
- **Updated:** 2022-04-05T01:06:21.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8739: Architecture Guide updates for PM-2711

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2022-01-27T04:27:41.000+0000
- **Updated:** 2022-04-05T00:54:37.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-8744: Clean up handling of performance stats in cppsuite tests

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2022-01-28T01:10:42.000+0000
- **Updated:** 2022-04-05T00:48:25.000+0000

**Description:**
Identified in WT-8450. The cppsuite test `hs_cleanup_stress` tracks some performance statistics and fails if they are not inside an expected range. However due to the nature of the test this can lead to random and false errors.

WT-8450 has been delivered to resolve a ci-blocker, and this ticket is to perform further clean up. These tasks include:
* Removing the min and max settings for stats in hs_cleanup_stress.txt if they are no longer needed
* Checking all other cppsuite tests to see if other stats can be treated the same way
* Moving `stat_db_size` into its own test with a constant number of writes rather than running for a constant amount of time

---

## WT-8763: Logging and extension API improvements for storage sources

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** neweng, tiered-storage, wt-s3-ext
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Ruby Chen
- **Created:** 2022-02-02T06:04:59.000+0000
- **Updated:** 2025-12-03T06:07:46.303+0000

**Description:**
We would like to have a separate verbose category for our storage source extensions. This is used when configuring WiredTiger's verbosity settings. We wanted to create a category similar to the current `Tiered` category.

This is to separate the debugging messages from that of Tiered Storage or other categories.

**Acceptance Criteria:** A new category is created and changes are tested.

---

## WT-8779: Investigate bounds for compression/decompression statistics

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-c
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2022-02-04T01:07:00.000+0000
- **Updated:** 2022-07-28T05:47:06.000+0000

**Description:**
What does it mean to have a compression ratio of more than 1? WiredTiger doesn't compress data if the compression returns larger than the input. Is this an issue with the statistics, or something else? It is worth investigating.

---

## WT-8793: enhance logging-based testing

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-02-08T17:09:15.000+0000
- **Updated:** 2022-04-05T01:12:22.000+0000

**Description:**
With the merge of WT-8601, WT logging-based testing (including backup and recovery) has been reduced, as test programs configuring transactional timestamps are less likely to configure logging (and vice-versa).

The simplest fix is probably to update some test programs to run with mixed logged and non-logged tables as MongoDB Server does, doing appropriate repeatable read testing for each. That would give us better testing of the oplog/collection combination found in MongoDB Server.

---

## WT-8800: Upgrade 3rdparty python test support libraries

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2022-02-09T14:43:29.000+0000
- **Updated:** 2022-04-05T00:59:03.000+0000

**Description:**
Libraries found in test/3rdparty haven't been updated for 8 years. If they are still in use, it would be good to update them, or kill them off if they are no longer needed.

There are six files in question: concurrencytest-0.1.2, extras-0.0.3, testscenarios-0.4, discover-0.4.0, python-subunit-0.0.16, testtools-0.9.34.

We directly use:
* **testscenarios** (which has a new version 0.5)
* **discover** (which is now part of unittest in Python3 - the import in `run.py` can be removed)
* **concurrencytest** - no new version, allows for the -p "parallel" option

---

## WT-8808: Data validation failure in test_timestamp_abort

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-c
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2022-02-10T16:12:06.000+0000
- **Updated:** 2023-04-04T05:37:34.000+0000

**Description:**
This ticket is a follow-on for WT-8392. There have been two failures on `ubuntu2004-small` hosts, both in `test_timestamp_abort -s` (the stress variant) where the local record is absent from the local table after crash and recovery.

The suspicion is that there is a file system bug. Both failures indicate that the local update completed, which means it wrote its insert into the WT log and that record would have been written to the OS buffer cache before returning.

In WT-8392, debugging was added and turned on for stress runs to record `pwrite` operations and print the thread and key written.

---

## WT-8810: enhance static test suite checkpoint tests

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-02-10T23:56:40.000+0000
- **Updated:** 2022-04-05T00:53:33.000+0000

**Description:**
Checkpoints are different for log- and checkpoint-durability files. It would be reasonable to enhance the python test suite checkpoint tests to run in both logged and not-logged modes.

---
