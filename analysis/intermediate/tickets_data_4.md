# WiredTiger Tickets Data - Group 4 (WT-8811 to WT-9808)

## WT-8811: test_log04 enhancement

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-02-10T23:59:34.000+0000
- **Updated:** 2022-04-05T01:26:15.000+0000

**Description:**
The python test suite test `test_log04.py` was added to smoke-test logging with timestamp configurations.

Sue LoVerso notes:
> I think it also would be useful to have a "update the values", flush the log, and then restart/reopen the connection and check the values. It would be test that a restart is properly retaining the timestamp, not timestamp and recovery/replay is applying all the necessary information. (Note I did not say you should checkpoint there, just flush the log.)

---

## WT-8813: Improve access to methods requiring an exclusive handle

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-02-11T00:47:10.000+0000
- **Updated:** 2022-04-05T01:11:37.000+0000

**Description:**
WT-8695 returns EBUSY to all WT_SESSION methods requiring exclusive access to a handle, if the cache for the file is dirty, in order to prevent data consistency errors. This change won't be reversed, we have no plans to ever again make it possible for a method to checkpoint single files.

This ticket is for follow-on work for WT-8695 and WT-7750.

---

## WT-8834: Automatically update parameter values in WT doxygen pages

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2022-02-14T16:52:57.000+0000
- **Updated:** 2022-04-05T01:27:22.000+0000

**Description:**
Problem:

Some pages in the WiredTiger documentation include the default values for various configuration parameters. Some pages also discuss the min and max values. This is especially common on the tuning documentation (`docs/tune-*.dox`).

Currently this documentation has to be updated by hand when these values are updated. This is error prone, especially since changes to the defaults are automatically propagated to other parts of the documentation, making it easy for developers to assume there isn't anything else they need to change after updating `dist/api_data.py`.

WT-8831 is case where we missed updating the documentation and it took us several years to spot the error.

Solution:

We should automatically fill in these values from `api_data.py` as part of generating the documentation.

---

## WT-8881: It is possible to commit with a durable timestamp earlier than that of data read by the same transaction

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** David Holland
- **Created:** 2022-02-26T01:02:37.000+0000
- **Updated:** 2022-04-05T01:15:19.000+0000

**Description:**
WT-8747 describes (at some length) a possible data consistency problem that can arise when transactions commit with durable timestamps after their commit timestamps and other transactions then read their writes and commit with earlier durable timestamps.

We believe that the problem itself does not affect MongoDB, and finding a way to prohibit these commits without interfering with MongoDB proved problematic, so WT-8747 was closed by documenting the issue as a hazard.

It would be better for these commits to be prohibited, so ideally at some future point this should be revisited, probably not until after the issues related to out-of-order updates have been resolved more thoroughly.

This github pull request contains most of a solution that works for WT; theoretically it should work for MDB but might not (has not been explicitly tested): https://github.com/wiredtiger/wiredtiger/pull/7485

It works by tracking the most recent durable timestamp read by each transaction and requiring the commit-time durable timestamp to not be before this.

It is known to be missing one bit -- it does not track the durable timestamps of values read from history, which in theory can be after stable. (Durable timestamps at or before stable don't actually need to be tracked as committing with a durable timestamp at or before stable is prohibited. The implementation tracks all durable timestamps to avoid unnecessary reads of stable, but the impact of the missing bit is still limited by this consideration.) No existing test covers this situation; I wrote a somewhat messy one but it trips on other issues. Might upload it here later; if I never get to that it can be rederived by hacking up the test_durable_ts04.py in the existing pull request.

(Note that the earlier approach in WT-8747 of prohibiting reads of data between its commit and durable timestamps breaks important optimizations in MongoDB and isn't workable.)

---

## WT-8916: Enable S3 extension build and test on the Windows

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** neweng, wt-s3-ext
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2022-03-08T07:03:37.000+0000
- **Updated:** 2023-05-03T18:27:11.000+0000

**Description:**
Summary: Make code and build changes to enable running S3 extension on Windows.

Motivation: Some effort was done as part of WT-8720 to get the extension compiling and working on Windows. There is a PR linked to that ticket and some comments detailing where the efforts were. Continue the work and get the extension working on windows. Also add the testing to a windows test variant on evergreen, if it makes sense to do so. Python tests for tiered storage (including local_store) are skipped on Windows for now, because of the way extensions are loaded as external libraries. Possibly they can be supported if the extension is built-in on Windows?

This work should only be done if the benefits of testing on Windows and making the build system and code platform-independent outweighs the effort involved.

Note: Do consider the costs involved in interacting with S3. Aim for a balance between the costs and benefits.

Is this issue urgent? No

Acceptance Criteria: The extension builds (and tests) on Windows. Evergreen has some form of testing for S3 on windows.

---

## WT-8937: Allow print_python_stack_trace.py to print traces for failing python tests on Windows

- **Status:** Backlog
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sid Mahajan
- **Created:** 2022-03-14T00:16:29.000+0000
- **Updated:** 2023-07-14T03:50:49.000+0000

**Description:**
Summary: The print_python_stack_trace.py script prints backtraces on Linux. I didn't see a core file on Windows (as a result of a failing Python test). Hence, the scope of WT-7661 was limited to obtaining the traces in the logs of failing Python tests on Ubuntu only.

Motivation: Does this affect any team outside of WT? No. Is this issue urgent? No.

Acceptance Criteria: Allow print_python_stack_trace.py to print traces of failing Python tests on Windows in the evergreen logs.
- Investigate how to obtain the core file on windows.
- Create a class in the script to dump the traces from the core file to the logs.

---

## WT-8974: Investigate naming/functionality of __wt_txn_publish_durable_timestamp

- **Status:** Open
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** code-quality, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2022-03-21T01:56:35.000+0000
- **Updated:** 2024-05-02T04:00:45.000+0000

**Description:**
As part of WT-8366, we realised that `__wt_txn_publish_durable_timestamp` is somewhat mis-named. Alex G summed it up best:

"It assumes that everyone knows that a commit timestamp implicitly becomes the durable timestamp if a different one isn't set. I think it could be renamed to `wt_txn_publish_user_timestamp`, and its intent would be clearer. It publishes any timestamp for a transaction that needs to be published."

We should check if (a) this is what we really want the function to do, and (b) rename it accordingly.

---

## WT-8976: Allow print_python_stack_trace.py to print traces for failing python tests on macOS

- **Status:** Open
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** Jie Chen
- **Reporter:** Sid Mahajan
- **Created:** 2022-03-21T04:35:10.000+0000
- **Updated:** 2024-05-02T04:06:26.000+0000

**Description:**
Summary: The print_python_stack_trace.py script prints backtraces on Linux.

Since the core dumps on macOS hosts have quotes around them, e.g. "dump_test_checkpoint.696.core". This makes it harder to load the core in lldb. I have created BUILD-14820 to get rid of the quotes.

Motivation: Does this affect any team outside of WT? No. Is this issue urgent? No.

Acceptance Criteria: Allow print_python_stack_trace.py to print traces of failing Python tests on macOS once the linked build ticket is done.

---

## WT-8977: Tiered Storage python tests shouldn't check contents of dir_store cache

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** refinement, tiered-storage, tiered-storage-misc
- **Components:** Tiered Storage
- **Assignee:** unassigned
- **Reporter:** Keith Smith
- **Created:** 2022-03-21T15:52:32.000+0000
- **Updated:** 2026-01-02T04:46:37.490+0000

**Description:**
Summary: Remove checks for the existence (or non-existence) of specific files in the `storage_source` cache directory from the WiredTiger-level tiered storage tests in the python suite.

If there are important test cases that are only covered in those tests, those cases should be moved to the `storage_source` tests, such as `test_tiered06.py` or `test_s3_connection.cpp`.

Motivation: The choice of whether and how to cache content from object storage on the local file system is implemented in the `storage_source`. So tests at the WiredTiger API level shouldn't embed assumptions about what is or isn't cached and how it is cached.

If we feel it should be a requirement for all storage sources to implement a cache in this way, then we should implement that functionality above the storage source.

---

## WT-9023: Create a cpp test for prepared updates

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** stability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-03-28T22:35:00.000+0000
- **Updated:** 2022-11-08T04:18:39.000+0000

**Description:**
Summary: Create a new test in which prepared updates are written concurrently to stress the cache and trigger eviction which will force the uncommitted prepared updates to be written to disk. The test will then call rollback for those prepared updates and make sure nothing remains on disk. If possible, we could also only commit a percentage of the prepared transactions.

Motivation: Does this affect any team outside of WT? It could if things don't work as expected. If the problem does occur, what are the consequences? Data corruption. Is this issue urgent? It is urgent to make sure the code around prepared transactions is correct.

Acceptance Criteria: A new test has been added to the cpp suite.

Suggested Solution:
- Create a new cpp test using `test/cppsuite/create_test.sh`.
- Configure a small cache size in the test configuration file.
- Override the `insert_operation` function which will create prepared transactions that are too big for the cache.
- Roll back those transactions once they have been evicted.

---

## WT-9042: commit/durability timestamps can race, perform potentially unnecessary checks

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, code-quality, group-b, requirements
- **Components:** Transactions
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-03-31T20:24:25.000+0000
- **Updated:** 2026-01-12T00:40:01.395+0000

**Description:**
The commit/durability timestamp validation code and global set timestamp code can race, and we perform potentially unnecessary checks (since oldest is necessarily less than stable, it's unnecessary to check both).

---

## WT-9043: commit/prepare timestamp checks against read timestamps are only done in #diagnostic builds

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-b, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-03-31T20:33:20.000+0000
- **Updated:** 2022-08-01T19:17:21.000+0000

**Description:**
Commit and prepare timestamps are not supposed to be set after any active read timestamp in the system. This is checked in HAVE_DIAGNOSTIC builds, and includes acquiring the global transaction lock.

Ideally, we would always do the check and without locking the global transaction structures.

We should investigate if we can track the current largest read timestamp on the system. As this is not required to be a definitive check, it's only "best efforts", we wouldn't need to lock or atomically CAS the "current largest read timestamp" value, instead we could let it race (and live with an increased number of cache shootdowns).

---

## WT-9066: format uses all_durable to set the stable timestamp

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-b, stability
- **Components:** Test Format
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-04-05T00:09:00.000+0000
- **Updated:** 2025-03-18T02:55:20.000+0000

**Description:**
format uses all_durable to set the stable timestamp (which is both expensive and dangerous because all_durable can move backwards).

Update format to use the known list of commit timestamps to set stable.

---

## WT-9113: More efficient cell encoding when adjacent keys differ by incrementing last byte by 1

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mathias Stearn
- **Created:** 2022-04-08T10:16:26.000+0000
- **Updated:** 2024-05-07T06:04:40.000+0000

**Description:**
Summary: WT should have a special key cell encoding to cover the case where two adjacent keys are identical except that the last byte has been incremented by 1.

Motivation: This will have the biggest impact on columnar indexes because it has the combination of frequently having adjacent keys matching this pattern, combined with frequently storing very small values (0-5 bytes will be very common in cases we care about), so that every byte of overhead is significant.

This also occurs in the RecordStore (`key_format=q`), although a) the values tend to be much larger, so the overhead is lower, and b) we aren't using prefix compression there, which might be a gate for this encoding.

It isn't urgent, but it would be nice to have prior to columnar GA (which is currently targeting for 6.3).

Suggested Solution: The best option is to let two adjacent value cells without an intermediate key imply that the prior key gets its last byte incremented by 1. This means that for a page with all incrementing keys, you only need to store the first key, then all remaining data is just values with no overhead.

---

## WT-9145: Add donor_stable_timestamp in WT_SESSION::create(import=())

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** tiered-storage
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexey Anisimov
- **Created:** 2022-04-13T04:35:43.000+0000
- **Updated:** 2025-12-03T06:07:49.628+0000

**Description:**
Summary: The format for the new option is: `donor_stable_timestamp=<ts value>`. It will be used in a scenario similar to:
```
session->create("table:A", "import=(enabled=true,donor_stable_timestamp=<TS>,metadata_file=WiredTiger.export")
```
If the caller wants a rollback to stable performed then the caller is required to provide WiredTiger with a timestamp value. If a value is not provided then rollback to stable will not be called. If a value is provided then we will provide that timestamp to rollback this specific table to that value.

The scope of work for this ticket is:
1. Add the new option to `WT_SESSION::create(import=())`.
2. Do rollback to stable of the imported table if the donor timestamp has been provided.
3. Add a test for the new option.

---

## WT-9148: Investigate the use of HWASAN instead of ASAN

- **Status:** Open
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-04-14T01:53:50.000+0000
- **Updated:** 2023-10-24T22:03:59.000+0000

**Description:**
Hardware-assisted AddressSanitizer (or HWASAN) is a tool similar to AddressSanitizer, but based on partial hardware assistance.

I was wondering if we should look into it to see if it could add some benefits to our testing.

In WT-8946, we found that the usage of ASan could trigger OOM issues on ARM and it would be great to check if using HWASAN could mitigate this.

---

## WT-9170: Shutdown RTS skips trees that have never been checkpointed

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, code-quality, group-a
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** David Holland
- **Created:** 2022-04-20T01:39:17.000+0000
- **Updated:** 2022-07-15T01:21:20.000+0000

**Description:**
The shutdown-time RTS apparently skips trees that have never been checkpointed.

The reason is at line 1628 of rts.c: "The rollback to stable will skip the tables during recovery and shutdown in the following conditions. 1. Empty table. 2. Table has timestamped updates without a stable timestamp." But the test it uses for "empty table" is whether it has a checkpoint address, which doesn't seem to be what was intended.

This is not a serious problem because RTS runs again after starting up, but it does mean we waste time writing out unstable data during shutdown.

I'm not sure what the right test for an empty table is (or if it's even worth checking) so I'm not sure what the right fix is, but it seems straightforward.

---

## WT-9172: remove force configuration for WT_CONNECTION.set_timestamp API

- **Status:** Blocked
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-04-20T18:48:31.000+0000
- **Updated:** 2024-06-17T18:52:29.000+0000

**Description:**
The only MDB Server use of the undocumented force configuration to the WT_CONNECTION.set_timestamp API is scheduled to be removed, at which time the API itself should be removed.

---

## WT-9178: Remove has_XXX booleans from the global transaction state

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-04-21T18:11:31.000+0000
- **Updated:** 2022-04-27T01:08:00.000+0000

**Description:**
Now that a timestamp of zero is reserved (WT-8973), we should be able to eliminate the has_durable_timestamp, has_oldest_timestamp and has_stable_timestamp booleans from the WT_TXN_GLOBAL structure, and use a timestamp of 0 as the not-set value instead. (We might be able to delete has_pinned_timestamp as well, but I doubt it.)

These are all shared memory updates, living on the same cache line, and likely impact timestamp set/get performance.

---

## WT-9182: Explore what should be the correct way to calculate upd_memsize in the durable history era

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-04-22T01:29:01.000+0000
- **Updated:** 2023-03-28T04:17:12.000+0000

**Description:**
Summary: upd_memsize is calculated as the total size of update chain saved in reconciliation and is used as a heuristic to decide whether we need to split the page or not. Before durable history, it only contains the size of update chain that needs to be restored. While in durable history era, this value becomes larger because it also includes the size of the update chains that are moved to the history store.

Motivation: With the current implementation, we are splitting more than before durable history. We need to understand its performance impact.

Acceptance Criteria: Testing the performance of the system with different ways of calculating upd_memsize and deciding which is the best strategy.

Suggested Solution: I have tried only calculating the size of uncommitted updates. The other way we can do is to only calculate the size of update chains that are going to be restored.

---

## WT-9187: Create consistent per-session I/O statistics

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2022-04-22T14:46:55.000+0000
- **Updated:** 2023-10-24T22:03:07.000+0000

**Description:**
Summary: Add per-session statistics that would allow a user to compute I/O throughput.

Motivation: WiredTiger maintains a handful of statistics for each session. This includes `WT_STAT_SESSION_BYTES_READ` (the number of bytes read into the cache, i.e., the sum of the uncompressed sizes of the block read into the cache) and `WT_STAT_SESSION_READ_TIME` (the time spent reading data from the file system).

Suggested Solution: It would be most useful to provide matching statistics for each of the above (and the write counterparts):
- `WT_STAT_SESSION_FILE_BYTES_READ`: total size of data read from the file system (sum of sizes of compressed blocks).
- `WT_STAT_SESSION_CACHE_READ_TIME`: time spent loading uncompressed data into the cache.

For this to be useful in MongoDB, there will need to be some server work to collect and deliver this data.

---

## WT-9193: test_gc02 WT_ROLLBACK: conflict between concurrent operations

- **Status:** Open
- **Type:** Build Failure
- **Priority:** Major - P3
- **Labels:** BB-Tools, stability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** xgen-buildbaron-user
- **Created:** 2022-04-25T23:55:30.000+0000
- **Updated:** 2025-02-11T00:10:25.000+0000

**Description:**
unit-test on macos-1014 failure. Host: macos-1014-142.macstadium.build.10gen.cc. Project: wiredtiger. Commit: d20a5bcc.

Error: `_wiredtiger.WiredTigerError: WT_ROLLBACK: conflict between concurrent operations` in test_gc02.test_gc02.test_gc at line 92 in large_updates.

---

## WT-9198: Improve the way we update the stable timestamp in the cpp suite

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, cppsuite
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-04-26T01:01:45.000+0000
- **Updated:** 2023-03-23T22:01:49.000+0000

**Description:**
Summary: We want to manage better the way we update the stable ts in the cpp suite. Currently we update the stable ts periodically and maintain a lag with the latest generated ts. However, we don't take into consideration what is happening in the system, i.e. running transactions. This means we could update the stable ts to a date that is beyond the commit ts one of those running transactions.

Motivation: We had issues in the past, see WT-9115 where a transaction would have a commit ts that is more recent than the stable ts.

Acceptance Criteria: Find a way to update the stable ts so it does not get beyond the commit ts of a running transaction.

Suggested Solution: Do something similar to what test/format does: check each running transaction and their commit ts, select the min and set the stable ts to a lower value than the min.

---

## WT-9245: Too many logs from compatibility_test_for_releases.sh

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Trivial - P5
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-04-27T04:29:32.000+0000
- **Updated:** 2022-05-02T01:05:31.000+0000

**Description:**
Many logs are generated during the execution of the script which makes it hard to parse/read them.

When `test checkpoint` is executed against `develop`, debug recovery logs like `Recovery oldest_timestamp 2f0` are visible. We may want to disable those logs if we judge them as not necessary.

---

## WT-9269: failed: test_config11 assertion error on macos-1014 [wiredtiger @ 4932879a]

- **Status:** Open
- **Type:** Build Failure
- **Priority:** Major - P3
- **Labels:** BB-Tools, stability
- **Components:** Evergreen
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** xgen-buildbaron-user
- **Created:** 2022-05-02T06:47:45.000+0000
- **Updated:** 2025-10-25T00:04:10.759+0000

**Description:**
unit-test on macos-1014 failure. Host: macos-1014-84.macstadium.build.10gen.cc. Commit: 4932879a.

Error: `AssertionError: 28618566 not greater than 52428800.0` in test_config11 assertGreater check for current_cache_usage > max_cache_size / 2.

---

## WT-9270: Ideas to improve the code style/flexibility of the cpp suite

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, cppsuite, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-05-02T08:28:28.000+0000
- **Updated:** 2023-10-09T03:18:34.000+0000

**Description:**
Summary: We could improve the code style of the cpp suite. This ticket lists some of the ideas we have so far:
- Align coding style with MongoDB server and the S3 extension (case, header includes, forward declarations, early exits, units in field names)
- Instead of having the framework generate the values when one performs inserts/updates, let the users provide the values (WT-9314)
- Use smart pointers and RAII wherever possible
- Make sure each class definition is in its own file (WT-9383)
- Rename functions/variables (such as `save_operation`) where it makes sense (WT-9387)
- Allow collections to handle all types supported by WiredTiger
- Identify and remove cases of code duplication and boilerplate
- Simplify the interface of `set_tracking_cursor` (WT-9388)
- Use Doxygen on the cpp suite
- Review string handling in cppsuite
- Set the minimum value of `key_size` or `value_size` to 1
- Review `can_rollback()` implementation
- Clean up overloaded functions in the CppSuite (WT-9333)
- Consider creating symlinks instead of copying config files to build directory
- Avoid using WT internal macros like WT_DECL_RET in cppsuite test code (WT-11793)

---

## WT-9285: tree walk code locks deleted WT_REFs twice.

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-05-05T01:02:27.000+0000
- **Updated:** 2022-05-26T14:10:17.000+0000

**Description:**
There is significant overlap between `__wt_btcur_skip_page()` and `__wt_delete_page_skip()` with respect to handling fast-truncate pages.

We are locking each fast-truncate WT_REF structure two times as cursors walk the tree which blocks other threads more than is necessary.

---

## WT-9286: Enhance existing GDB functions to be consistent

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2022-05-05T07:10:50.000+0000
- **Updated:** 2023-10-26T21:56:51.000+0000

**Description:**
Summary: Some GDB functions were added to the WiredTiger tree as part of WT-8158. One is a gdb-style script the other a Python extension. We should review the content, and ensure the naming and functionality are consistent.

Scope:
- Investigate converting `dump_row_int.gdb` into a python gdb script.
- Review content in all 3 scripts and ensure the content, functionality and style is consistent between them.
- Convert `wt_debug_script_update` to use `gdb.Commands`.
- Update `wt.gdb` to source `dump_insert_list.py`.

---

## WT-9288: Include external functions in GDB auto loading

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2022-05-05T07:26:26.000+0000
- **Updated:** 2023-11-13T04:45:28.000+0000

**Description:**
Summary: WiredTiger recently added some GDB functions to the repository (WT-8158). There are some external GDB functions that might also be useful for WiredTiger developers, for example in MongoDB's mongo.py. It would be nice to figure out how to auto download and load that file for WiredTiger developers as well.

Bonus points for finding other GDB functions that make GDB usage better and including them as well. Double bonus points for optionally checking for updates and downloading new versions.

Testing: Test that the auto-download works and the functions become available in GDB automatically.

Documentation: Add some comments to the `tools/gdb` directory describing what is going on and how it works.

---

## WT-9294: Understand performance of cursor creation

- **Status:** Open
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** perf-improvement, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2022-05-06T17:41:01.000+0000
- **Updated:** 2023-07-31T20:36:19.000+0000

**Description:**
Summary: In WT-8366 we allow the use of numerical values rather than strings when passing timestamps into WiredTiger API calls, providing 15+% performance improvements on Intel and larger benefits on ARM.

Are there other places we could make similar changes and also achieve good speedups? Cursor creation is another frequent operation, but perhaps any performance issues there are already addressed by the WiredTiger cursor cache.

In a hallway conversation, Geert Bosch said that one reason for the MongoDB-level cursor cache is to avoid config string parsing. So maybe there is a way to make similar optimizations in cursor creation and eliminate the need for two levels of cursor caching.

The goal is to provide a forum to collect knowledge of cursor/cursor caching implementers and, if desirable, collect and analyze supporting data, then create follow-on tickets for any identified work.

---

## WT-9330: Add observability on the last thread that accessed a session

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** refinement, supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Josef Ahmad
- **Created:** 2022-05-17T10:30:26.000+0000
- **Updated:** 2024-05-21T04:03:19.000+0000

**Description:**
This will help diagnose a class of issues described in SERVER-61116. Today, we have a wealth of diagnostics on the WT sessions and on the thread state, however it's not straightforward to map a session to the thread that last accessed it.

It would be useful to report this session -> thread mapping in a way that's readily accessible both on a live mongod process and in a core dump, such as those generated when a MongoDB integration test times out.

---

## WT-9333: Clean up overloaded functions in the CppSuite

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, cppsuite, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2022-05-18T00:20:42.000+0000
- **Updated:** 2022-08-12T03:15:56.000+0000

**Description:**
The CppSuite framework code provides default implementations for the following operations: populate, insert, update, remove. The expectation was that the majority of tests would only need to overload 1 or maybe 2 of these operations. However a number of tests have been written recently that overload most of the operations.

This could be caused by a few things but the main likely reason is that the default implementation doesn't meet the needs of the test writer.

Scope:
- Investigate tests with a large number of overloaded operations.
- Try to improve the default implementation.
- Remove the overloaded implementation.

---

## WT-9346: format "cp: Argument list too long" failure

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-a
- **Components:** Test Format
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-05-20T13:52:49.000+0000
- **Updated:** 2025-03-18T02:56:18.000+0000

**Description:**
When the test program format tests salvage, it copies files for future replay. The copy includes WiredTiger log files, and if there are enough log files it can overflow the shell's ARG_MAX buffer size.

---

## WT-9352: Improve eviction performance during RTS

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** stability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Vamsi Boyapati
- **Created:** 2022-05-23T07:31:01.000+0000
- **Updated:** 2023-05-15T01:51:11.000+0000

**Description:**
Summary: When RTS is in progress, based on the RTS activity, cache utilization could reach peak levels. RTS is currently configured to ignore cache bounds as RTS failure will lead to recovery failure which could be catastrophic. RTS is also configured not to participate in eviction as it could increase the recovery latency.

Motivation: Affects log keeper system. Long recovery time consequences.

Acceptance Criteria: RTS should fine tune its activity based on the cache usage like aiding in eviction when cache usage is high.

---

## WT-9375: Update Windows Evergreen testing to generate dump files

- **Status:** Open
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, dev-prod, group-a, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-05-30T01:39:49.000+0000
- **Updated:** 2023-10-30T00:51:27.000+0000

**Description:**
A patch build generates a segmentation fault in the windows python unit test. However, there is no core dump to investigate.

Ideally a crash from a segfault or other similar failure on Windows would generate a mini-dump that could be investigated to understand more about the state of the system on failure.

---

## WT-9386: Review and update the namespaces and naming conventions in the cppsuite

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, cppsuite
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Tammy Bailey
- **Created:** 2022-05-30T05:06:16.000+0000
- **Updated:** 2023-04-04T05:07:14.000+0000

**Description:**
From the Google C++ Style Guide: "The name of a top-level namespace should usually be the name of the project or team whose code is contained in that namespace."

Review the use of namespaces and their naming conventions in the cppsuite source code and make changes as necessary (e.g. `test_harness` -> `cppsuite`).

---

## WT-9387: Rename classes, methods, instance variables using OOP conventions

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Tammy Bailey
- **Created:** 2022-05-30T05:19:31.000+0000
- **Updated:** 2023-04-04T05:07:39.000+0000

**Description:**
Rename classes, methods, and variables using the conventions below. Refer to the naming section of the google style guide for details.
- Class names should be nouns that are simple and descriptive.
- Methods should be verbs and describe an action the class can perform.
- Variable names should be mnemonic. One-character variable names should be avoided.
- Update variable names to use units where it makes sense.
- Do not use acronyms and/or abbreviations.
- Names should be descriptive enough to be immediately understandable by a new reader.

---

## WT-9390: Code quality improvements in the cppsuite: Part two

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, cppsuite
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Tammy Bailey
- **Created:** 2022-05-30T05:38:16.000+0000
- **Updated:** 2023-11-21T00:49:31.000+0000

**Description:**
We identified several ways we could improve the quality of the code in the cppsuite. In this ticket, we will make the following changes:
- Update to use early exits where necessary.
- Review and remove use of preprocessor macros.
- Review use of type deduction.

---

## WT-9391: Replace WiredTiger macros with C++ functions in cppsuite

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, cppsuite
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Tammy Bailey
- **Created:** 2022-05-30T05:50:13.000+0000
- **Updated:** 2022-07-13T01:16:49.000+0000

**Description:**
The cppsuite code is tightly coupled with the WiredTiger code and makes heavy use of the WiredTiger macros. Ideally, only the code in the `storage` directory should be linked with WiredTiger. The scope of this ticket is to remove WiredTiger macros where possible and replace them with C++ functions, such as replacing `testutil_assert` with `assert`.

There are also `#define` that use underlying WiredTiger macros that should be revisited, i.e `SCHEMA_TRACKING_TABLE_CONFIG`.

---

## WT-9399: Code quality improvements in the cppsuite: Part three

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2022-05-31T01:39:00.000+0000
- **Updated:** 2022-06-09T05:14:13.000+0000

**Description:**
We identified several ways we could improve the quality of the code in the cppsuite. In this ticket, we will make the following changes:
- Make sure we are using logger.h for all traces, not std::cout.
- The checkpoint operation (as of WT-9384) has one thread running by default. It would be better to default to 0 threads so there are no implicit threads running that aren't defined in the config files.

---

## WT-9403: Add more format stress tests to run for less time in Evergreen

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** Evergreen
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Tammy Bailey
- **Created:** 2022-05-31T07:09:25.000+0000
- **Updated:** 2025-04-09T07:16:30.000+0000

**Description:**
Summary: Our `format` stress tests catch a lot of issues in WiredTiger but they are long running tests that take up to six hours to complete. We would like to add another group of `format` tests that run for less time, so we can get the results faster.

Acceptance Criteria: This ticket is complete when the `evergreen` YML is updated to include the additional `format` stress tests.

Suggested Solution: Keep the existing 12 long running `format` tests in the Ubuntu 20.04 Stress Tests distro and add additional tests that run for one hour each. We will initially start with one distro and can expand to more architectures if the additional testing proves valuable.

---

## WT-9414: Add information about ref state transitions to developer docs

- **Status:** Open
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** code-quality, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2022-06-02T05:36:55.000+0000
- **Updated:** 2023-10-24T03:48:49.000+0000

**Description:**
David Holland did a great write up of the various states a WT_REF can be in, and the transitions allowed.

We should add that to our architecture guide at http://source.wiredtiger.com/develop/arch-index.html.

---

## WT-9419: Replace the WiredTiger PRNG with an xoshiro variant

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-06-05T15:09:05.000+0000
- **Updated:** 2022-06-05T21:49:55.000+0000

**Description:**
There are better PRNGs than the one we chose when originally developing WiredTiger. Replace the current PRNG with xoshiro128**, then instead of seeding it multiple times in the test programs, seed it once and then generate independent streams using the jump operator.

https://prng.di.unimi.it/

Note the page comment: "We suggest to use SplitMix64 to initialize the state of our generators starting from a 64-bit seed, as research has shown that initialization must be performed with a generator radically different in nature from the one initialized to avoid correlation on similar seeds."

---

## WT-9443: Implement a basic repeatable reads checker in the CppSuite

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2022-06-08T03:21:23.000+0000
- **Updated:** 2023-03-27T10:07:35.000+0000

**Description:**
Summary: Similar to what exists in test format we should implement a basic repeatable reads checker in the test suite.

Motivation: This should increase the testing coverage by providing snapshot isolation validation.

Suggested Solution: The CppSuite is flexible. One way would be to implement a new repeatable_read_operation that either:
1. Creates a read transaction and sleeps for N seconds and then reads again and validates that it sees the same data.
2. Creates a read timestamp transaction and rollback and restart that transaction at a later time.

Acceptance Criteria:
- Design and implement a repeatable reads operation or component.
- Update the basic tests to use it in some way.

---

## WT-9444: Enhance flags.py to generate the flags variable declaration

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2022-06-08T04:02:44.000+0000
- **Updated:** 2023-03-27T03:05:44.000+0000

**Description:**
Summary: The idea behind this ticket is to enhance flags.py to control the flag variable declaration of each flag. This will allow us to correctly use flags in the case that the size needs to change depending on the number of flags.

Keith Bostic: "The fundamental problem here is that flags.py doesn't control the variable declaration, and so we rely on engineers to ensure the STOP line information matches the required number of flags, which is error prone. I'd recommend changing it so flags.py owns the variable declaration."

See the attached PR: https://github.com/wiredtiger/wiredtiger/pull/7991

---

## WT-9449: Add a stage to the "upload artifact" function in evergreen.yml to fail the test if the artifact is too big.

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2022-06-09T00:48:01.000+0000
- **Updated:** 2023-04-04T05:07:32.000+0000

**Description:**
This is a suggested change. Currently we have numerous tests in evergreen which upload artifacts of any size. In testing, a 12.5GB artifact was found from one of the perf tests.

To prevent future tests uploading large artifacts we can consider adding a test killing stage to the 'upload artifact' function in the evergreen.yml. A shell.exec step could check the file size and fail if it exceeds a threshold (e.g. 15GB).

Before this goes in, some discussion is required and a reasonable limit would need to be agreed on.

---

## WT-9451: Test to demonstrate append only workloads

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2022-06-09T06:39:27.000+0000
- **Updated:** 2022-11-13T21:59:19.000+0000

**Description:**
Summary: Recently we have had a couple of issues related to MongoDB oplog table (logged table) data. This table is modified with append only workloads and truncates the old data using session truncate API.

As part of this ticket, we want to write a standalone WT test using the cppsuite to demonstrate append only workloads similar to MongoDB oplog table.

The newly added test can be easily configured to a particular size of the table data where the size can be varied.

---

## WT-9460: Documentation updates for PM-2942

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2022-06-14T04:59:58.000+0000
- **Updated:** 2022-06-20T00:21:50.000+0000

**Description:**
This ticket is to update all types of documentation API Guide, Programming Guide, and Architecture Guide related to the project.

---

## WT-9461: Documentation updates for PM-2943

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2022-06-14T05:02:00.000+0000
- **Updated:** 2022-06-20T00:21:54.000+0000

**Description:**
This ticket is to update all types of documentation API Guide, Programming Guide, and Architecture Guide related to the project.

---

## WT-9464: Documentation updates for PM-2947

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2022-06-15T03:42:06.000+0000
- **Updated:** 2022-06-20T00:22:03.000+0000

**Description:**
This ticket is to update all types of documentation API Guide, Programming Guide, and Architecture Guide related to the project.

---

## WT-9469: CppSuite: Tune search_near_01 stress to make it more stressful on the new variant.

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2022-06-15T23:42:44.000+0000
- **Updated:** 2022-08-12T03:03:08.000+0000

**Description:**
We upgraded to a more powerful host to run the cppsuite stress tests, we should tune the existing workloads to make them more stressful.

In this ticket we will tune search_near_01_stress. The best way to do that is to change the parameters, create a patch build, view the ftdc, repeat.

Scope:
- Tune the workload.
- Fix bugs found.

---

## WT-9470: CppSuite: Tune search_near_02 stress to make it more stressful on the new variant.

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2022-06-15T23:43:12.000+0000
- **Updated:** 2022-08-12T03:03:06.000+0000

**Description:**
We upgraded to a more powerful host to run the cppsuite stress tests, we should tune the existing workloads to make them more stressful.

In this ticket we will tune search_near_02_stress. The best way to do that is to change the parameters, create a patch build, view the ftdc, repeat.

Scope:
- Tune the workload.
- Fix bugs found.

---

## WT-9471: CppSuite: Tune search_near_03 stress to make it more stressful on the new variant.

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2022-06-15T23:43:40.000+0000
- **Updated:** 2022-08-12T03:03:05.000+0000

**Description:**
We upgraded to a more powerful host to run the cppsuite stress tests, we should tune the existing workloads to make them more stressful.

In this ticket we will tune search_near_03_stress. The best way to do that is to change the parameters, create a patch build, view the ftdc, repeat.

Scope:
- Tune the workload.
- Fix bugs found.

---

## WT-9478: Extension libraries should be installed in a (versioned) subdir

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** David Holland
- **Created:** 2022-06-18T02:05:49.000+0000
- **Updated:** 2025-03-25T02:07:35.000+0000

**Description:**
With the current CMake configury, the plugin libraries for extensions are installed into $PREFIX/lib without any kind of versioning. This means that while you can have two versions of the main WiredTiger library installed, you cannot have more than one version of the extensions at a time and installing a newer version will overwrite the previous copies.

The best thing to do is to create a subdirectory of lib/ that includes the version number (e.g. $PREFIX/lib/wiredtiger-10.0.2) and install them in that directory; then each version's plugin libraries can coexist safely.

Also currently the plugin libraries seem to get built with no SONAME — this might be ok for plugin libraries if they get installed into their own directory.

---

## WT-9482: Document how page splits work in WiredTiger

- **Status:** Open
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** SEKB, code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2022-06-20T04:42:24.000+0000
- **Updated:** 2022-08-29T21:45:29.000+0000

**Description:**
Page splits are a complex part of the WiredTiger system. Further complicating matters, there are different aspects of the system that split in different ways — for example, when a page is written from cache to disk it is generally split into multiple pages, but also when those multiple pages are created each needs a new reference in a parent page.

This ticket captures some work to document what a page split is in WiredTiger, bonus points for adding content about how page splits work.

---

## WT-9496: Generate documentation front page with correct current and previous versions

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2022-06-24T07:46:46.000+0000
- **Updated:** 2022-08-12T03:02:26.000+0000

**Description:**
When running "dist/s_docs -l" to generate documentation for a new OSS release, in the generated `index.html` file the "current" version is replaced with the new version (e.g. 11.0.0) to be released but the "previous" version is left unchanged (e.g. 3.2.1). It is expected the "previous current" version (e.g. 10.0.0) would be moved to the "previous" version instead (the cascading effect).

We should make the expected behavior about current/previous version settings on the documentation front page automated and built into the release workflow.

---

## WT-9498: Move the documentation build into the CMake build directory

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, neweng, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2022-06-26T17:14:58.000+0000
- **Updated:** 2022-08-12T03:02:28.000+0000

**Description:**
We should move the WiredTiger documentation build into the standard CMake build directory.

---

## WT-9499: Identify storage HW corruption from extensions

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-06-27T02:29:03.000+0000
- **Updated:** 2024-05-21T04:13:22.000+0000

**Description:**
Summary: This ticket comes after WT-9311. It would be great to indicate when an extension returns a failure due to a checksum error due to an HW failure.

Motivation: See WT-9311.

Acceptance Criteria: Each extension should be investigated and specifically the compression and encryption ones. Find out where it is likely to get an HW corruption and ensure it is correctly indicated in the logs in case of a failure.

---

## WT-9517: Randomise the collections a thread is assigned to in the insert operation of the cppsuite

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality, cppsuite
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-06-29T11:07:21.000+0000
- **Updated:** 2022-08-12T03:02:29.000+0000

**Description:**
Summary: In order to vary the data generated by the default insert operation defined in the cppsuite, we could randomise which collections the threads are working on.

Currently, each thread is assigned to a defined set of collections and inserts some key/value pairs into each of them, resulting in a uniform distribution of data across all collections.

Instead, we could use different pseudo-random generations. The chosen distribution could be specified through the configuration file.

Acceptance Criteria: The configuration can be specified through the test configuration file and the insert operation uses it to insert data in the collections.

---

## WT-9519: Use random cursors in the update operation of the cppsuite

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality, cppsuite, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-06-29T11:38:54.000+0000
- **Updated:** 2023-02-07T02:26:25.000+0000

**Description:**
Summary: In the `update_operation` of the database_operation class, after selecting a random collection to update, we generate a random key using the `get_key_count` function on the collection. Instead, we could use a random cursor, find a random record and retrieve the key. This is already done in the `remove_operation` of the same class.

Acceptance Criteria: Use random cursors instead of relying on `get_key_count`.

Suggested Solution: See the `remove_operation` implementation.

---

## WT-9520: Randomise read logic in the cppsuite

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, cppsuite, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-06-29T11:40:23.000+0000
- **Updated:** 2023-02-07T02:26:25.000+0000

**Description:**
Summary: In order to vary the interaction with the database in the default read operation defined in the cppsuite, we could randomise which key a thread reads.

Currently, each thread positions a cursor at the start of the collection and reads it, key after key, until the transaction is completed and resets the cursor.

Instead, we could position the cursor on a random key and start reading.

Suggested Solution: It might be as simple as passing the random cursor configuration when creating the cursor: `cursors.emplace(coll.id, std::move(tc->session.open_scoped_cursor(coll.name, "next_random=true")));`

---

## WT-9523: Decouple the cppsuite code related to the timestamp manager

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** cppsuite, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-06-30T10:02:16.000+0000
- **Updated:** 2022-08-12T03:20:27.000+0000

**Description:**
Summary: The timestamp manager is currently defined as a component in the cppsuite. This means it can be enabled/disabled through the configuration file and has a `load`, a `run` and a `finish` phase. However, when we disable the timestamp manager, we don't really do it.

The `run` phase of the timestamp manager updates the oldest and stable timestamps; we should probably let the user have control over this. This could become a database_operation function. We would keep the timestamp manager as an interface with the WT APIs to interact with timestamps. It could become a singleton.

Acceptance Criteria: Decide on what to do with the timestamp manager so the code is less coupled and its purpose makes more sense.

---

## WT-9524: Revisit the purpose of the workload manager of the cpp suite

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** cppsuite, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-06-30T10:13:54.000+0000
- **Updated:** 2022-08-12T03:20:28.000+0000

**Description:**
Summary: The workload manager is in charge of populating the database and spawning the different threads defined by the test configuration file for each database operation.

Without the workload manager (i.e. when disabled), a test in a cppsuite probably does not make much sense. We should probably not let the user disable it but rather have it always on by default. Furthermore, the workload manager being a component does not make much sense either. Whatever is in the run function of the workload manager could fit inside the run function of the test class. Ultimately, the workload manager may disappear.

Acceptance Criteria: Decide whether the workload manager should remain a component and if its code would fit inside the test class.

---

## WT-9531: Documentation updates for PM-2958

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2022-06-30T22:32:37.000+0000
- **Updated:** 2022-07-22T01:12:56.000+0000

**Description:**
This ticket is to update all types of documentation API Guide, Programming Guide, and Architecture Guide related to the project.

---

## WT-9532: Documentation updates for PM-2959

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2022-06-30T22:36:04.000+0000
- **Updated:** 2022-07-22T01:13:01.000+0000

**Description:**
This ticket is to update all types of documentation API Guide, Programming Guide, and Architecture Guide related to the project.

---

## WT-9540: Add API to get the durable timestamp of an associated write

- **Status:** Backlog
- **Type:** New Feature
- **Priority:** Major - P3
- **Labels:** refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Daniel Gottlieb
- **Created:** 2022-07-01T14:30:15.000+0000
- **Updated:** 2023-10-26T15:32:25.000+0000

**Description:**
Summary: MongoDB, in the context of replicated data, at times needs to commit its reads. Knowing the timestamp of values that were read can simplify how MDB commits reads.

Motivation: Today MDB acquires a new WT snapshot and reads from the top of oplog to get an upper bound on what data may have been read. This is costly in multiple ways: it provides only an upper bound, requires acquiring a new snapshot + cursor + reverse cursor walk, and uses a new WT_SESSION on the same thread while the original may hold resources (which can lead to deadlock).

Acceptance Criteria: An API that can expose either individual durable timestamps for positioned cursors, or the max of all durable timestamps for updates done within a transaction. If the timestamp information has been wiped because it is smaller than the oldest timestamp, returning 0 is acceptable.

Suggested Solution: `WT_CURSOR::get_timestamp_and_value(WT_CURSOR*, int64_t* timestamp, WT_ITEM/value macros)`

---

## WT-9543: Rename the database operations of the cppsuite

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** cppsuite, neweng, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-07-05T02:05:56.000+0000
- **Updated:** 2024-01-04T23:32:37.000+0000

**Description:**
Summary: In the database_operation class, we define a set of operations: `insert_operation`, `update_operation`, `remove_operation`. There is some confusion about whether those represent an operation or a transaction. Indeed, we start and finish a transaction within those functions while a transaction is made of multiple operations. This is misleading.

Acceptance Criteria: Find a name that suits better what is actually done by those functions. In the long term, we may want to extract the transaction lifecycle outside those functions.

---

## WT-9548: Better RAII in unit tests

- **Status:** Backlog
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2022-07-06T00:07:50.000+0000
- **Updated:** 2022-07-14T02:29:15.000+0000

**Description:**
As part of the WT-9455 PR, there are a couple of places in the unit tests where we call `__wt_free(session, m.ovfl_track)`. These should be replaced with some sort of wrapper around the session to make sure this happens reliably and without having to remember to do it for each test where we use `conn.createSession()`.

---

## WT-9560: Remove forward compatibility checking on open

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** unassigned
- **Reporter:** Alexander Gorrod
- **Created:** 2022-07-08T02:54:00.000+0000
- **Updated:** 2024-05-22T02:42:42.000+0000

**Description:**
Summary: There is a check that is sometimes triggered when calling wiredtiger_open, to see whether the database files have been created by a more recent release of WiredTiger. We should remove that check.

Motivation: The versioning scheme for WiredTiger releases isn't intended to capture upgrade/downgrade requirements. The scheme we have for managing that is the `compatibility` configuration option to the `wiredtiger_open` API.

Acceptance Criteria: Fully understand upgrade/downgrade consequences of bumping WiredTiger release versions, and confirmed that they are compatible with MongoDB requirements.

Suggested Solution: Remove the compatibility check in `__conn_config_check_version`. The complexity is that this is a forward facing change — we need to figure out whether we'd also need to backport that change.

---

## WT-9565: Review API coverage in test-format

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, test/format
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexey Anisimov
- **Created:** 2022-07-08T07:37:55.000+0000
- **Updated:** 2023-04-11T05:10:27.000+0000

**Description:**
Summary: We should review the test/format application in light of the WiredTiger API, and identify places where test/format could provide additional testing and coverage if support for API features were added.

An example of where it might be extended is the new cursor bound API.

Output: The outcome of this ticket will be a catalogue of extensions we could make to test/format to increase the test coverage it gives, ideally with a recommendation about which extensions are worthwhile.

---

## WT-9574: Documentation updates for PM-2975

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Steven Vannelli
- **Created:** 2022-07-13T22:39:47.000+0000
- **Updated:** 2022-07-22T01:13:15.000+0000

**Description:**
This ticket is to update all types of documentation API Guide, Programming Guide, and Architecture Guide related to the project.

---

## WT-9585: Clean up unused imports and main function in python tests

- **Status:** Backlog
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-07-14T05:14:51.000+0000
- **Updated:** 2022-07-22T01:15:54.000+0000

**Description:**
Summary: There are many unused imports in the python tests. Clean up them and consistently add or remove the main function in python tests:
```python
if __name__ == '__main__':
    wttest.run()
```

Also check whether the test name matches with the file name and uri of the table created in the test.

---

## WT-9586: Python test rollback failures with retries should be more obvious in the output log.

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2022-07-14T15:19:17.000+0000
- **Updated:** 2023-08-03T22:57:37.000+0000

**Description:**
Summary: When a rollback failure occurs in the python tests, there are retries of the test (via WT-9063), that should limit the number of failed runs. However, when we get a rollback error, there's nothing in the output log about this. There is info in results.txt, but we need to download artifacts to see it. Also if running with an old version (< 3.7) of Python, the retry mechanism will not work. A good log message will remove any doubt.

Motivation: Does this affect any team outside of WT? No. Is this issue urgent? No.

---

## WT-9597: Teach cmake/ninja about api_config

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2022-07-18T22:21:51.000+0000
- **Updated:** 2023-03-27T03:06:08.000+0000

**Description:**
Currently, adding new config involves editing `api_config.py` and then manually running it (either in isolation, or as part of `s_all`). This can be confusing when the generated code doesn't change. It'd be good to teach `ninja` about this file, so we can re-generate various fields on the next build after the config is added.

---

## WT-9598: Teach cmake/ninja about s_stats

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2022-07-18T22:25:13.000+0000
- **Updated:** 2023-03-27T03:02:57.000+0000

**Description:**
Similar to WT-9597, adding new stats requires the user to manually run `s_stat` (either standalone or as part of `s_all`). It can be confusing when this doesn't work, and can cause really strange problems when people try to manually edit `wiredtiger.in` to add their stats. It'd be a good improvement for `ninja` to know when to re-run this.

---

## WT-9613: Make alter transactional with updates to other tables

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Yuhong Zhang
- **Created:** 2022-07-21T21:18:07.000+0000
- **Updated:** 2022-10-10T21:48:38.000+0000

**Description:**
Summary: Currently wiredtiger `alter` is not transactional. In MongoDB, when we convert indexes to unique or non-unique, the change to the index table metadata by `alter` will become visible right away before the changes to the catalog commits, causing a short window of inconsistency.

Motivation: Affects index uniqueness conversion in the server code. A workaround SERVER-68186 is investigated and it shouldn't be blocked by this.

How likely: This is an edge case. Only a checkpoint + shutdown or fcbis opening a backup cursor during the inconsistent window will cause this issue.

Consequences: The server will fassert and crash at startup.

Acceptance Criteria: The update to the index table metadata and the update to other tables' contents are atomic.

---

## WT-9615: Create data structures for the fail points

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-07-22T01:01:37.000+0000
- **Updated:** 2022-07-22T01:22:26.000+0000

**Description:**
Create the basic data structures for the fail point at the session and connection level including WT_FAIL_POINT and WT_FAIL_POINTS in connection and session.

---

## WT-9616: Create a header file where the file point data structures should live

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-07-22T01:04:14.000+0000
- **Updated:** 2022-07-22T01:22:32.000+0000

**Description:**
Create the header file and build that with cmake in diagnostic build.

---

## WT-9617: Add the fail point apis with empty implementation

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-07-22T01:06:11.000+0000
- **Updated:** 2022-07-22T01:22:37.000+0000

**Description:**
Add the empty implementation of fail point apis for diagnostic build and throws an unsupported error for production build.

---

## WT-9618: Define fail point apis skeleton in api_data.py

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-07-22T01:10:30.000+0000
- **Updated:** 2022-07-22T01:22:43.000+0000

**Description:**
Define fail point apis skeleton in api_data.py and be able to generate a test config for a fail point using s_all.

---

## WT-9619: Write a python script to generate the mapping for each defined fail point to a unique number

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-07-22T01:12:57.000+0000
- **Updated:** 2022-07-22T01:22:48.000+0000

**Description:**
The python script should be run in s_all. The generated mapping should live in a separate header file. Make sure the header file builds with cmake.

---

## WT-9620: Create a c implementation file where the evaluation functions and init functions live

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-07-22T01:22:02.000+0000
- **Updated:** 2022-07-22T01:23:43.000+0000

**Description:**
Make sure it builds with cmake.

---

## WT-9658: Add visible statistics for s3_store module

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, tiered-storage, tiered-storage-misc, wt-s3-ext
- **Components:** Not Applicable, Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2022-07-28T19:18:40.000+0000
- **Updated:** 2025-12-03T06:07:47.989+0000

**Description:**
Summary: We want to be able to track actual S3 requests (size of requests, how many, is input/output, cached or not) and have them appear as WT statistics. Right now many of these things are being tracked and reported via a logging mechanism. We really want it tracked in a standard way that can be accessed directly via statistics cursors and pushed into time series files that can be examined by t2.

TBD on whether we want to make a general solution for extension statistics, or something more specific that models perhaps current file system statistics.

Acceptance Criteria: Ideally, we'd have the ability to look at number and size of S3 put/get calls along with latency and access this with statistics cursors and t2. We'll need some simple (probably python) tests to show this.

---

## WT-9665: Project suggestion: Implement a WiredTiger b-tree visualizer.

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** project-suggestion
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2022-07-29T01:42:55.000+0000
- **Updated:** 2023-05-05T02:31:49.000+0000

**Description:**
As part of a Treehouse skunkworks project we demonstrated that a b-tree could be visualized in a cross platform javascript based UI.

The work was largely broken down into two parts:
1. Dumping a b-tree as JSON.
2. Implementing the visualizer.

We would like to see the visualizer become an actual tool and this ticket serves as a suggestion for a project to be planned around that. We believe this tool can serve as both an education and debugging tool.

Some interesting features:
- Opening WT database directories from the UI and skipping the JSON dump part.
- Opening WT corefiles from the UI.

---

## WT-9668: Improve overlap detection in WT_MODIFY paths

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jordi Olivares Provencio
- **Created:** 2022-07-29T15:22:16.000+0000
- **Updated:** 2023-05-15T01:55:16.000+0000

**Description:**
Summary: The detection for the non-overlapping case in WT_MODIFY application is prone to false negatives.

Motivation: In MongoDB time-series inserts, the support for using WT_MODIFY is disabled due to performance being worse than a simple full document. One of the reasons for doing that is that WiredTiger is materialising all intermediate modifies in a transaction once it is out of the fast path. The inserts are non-overlapping and don't contain padding so they should be taking the fast single-pass path instead of the slower one.

Suggested Solution: An attached patch for WT_MODIFY improves the padding and overlap detection. It seems to pass the WT tests for WT_MODIFY application (test_cursor12.py).

---

## WT-9671: Investigate extremely varied checkpoint cleanup statistic in CppSuite test hs_cleanup.

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality, stability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2022-08-01T00:25:28.000+0000
- **Updated:** 2022-08-12T04:01:32.000+0000

**Description:**
After the CppSuite tests were moved to a new Evergreen variant and a subset were made more stressful (WT-8640), the `hs_cleanup` test's `cc_pages_removed` statistic is extremely varied at the end of test runs.

The performance plot shows that prior to a certain commit the statistic was noisy to begin with, but after WT-8640 the variance between commits jumped significantly.

Scope:
- Investigate why the statistic is so varied between runs.
- Create follow up ticket to fix the behaviour if possible.

---

## WT-9699: Spike: Investigate a solution to better pass keys and recno's within WiredTiger

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2022-08-02T00:19:22.000+0000
- **Updated:** 2022-08-15T00:58:03.000+0000

**Description:**
In WiredTiger we pass keys/recnos into a number of functions including those that have CBT's and or cursors passed into them. The cursor itself has a `key` member and a `recno` member, the CBT has a `tmp` field which often holds a key and a `recno` field.

This raises the question of why do we have a `key/recno` on the cursor but choose to pass keys alongside the cursor? Additionally if the cursor has a `recno` or a `key` and the CBT has a `recno` or a `tmp` which one should be used?

Scope:
- Investigate the scope of the problem, i.e. how many functions follow this pattern.
- Determine if storing the keys in the cursor or the CBT would make the code simpler.
- Ticket out tickets to improve the code.

---

## WT-9708: WT_REF::flags should never change from WT_REF_FLAG_INTERNAL to WT_REF_FLAG_LEAF or vice versa

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jeremy Thorp
- **Created:** 2022-08-03T02:20:45.000+0000
- **Updated:** 2022-10-10T03:22:51.000+0000

**Description:**
Summary: WT_REF::flags should never change from WT_REF_FLAG_INTERNAL to WT_REF_FLAG_LEAF or vice versa.

Motivation: Currently, in `__wt_btree_new_leaf_page()`, the flags in a WT_REF can change from WT_REF_FLAG_INTERNAL to WT_REF_FLAG_LEAF being set. The code would be safer from the risk of race conditions if WT_REF::flags never change from WT_REF_FLAG_INTERNAL to WT_REF_FLAG_LEAF or vice versa. However, the code changes required to do this are not straightforward.

Does this affect any team outside of WT? No. Is this issue urgent? No.

Acceptance Criteria: A code inspection shows that the WT_REF::flags can never change from WT_REF_FLAG_INTERNAL to WT_REF_FLAG_LEAF or vice versa, and asserts are in place to enforce the correct flag state where required.

---

## WT-9719: CppSuite tests that call try_rollback aren't honoring the configured op_count.

- **Status:** Open
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2022-08-05T04:17:16.000+0000
- **Updated:** 2022-10-10T03:02:29.000+0000

**Description:**
A recent change removed the op_count check from `try_rollback` which means transactions will rollback after a single operation most times. This isn't great as it reduces the overall stressfulness of the workload. We could convert those transactions to `try_commit` and remove `try_rollback` altogether so it is less confusing.

This requires discussion before it can be fixed.

---

## WT-9731: Add test case encouraging race conditions on collection creation

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2022-08-09T04:37:05.000+0000
- **Updated:** 2023-05-15T02:40:24.000+0000

**Description:**
As part of WT-9323 there was an issue with correctness when a collection is created and updated once at the same time as a checkpoint happens.

Add a test application that stresses those aspects of the system, to capture such bugs earlier in the development cycle in the future.

---

## WT-9754: Add testing coverage for newer versions of GCC and Clang

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2022-08-10T04:21:16.000+0000
- **Updated:** 2024-01-08T02:38:20.000+0000

**Description:**
Summary: We have lagged behind on supporting and testing newer versions of GCC and Clang in WiredTiger. Currently, we test GCC up to v9 and Clang up to v8. GCC 10-12 and Clang 9-15 became available and should be added to our support list.

We should use this ticket to add the testing coverage for newer versions. Any WT source code compile issues uncovered by the newer compiler versions should be addressed in separate tickets.

Acceptance Criteria: Testing coverage should be added for GCC versions 10-12, Clang versions 9-14 in WT standalone tests.

---

## WT-9780: Rationalize and tidy ref locking

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** David Holland
- **Created:** 2022-08-19T03:52:33.000+0000
- **Updated:** 2023-04-11T05:03:34.000+0000

**Description:**
Summary: There are WT_REF_LOCK and WT_REF_UNLOCK macros, but they are not used consistently. Furthermore, there's no WT_REF_TRYLOCK, and consequently the trylock operation is open-coded in several places in different ways.

We should add a WT_REF_TRYLOCK, and check that all ref state changes that correspond to locking operations use the lock and unlock macros. Also, the memory barriers that lock and unlock operations should ordinarily have are not always present.

Motivation: Tidiness and readability.

Acceptance Criteria:
1. The tidyup part is done when all the ref state changes that correspond to lock and unlock operations but don't use the macros have been updated.
2. The concerns about refhist spam are resolved.
3. The concerns about memory barriers need further investigation.

---

## WT-9784: WiredTiger cache stuck logic abort transactions (some times) that are not blocking the eviction

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2022-08-22T03:27:52.000+0000
- **Updated:** 2022-09-13T08:54:07.000+0000

**Description:**
Summary: Eviction cache stuck unnecessarily rollbacks the transactions that are not blocking the eviction.

Whenever eviction is not able to progress, it checks for any older transactions that are blocking the eviction and rollbacks them to avoid cache being stuck in `__wt_txn_is_blocking` function. This function rollbacks the current session that is performing the eviction if the `oldest` transaction in the system or the `snapshot pinned transaction` is the oldest transaction.

This process of rolling back the transaction with a snapshot of pinned transaction that has the oldest id doesn't avoid the cache stuck as these transactions can be new and all the newer transactions can have the oldest id as the pinned transaction.

Motivation: More likely whenever a long-running transaction is running in the system. Unnecessary transaction rollback needs to re-run the transaction.

---

## WT-9798: Fill in function TODO comments

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2022-08-25T15:16:15.000+0000
- **Updated:** 2023-10-26T03:02:09.000+0000

**Description:**
In WT-8274 the `dist/function.py` script was extended to pass over all the source files. This resulted in a lot of function header comments being added to non-core library code (like tests, examples, utility files, etc).

There are many functions with this as their header:
```
/*
 * <function_name> --
 *     TODO: Add a comment describing this function.
 */
```

Those missing comments should be filled in with something useful.

---

## WT-9800: Enhance bulk load to support multiple threads

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2022-08-25T21:09:28.000+0000
- **Updated:** 2022-08-26T01:36:34.000+0000

**Description:**
Summary: WiredTiger has an optimized implementation that allows for efficient ingestion of data into an empty table, when that data is inserted in-order.

It would be useful for some applications to be able to do such a load via multiple threads.

Motivation: Graph databases in particular care about load speed.

---

## WT-9808: Fix suite_subprocess.runWt for tiered storage

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** tiered-test-part2
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2022-08-26T15:23:38.000+0000
- **Updated:** 2025-12-22T05:23:14.065+0000

**Description:**
In getting the tiered hook to run on the bulk of test/suite tests in WT-9741, there were a large number of tests that failed when running runWt. As an expedient move, any tiered test that calls runWt is marked as 'skipped'. This ticket is to remove that designation if possible, and resolve the underlying problem. Tests that don't make sense in tiered_storage could of course be individually skipped. See `FIXME-WT-9808` in the code.

There's probably some mixup from the fact that the main test runs with tiering, and the "wt" command is running with or without tiering and tiered vs. non-tiered files don't match the configuration.

---
