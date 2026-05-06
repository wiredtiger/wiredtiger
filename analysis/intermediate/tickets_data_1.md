# WiredTiger Tickets Data - Group 1 (WT-999 to WT-6076)

## WT-999: Test hot backup with a log path

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Michael Cahill
- **Created:** 2014-05-08T00:46:07.000+0000
- **Updated:** 2022-04-05T01:23:03.000+0000

**Description:**
If log files are being written somewhere other than the database home directory, hot backup will not work in the obvious way: the path will not be included in the file names returned by the backup cursor.

---

## WT-1559: backup log URIs need to use log path

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2015-01-14T20:36:48.000+0000
- **Updated:** 2022-04-05T01:24:31.000+0000

**Description:**
It looks like `wt backup` with log files in a different path is not working. References SERVER-16833.

---

## WT-2144: Deprecate support for overflow keys

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** streamline-standalone-wt
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Michael Cahill
- **Created:** 2015-09-28T04:19:45.000+0000
- **Updated:** 2022-04-05T00:57:27.000+0000

**Description:**
I'm opening this ticket to generate discussion...

We've hit issues again recently with overflow items, specifically overflow keys in WT-2119. Tracking overflow items across reconciliation calls for a page is painful, and overflow keys constrain what pages can be evicted during checkpoints (to avoid freelists becoming inconsistent before a checkpoint completes).

Some time back, we changed the handling of overflow items so that we could store values much larger than the configured "page_max" setting on a page. In other words, we made "page_max" a soft limit instead of a hard limit. FTR, MongoDB sets `leaf_value_max=64MB` because of a performance cliff with overflow values.

What if we change WiredTiger to treat `*_key_max = *_value_max = infinite`? In other words, what if we stopped writing overflow records at all?

---

## WT-3246: Expose internal thread states to allow applications to track idleness

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2017-03-30T19:17:06.000+0000
- **Updated:** 2022-05-26T01:04:03.000+0000

**Description:**
There have recently been changes in MongoDB aimed at improving diagnosability and debuggability by tracking some context for threads in the system. The particular information currently tracked is:
* A name for the thread
* Whether the thread is currently idle or active

It would be nice if threads in the WiredTiger library could share the same context with MongoDB. The API in MongoDB can be found here:
https://github.com/mongodb/mongo/blob/master/src/mongo/util/concurrency/idle_thread_block.cpp

---

## WT-3519: Review uses of API_END_RET_NOTFOUND_MAP

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Michael Cahill
- **Created:** 2017-08-18T01:51:18.000+0000
- **Updated:** 2024-04-05T02:32:29.000+0000

**Description:**
We have an obsolete comment in `api.h` that says:

    * In almost all cases, API_END is returning immediately, make it simple.
    * If a session or connection method is about to return WT_NOTFOUND (some
    * underlying object was not found), map it to ENOENT, only cursor methods
    * return WT_NOTFOUND.

That isn't how we're using `API_END_RET` vs `API_END_RET_NOTFOUND_MAP`. The latter should be used to map a `WT_NOTFOUND` from a handle cache lookup or metadata read to `ENOENT`, indicating that some requested URI does not exist. Some non-cursor calls explicitly expect to return `WT_NOTFOUND` (e.g., `WT_CONNECTION::query_timestamp`).

For calls where `WT_NOTFOUND` is not expected, mapping it to `ENOENT` doesn't make anything better. Review the use of these macros, fix the comment and consider adding new versions to make it clear which one to use.

In particular, `API_END_RET` should be the common case: potentially it could assert that `ret != WT_NOTFOUND`, then we could have `API_END_RET_NOTFOUND_OK` for methods that are expected to return `WT_NOTFOUND`, and limit `API_END_RET_NOTFOUND_MAP` to methods that look up URIs.

---

## WT-3626: Allow updates to be restored against an empty column store page

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** Column Store
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Michael Cahill
- **Created:** 2017-10-05T04:09:58.000+0000
- **Updated:** 2025-08-20T21:59:21.272+0000

**Description:**
There is special case code in `__wt_col_modify` that never sets `append` when restoring updates (i.e., it assumes updates will always be applied to existing column store records). As a consequence, we can't do update/restore eviction on column store pages with no visible data.

There is a check for empty pages with saved updates that are not row store leaf in `rec_write.c`. If that is removed, `test/format` workloads will fail with calls to `__wt_calloc` attempting to allocate empty arrays when restoring updates to column store pages.

---

## WT-3633: Have checkpoints be less IO hungry in low throughput workloads

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** 3.7BackgroundTask, SEKB
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2017-10-06T04:10:25.000+0000
- **Updated:** 2022-04-05T01:14:11.000+0000

**Description:**
The additional IO load triggered by checkpoints can lead to application operations average and maximum latency being affected. In workloads where checkpoints can be completed in a short period of time it may be beneficial to spread the IO load generated by checkpoints out over a longer period in order to reduce the influence checkpoints have on application operations.

---

## WT-3700: Test crashing during various non-CRUD operations

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2017-10-26T22:02:57.000+0000
- **Updated:** 2024-01-08T19:23:12.000+0000

**Description:**
We have a few tests that test crashing and recovery during normal insert operations such as `random_abort` and `timestamp_abort`. It would be good to test recovery in a variety of other situations (while still writing things to the log). Some of these would need to be new and independent tests and others could be enhancements to the existing tests.

Some situations are:
1. add abort tests for other table types such as LSM, column groups and indexes.
2. crash during schema operations such as create, drop, rename and alter.
3. crash a second time during WiredTiger recovery and then verify the next recovery.
4. crash during `WT_CONNECTION::close`
5. crash during a verify and/or salvage operation.
6. crash while a backup cursor is open.

These do not need to be randomized with each other but can be independent standalone tests. I think one of the harder parts is going to be knowing what the data should look like and confirming it after recovery runs. But I think the existing parent/child process model with fork() and kill() provides a good framework and starting point for these.

Probably we should use this ticket for brainstorming and create a separate ticket for each individual test.

---

## WT-3723: Add timestamp support to wtperf

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2017-11-06T15:53:50.000+0000
- **Updated:** 2024-01-22T00:15:19.000+0000

**Description:**
It would be good if `bench/wtperf` had timestamp support so that we could have Jenkins and the plots comparing inserts and other workloads with and without timestamps rather than having all timestamp related performance work going through MongoDB.

While I know the long term plan is to use workgen, I think this is a lot more urgent and should be added to the existing performance program we're using.

This support will need a new thread similar to `test/csuite/timestamp_abort` to coordinate the updates to timestamps.

---

## WT-3731: Avoid making a copy of table URI in every cursor

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** SEKB
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Michael Cahill
- **Created:** 2017-11-07T21:50:46.000+0000
- **Updated:** 2024-05-30T20:01:47.000+0000

**Description:**
In WT-3555 we tried to avoid making a copy of table URIs every time a cursor is opened.

That change turned out to be unsafe, because we open a "file:" cursor in that case and it does not reference the "table:" dhandle. As a consequence, the table dhandle can be closed by a sweep while there is a file cursor open with a pointer to its URI. WT-3730 reverted the original change.

Since there was a measurable performance improvement from this change, and we don't want table dhandles to be discarded when they are in use by cursors, we should investigate reinstating the change in WT-3555 but further having the cursor keep a reference to the "table:" dhandle and only releasing it on close.

---

## WT-3778: Enhance test timestamp abort to support modify operations

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2017-11-30T01:33:36.000+0000
- **Updated:** 2022-04-05T00:50:52.000+0000

**Description:**
It would be useful to expand correctness testing of cursor modify operations - one of the tests we have that validates data correctness is test_timestamp_abort - we should add support for `cursor->modify` operations to that test.

---

## WT-3873: Document legal page state transitions

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Neha Khatri
- **Created:** 2018-01-23T05:34:33.000+0000
- **Updated:** 2022-04-05T01:07:32.000+0000

**Description:**
There are various states possible for a page in WiredTiger. It would be useful to have page state transitions documented.

---

## WT-3951: Add bulk load and checkpoint abort test

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2018-03-02T21:14:34.000+0000
- **Updated:** 2022-04-05T01:29:53.000+0000

**Description:**
In WT-3948 we discuss a bug between bulk load and checkpoint. Create this ticket to add a test similar to `timestamp_abort` specific to those operations. I suggest looking at `timestamp_abort` because that program already has a checkpoint thread infrastructure in it and would be a place to start with a new program. The more interesting parts of this ticket would be figuring out what data is added and verification in the parent after the kill.

---

## WT-3965: Make schema operations atomic

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2018-03-07T03:30:40.000+0000
- **Updated:** 2025-11-06T04:35:24.306+0000

**Description:**
At the moment schema operations that involve complex tables can result in multiple updates to the metadata that aren't wrapped in a single transaction - which means that such operations aren't atomic.

We should use a transaction so those changes are grouped into a single unit of work.

We attempted to make this change in WT-3829, but that resulted in some unexpected failures for MongoDB. The broken code was disabled as part of WT-3964, we should understand those failures, and make a correct version of that change.

---

## WT-3983: Transaction isolation documentation should cover phantom reads and write skew

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** nyc
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2018-03-19T00:53:15.000+0000
- **Updated:** 2025-01-09T21:27:04.000+0000

**Description:**
Specifically make sure the distinction between write skew and phantom reads is clear.

http://source.wiredtiger.com/develop/transactions.html

---

## WT-4047: Document what split generations are, and how they work

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2018-04-19T03:39:18.000+0000
- **Updated:** 2022-04-05T01:26:20.000+0000

**Description:**
We've recently fixed a few bugs related to how we track when structures that have been replaced during a split is freed (WT-4037 for example). It would be nice to have some documentation describing how split generation tracking is expected to work.

We have quite a number of long comments in our code, my preference would be for this documentation to give an overview, and links to the comments in the code.

---

## WT-4054: Free transaction snapshot resources on session reset

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2018-04-24T13:08:18.000+0000
- **Updated:** 2022-04-05T00:50:57.000+0000

**Description:**
As a followup to WT-4052, it was pointed out that the transaction snapshot array can add up to some significant memory. The array is sized at the max number of sessions the system is sized for, and each element of the array is 8 bytes. MongoDB has `session_max=20000`, so the burden for each MongoDB session is 160K. If we could free this array on session reset, it should reduce memory footprint for idle sessions. We'd need to find a way to lazily allocate it on first use.

---

## WT-4066: Improve test coverage for timestamp races

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Michael Cahill
- **Created:** 2018-04-27T03:55:48.000+0000
- **Updated:** 2022-04-05T01:16:22.000+0000

**Description:**
In WT-4057, we had another report of a race between setting a timestamp and getting a valid transaction snapshot. It would be good to have better test coverage of this combination:

* a multi-threaded workload with checking of on-disk state;
* reading as-of timestamp with separate `begin_transaction` and `timestamp_transaction("read_timestamp=X")` calls; and
* exercising of `"round_to_oldest"` -- i.e., a test application where the oldest timestamp can move forward concurrent with a call to `timestamp_transaction`.

---

## WT-4073: Provide a way to fix app_metadata inconsistency after non-exclusive alter call

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2018-05-03T06:37:53.000+0000
- **Updated:** 2022-04-05T01:28:37.000+0000

**Description:**
WT-4033 added an undocumented `exclusive_refreshed` flag to an alter call. By setting this flag as false, applications can call alter on a table and expect to change `app_metadata` for a table without taking an exclusive lock. In such a case `app_metadata` is changed only for the table object, creating an inconsistency with the `app_metadata` for the underlying index, colgroup and file objects.

This ticket is to track effort in providing a way to fix this inconsistency.

---

## WT-4082: Track all memory allocations not intended for the cache

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** SEKB
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2018-05-11T05:42:03.000+0000
- **Updated:** 2022-04-05T01:18:03.000+0000

**Description:**
Users of WiredTiger have a general desire to know how much memory WiredTiger is consuming outside of the cache. It might help us identify where memory usage is coming from if we tag allocations and frees into different categories (for cache, not for cache to start with).

We should carefully consider whether this would be useful. The WiredTiger cache tracking is complex and disassociated from the actual memory allocation, and involves adjustments for assumed allocator overhead. As such it's not likely that we could create a set of information that could be used for double-accounting in terms of comparing what WiredTiger reports with what the allocator reports.

---

## WT-4089: Inconsistency in documentation configuration output

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2018-05-16T15:47:38.000+0000
- **Updated:** 2022-04-05T00:45:02.000+0000

**Description:**
In our documentation source files, src/docs/*.dox, we reference configuration strings with `\c` and those get rendered as a fixed-width typeface font. It appears generally that it will render the next word in that typeface. But there is an anomaly in that if the text until the next whitespace contains parens, then the content of the parens is not in the same fixed-width font. It leads to text that looks like `log=`(archive=false).

There are several files that exhibit the error. There are several others that solve this problem in different ways. We should pick one way and make all of them consistent and correct.

---

## WT-4095: Review log slot switch algorithm to reduce lock contention

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2018-05-21T23:04:35.000+0000
- **Updated:** 2022-04-05T01:21:26.000+0000

**Description:**
The current log slot switching algorithm holds the slot lock while writing the content out to filesystem. That's necessary because a forced flush can come in, and it relies on an acquisition of the slot lock to indicate that any data in a slot has been written.

If we could make a change to the forced flush algorithm so that it no longer relies on the write happening inside the lock, we could remove a point of contention in our logging algorithm that is noticeable when the log is written on a slow filesystem.

This is related to WT-4058 and WT-4077. Those tickets have more complete description of a first attempt at this optimization, and the race condition it introduced.

---

## WT-4109: Extend testing of write-failure scenarios

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2018-06-04T21:26:54.000+0000
- **Updated:** 2022-04-05T01:07:21.000+0000

**Description:**
We currently have fault injection testing that isn't currently running in our CI tool, and a filesystem implementation aimed for testing.

Neither of those are giving us good coverage of system behavior when write operations fail. It would be interesting to see if we can implement a more targeted version of the fault injection tests - that just simulates occasional write failures.

---

## WT-4158: Fix concurrent behaviour of insert with truncate.

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** SERW
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Vamsi Boyapati
- **Created:** 2018-07-02T22:55:39.000+0000
- **Updated:** 2022-10-05T15:48:25.000+0000

**Description:**
At the moment if a range truncate is started over a range of keys, and in parallel another transaction inserts a key into that range, the behavior is not well defined - a conflict may be detected or both transactions may be permitted to commit. If they do commit, then there is a crash and recovery runs, the result may be different than what was in cache before the crash.

---

## WT-4161: Extend test/format to test write failure handling

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2018-07-03T04:28:21.000+0000
- **Updated:** 2022-04-05T01:08:17.000+0000

**Description:**
We don't do enough testing around the behavior of WiredTiger when there are error returns from write system calls. We should figure out how to add test coverage using test/format for such failures. We have another ticket to track doing similar work in more structured test scenarios: WT-4109.

---

## WT-4165: Optimize stability of workload with many tables

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** SEPR, bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2018-07-04T03:48:37.000+0000
- **Updated:** 2022-07-22T00:50:21.000+0000

**Description:**
The following wtperf configuration simulates a workload on 18000 tables, pushing the dirty cache above 20% during checkpoints. A brief slowdown of several seconds is observed that can be explored for possible improvements.

---

## WT-4173: workgen: refactor runner functions

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2018-07-09T18:50:35.000+0000
- **Updated:** 2022-04-05T00:58:29.000+0000

**Description:**
In the workgen `runner` directory, there runners that define their own version of `op_append`, even though that function is defined in `runner/runner/core.py` library. Also, there are multiple versions of `operations`. Probably the function can be replaced by `op_multi_table` also in `core`.

---

## WT-4180: Transaction sync timeout in log flush testing

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2018-07-12T02:33:36.000+0000
- **Updated:** 2022-07-22T00:48:59.000+0000

**Description:**
A recent Jenkins test failure was observed on `wiredtiger-test-unit-ppc` that the transaction sync timeout while performing log flush testing. The run took abnormally long (1 day) to fail. It looks the build was triggered by commit of WT-4174.

Failure signature:
```
ERROR: test_txn14.test_txn14.test_log_flush(bg) (subunit.RemotedTestCase)
test_txn14.test_txn14.test_log_flush(bg)
_StringException: Traceback (most recent call last):
  File ".../test/suite/test_txn14.py", line 100, in test_log_flush
    self.session.transaction_sync('timeout_ms=30000')
WiredTigerError: Connection timed out
```

---

## WT-4204: Add test case to verify complex metadata

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** storage-engines, testing
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2018-07-27T06:23:05.000+0000
- **Updated:** 2022-04-05T00:50:33.000+0000

**Description:**
During a recent discussion about reproducing a fsyncLock/snapshotting backup issue, suggestions were made that it would be helpful to add a test case to test_alter03.py such as test_alter03_complex_metadata. The test should set up a table, set up a complex JSON metadata string, then alter the app_metadata with that JSON and verify it. An example of complex JSON strings can be found in test_jsondump02.py. Perhaps the code in there using bin_unicode and mixed_unicode can be used to test embedded special characters in the app_metadata.

---

## WT-4320: Potentially subsume schema_abort test into random_directio

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2018-09-14T18:47:07.000+0000
- **Updated:** 2022-04-05T01:06:16.000+0000

**Description:**
The new `random_directio` test (developed in WT-4225) could take over `schema_abort`'s role. A general comment: this test seems more comprehensive for the schema validation than `schema_abort` which does not really keep track of what should be there. It only makes sure that the crashed database can recover without error. It never has turned up a problem, nor did it reproduce the original problem we were trying to repro. It does use timestamps too though. Should this test replace that one?

---

## WT-4354: Improve fast path WT_SESSION:alter

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2018-10-05T01:49:37.000+0000
- **Updated:** 2022-04-05T01:02:18.000+0000

**Description:**
When users call `WT_SESSION::alter` but aren't changing any configuration options, we don't make any changes to the schema operations, but we do still acquire the checkpoint and schema locks.

Ideally we wouldn't acquire the locks at all if nothing is being changed.

---

## WT-4363: Identify and improve test coverage gaps

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2018-10-19T15:59:11.000+0000
- **Updated:** 2022-04-05T00:44:57.000+0000

**Description:**
WiredTiger test coverage can be measured by the `wiredtiger-test-coverage` Jenkins job. We should periodically, either manually or automatically, review this coverage and identify/fix areas that have poor coverage.

As a small related issue, the current job includes running programs in the `examples/c` directory, and it really should not. The examples have a different purpose, to demonstrate API calls, and don't necessarily check that the calls actually did what they promise to do. If we lose coverage by not including examples, we should add explicit tests. We should also include `test/csuite` programs in our coverage testing, and use explicit configurations when running `test/format` to maximize our coverage.

---

## WT-4365: Simplify control flow in dhandle close function

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2018-10-19T19:49:28.000+0000
- **Updated:** 2022-04-05T01:00:29.000+0000

**Description:**
The control flow in `__wt_conn_dhandle_close` is complex and subtle - we've made previous attempts to simplify it and introduced bugs (see below). We should revisit and clarify.

In WT-4339 (and possibly WT-4334 and WT-4358) we determined that some changes from WT-4314 caused a variety of test failures. We have reverted part of that change. Specifically a change that was supposed to be a simplification causes the failures.

We should dissect this change more and figure out what is causing the problem.

---

## WT-4388: Add complex table types to abort csuite tests

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2018-10-24T15:39:14.000+0000
- **Updated:** 2022-04-05T00:44:12.000+0000

**Description:**
Inspired by the test program in WT-4376 we should add testing and usage of column groups and indexes to the recovering/abort style programs in `test/csuite` to increase the code coverage and recovery coverage of various log record types. Some to start with would be `random_abort`, `timestamp_abort`, `random_directio`, `schema_abort`.

For example, in `random_abort` or `timestamp_abort`, it might look like this:
1. Today's usage: In a transaction, insert to row store, commit, add to "records-N" verification file.
2. New: In another transaction, insert to table (and indexes), commit, add to "indexes-N" verification file.
3. New: In another transaction, insert to table (and col group), commit, add to "cg-N" verification file.
4. New: Add verification code of new data from new tables.

---

## WT-4391: Tracking file system latency below 10ms

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Rodrigo Nascimento
- **Created:** 2018-10-26T17:36:36.000+0000
- **Updated:** 2023-04-07T16:16:55.000+0000

**Description:**
Thinking about storage class memory technologies hitting the market in the next calendar year. Technologies such as Intel Optane SSD Persistent Memory (3D Xpoint) will give users the ability to get extreme low-latency out of the file system layer.

MongoDB should think about improving its instrumentation by adding new latency buckets to track lower latency at the file system layer.

For file system operations, the first bucket sits between 10-49ms. This first bucket will track latency of a file system on top of HDD; it doesn't do much for SSDs and it won't do anything for persistent memory.

---

## WT-4462: Refactor top level open_cursor code

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** storage-engines
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2018-11-28T21:19:27.000+0000
- **Updated:** 2024-04-24T13:57:37.000+0000

**Description:**
In WT-4442 support for duplicate backup cursors was added. The code changes in `session_api.c:__session_open_cursor` make it too specific with the `statjoin` and `dup_backup` booleans at that top level.

The original code changes came from WT-1315.

---

## WT-4487: Use more accurate statistics for various running totals

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2018-12-10T21:16:23.000+0000
- **Updated:** 2022-04-05T01:02:35.000+0000

**Description:**
In WT-4438, we determined that any statistics that do not use atomic increment may be inaccurate. For many cases we don't care about a fractional percentage drift. But in particular when a statistic holds a counter, and the drift can accumulate in one direction, we may care.

A check of the source tree looking for matches of `STAT.*DECR` should identify most or all cases where a statistic value represents a resource count.

For connection-wide statistics counters, we have: `lsm_work_queue_switch`, `lsm_work_queue_manager`, `lsm_work_queue_app`, `session_open`, `txn_prepare_active`; and for statistics that are per-data source, we have: `cursor_open_count`, `cursor_update_bytes`.

We should fix these to use the `STAT.*ATOMIC` macros, or document that they may accumulate drift.

---

## WT-4597: Add a static test for verifying the correctness of statistic values.

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jongbin Kim
- **Created:** 2019-02-20T03:40:07.000+0000
- **Updated:** 2024-04-23T03:00:19.000+0000

**Description:**
Add a static test for verifying the correctness of statistic values. Keith also suggested to include a similar test for a complex table with multiple column groups and indexes, and confirm we're correctly clearing and aggregating statistics.

---

## WT-4622: Handle txn_state->is_allocating routines in __wt_verbose_dump_txn

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Hyeongwon Jang
- **Created:** 2019-03-06T23:10:32.000+0000
- **Updated:** 2024-04-23T04:33:13.000+0000

**Description:**
From WT-4571, this change newly added `is_allocating` variable to txn_state which maintains the global transaction table. That change requires any thread who tries to read this global txn_state to check until `is_allocating` flag is set to false (i.e., until the transaction ID becomes valid). This re-checking routine is already done in `__wt_txn_get_snapshot` and `__txn_oldest_id_scan` code and this ticket is to add this kind of routine to `__wt_verbose_dump_txn`.

---

## WT-4656: Enhance salvage to use timestamps when determining recency

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2019-03-22T14:36:49.000+0000
- **Updated:** 2024-04-23T04:39:54.000+0000

**Description:**
When timestamps are written into the data files, salvage could use them to determine the most recent versions of key/value pairs, as that's finer-grained information than the page write generations we currently use.

---

## WT-4667: Add automated testing for non-hardware CRC functionality

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2019-03-28T18:21:00.000+0000
- **Updated:** 2024-04-23T04:47:27.000+0000

**Description:**
Reviewing the compiler tests, there are a few configurations not regularly tested as part of the Jenkins `wiredtiger-pull-request-compilers` job. These include: `--disable-crc32-hardware`, `--enable-leveldb`, `--enable-python`, `--enable-tcmalloc`, `--java`, `--with-spinlock=gcc`, `--with-spinlock=pthread`, `--with-spinlock=pthread_adaptive`.

The compiler build currently takes 11 minutes, so it's not the slow part of a PR build. But there's no point in testing these on every build; once a week on the `develop` branch would be more than sufficient.

---

## WT-4713: Python documentation not exposed at top level

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2019-04-09T18:42:12.000+0000
- **Updated:** 2022-04-05T01:24:26.000+0000

**Description:**
Our doc tools create some output html that describes the Python API, for example `classwiredtiger_1_1_cursor.html`. However, it doesn't appear that this can be found from the top level `index.html`.

If we want to have Python documentation, we should review these pages and make some sort of top level link.

Also, there is a `pyfilter` script (that uses a subordinate `fixlinks.py`) that is referenced by Doxygen. These are both currently unused. If we're going to support Python doc, this should be investigated. Otherwise we can remove these files.

---

## WT-4802: Enable and improve random dhandle selection and eviction target calculations

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2019-05-20T01:11:32.000+0000
- **Updated:** 2022-04-05T00:49:16.000+0000

**Description:**
An external user reports that a change to evict page target calculation in `__evict_walk_tree()` generates better eviction results. This ticket is to investigate the reported change. As part of this ticket, we will also run the reported tests and investigate improvements if any we could come up with.

---

## WT-4813: Enable cursor caching for statistics cursors

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2019-05-23T07:35:50.000+0000
- **Updated:** 2022-04-05T01:20:43.000+0000

**Description:**
SERVER-41048 tracks regression in transaction performance with storage statistics collection. The regression is mostly because of re-creating and closing the statistics cursor on the session with each operation.

This ticket is to enable statistics cursor caching. This should mitigate the regression seen in SERVER-41048.

---

## WT-4880: Make Python tests work with statically linked extensions

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Trivial - P5
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alex Cameron
- **Created:** 2019-06-21T11:28:14.000+0000
- **Updated:** 2024-04-24T03:03:29.000+0000

**Description:**
If you supply `--with-builtin=snappy,zlib,zstd,etc` to the configure script, our Python tests will not pick up the compressors and any test scenarios that use them will be skipped.

The only way to get tests to use extensions is to supply `--enable-snappy` and other similar flags to build each of them as shared libraries.

Having compressors statically linked as a builtin should not prevent them from being used in Python tests.

---

## WT-4903: extend test/checkpoint online snapshot verification using prev

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2019-07-02T19:42:40.000+0000
- **Updated:** 2022-04-05T01:14:36.000+0000

**Description:**
A couple months ago, in WT-4703 we added online consistency checking to `test/checkpoint` using a snapshot transaction. The test should be extended to include an additional consistency check that walks with an online snapshot transaction using `cursor->prev`. The prev code is very different and exercises very different code paths than the next code.

---

## WT-4914: Log cursor value_format change from qIIIuu to QIIIuu

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-e, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2019-07-05T02:08:46.000+0000
- **Updated:** 2022-07-22T00:50:33.000+0000

**Description:**
The log cursor is created with key and value formats. The first value of the value_format is txnid, which is of type uint64_t, but the cursor is declared with int64_t.

Example code in "ex_log.c" and "ex_encrypt.c" that tests log cursors is written with uint64_t txnid as the return value.

Possible solutions:
* Correct the log cursor format from q to Q for txnid.
* Update the example code that uses log tests to refer txnid as int64_t.

---

## WT-4938: Error while running to install the wiredtiger Python module on Windows

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** Python,, WiredTiger, python
- **Components:** Language Bindings
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** mag istr
- **Created:** 2019-07-17T04:36:20.000+0000
- **Updated:** 2022-04-05T00:51:41.000+0000

**Description:**
Environment: MS Windows 10 Python 3.7.1
Encountered an error while running to install the wiredtiger Python module via pip. The setup.py reports "Python3 is not yet supported" for version 3.1.0, and for version 3.2.0 a FileNotFoundError occurs because setup.py is missing from the extracted archive.

---

## WT-4941: Add accessor functions for WT_CONFIG_ITEM fields.

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2019-07-18T02:09:46.000+0000
- **Updated:** 2022-04-05T01:09:32.000+0000

**Description:**
It's relatively easy to incorrectly use the `WT_CONFIG_ITEM` fields. For example, code from WT-4939 retrieves a value for a string configuration but tests an integer configuration field and so always returns incorrectly. It might be reasonable to add `WT_CONFIG_ITEM` accessor functions that use the `WT_CONFIG_ITEM.type` field to avoid making the wrong test.

---

## WT-4945: Expand io_capacity configuration setting to allow number of IOs

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2019-07-18T18:59:32.000+0000
- **Updated:** 2022-06-08T01:08:43.000+0000

**Description:**
An additional setting for the `io_capacity` configuration that would specify number of IOs per second instead of byte capacity was requested. Use this ticket to discuss this.

---

## WT-4948: WiredTiger.backup file should be a normal WT table

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2019-07-19T19:33:08.000+0000
- **Updated:** 2022-07-22T00:49:14.000+0000

**Description:**
The `WiredTiger.backup` file is currently a text file. It should be fixed to be a normal WiredTiger table so that it goes through the block manager and is subject to compression and encryption paths.

---

## WT-4951: Create standalone disk validation utility

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2019-07-22T01:30:47.000+0000
- **Updated:** 2024-04-23T20:40:51.000+0000

**Description:**
Sometimes customers report that they have experienced data corruption, it would be useful when that happens to have a tool they can run which verifies that the disk isn't unusually susceptible to data corruption.

The tool wouldn't need to run everywhere. It could possibly be based on the fio tool, using its verify option. We also have some internal testing setup that uses dd - which might be an alternative implementation.

---

## WT-4962: add gdb functions that mimic the debug functions

- **Status:** Backlog
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** neweng
- **Components:** GDB Scripts
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2019-07-25T14:34:15.000+0000
- **Updated:** 2024-01-04T23:25:53.000+0000

**Description:**
When debugging a core file one cannot call any of the debug functions, typically in `btree/bt_debug.c`, because it is not a running process. It would be helpful if we had gdb equivalents to look at the cache, update structures and the tree when debugging a core.

These should probably reside in the `wiredtiger/dotfiles` tree somewhere.

---

## WT-5035: Decommission Jenkins CI system

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jocelyn del Prado
- **Created:** 2019-07-30T04:50:18.000+0000
- **Updated:** 2022-04-05T01:18:50.000+0000

**Description:**
No description

---

## WT-5049: Removal of turtle file should be salvageable

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2019-08-06T17:22:22.000+0000
- **Updated:** 2025-03-18T02:25:18.000+0000

**Description:**
As a followup to WT-4344, we noticed that (case 1) after removing the `WiredTiger.turtle` file, and immediately calling `wiredtiger_open` with the salvage option, we get a failure return. However, if instead (case 2) after removing `WiredTiger.turtle`, we immediately call `wiredtiger_open` without the salvage option, we get a successful open. We can then call `wiredtiger_open` with salvage with no error. This seems strange, it seems like we should be able to salvage immediately in case 1.

---

## WT-5053: Enhance salvage database to be able to use source objects

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2019-08-08T18:30:46.000+0000
- **Updated:** 2023-05-03T01:45:58.000+0000

**Description:**
Now that files include their metadata, it would be possible to restore missing objects based on their source objects.

So, we could re-create the `WiredTiger.turtle` file by scanning the `WiredTiger.wt` file for the relevant metadata and final checkpoint.

Further, we could re-create the `WiredTiger.wt` file by scanning the source files in the database. This step would require additional information: we wouldn't know how those files are related, for example, the relationships between indexes and collections would be unknown after that recovery.

---

## WT-5070: Test that using WT_CURSOR::modify works with all visibility scenarios

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** durable-history
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2019-08-21T00:14:33.000+0000
- **Updated:** 2022-04-05T01:20:20.000+0000

**Description:**
The `WT_CURSOR::modify` API is a bit unusual, in that an update chain will build upon previous entries to create a full version of a value. We should add testing to ensure that `WT_CURSOR::modify` works as expected when used with unusual timestamp rules. Including if interleaving timestamp and non-timestamped updates, and adding timestamped updates out of order.

This is follow on from WT-4776.

---

## WT-5091: Enhance the random_abort to fine control the test execution

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2019-08-29T23:28:57.000+0000
- **Updated:** 2022-04-05T00:58:14.000+0000

**Description:**
With WT-4884, `random_abort` test has been enhanced with the support of `column_store` to test all the cursor possible operation WAL log types. But currently, there is no way to control for running partial tests. As part of this, adding additional options would be good:
* An option to run row-store only tables.
* An option to run column-store only tables.
* An option to run the insert-only workload.
* Updating `smoke.sh` to include running with these options.
* Refactoring of verification logic code.

---

## WT-5103: Investigate improvements to eviction slot calculations

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2019-09-04T01:15:43.000+0000
- **Updated:** 2022-04-05T00:50:19.000+0000

**Description:**
An external user has raised a question about the eviction slot calculation logic in WiredTiger, specifically about `WT_EVICT_WALK_BASE` (300) and `WT_EVICT_WALK_INCR` (100) and whether `bytes_per_slot` should be computed against `WT_EVICT_WALK_INCR` rather than `cache->evict_slots` (which is 400). The user provided profiling results showing their proposed change produces more stable eviction. This ticket is to investigate and address improvements if warranted.

---

## WT-5107: Update WiredTiger Python formatting/linting standard to match MongoDB

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2018-04-16T04:26:37.000+0000
- **Updated:** 2022-04-05T01:12:17.000+0000

**Description:**
A recent Python formatting/linting update was made to the MongoDB repo for build and test related scripts. WiredTiger team decided to apply the same standard to the WiredTiger repo. It would be great to incorporate the format checking into a script under dist/ directory so that it's covered by existing checking commands without overhead to remember running a separate new script before committing code.

---

## WT-5110: Add dsrc statistic for size of checkpoints unable to be deleted

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2019-09-05T19:33:13.000+0000
- **Updated:** 2023-05-03T02:08:28.000+0000

**Description:**
With Atlas using backup cursors and potentially holding them open, it would be useful if statistics could give the user an indication about how much space is being retained in a table due to checkpoints unable to be deleted because a backup cursor is open.

---

## WT-5127: Fix a bug where code uses leaf page size, not memory page max

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2019-09-17T18:44:13.000+0000
- **Updated:** 2022-07-22T00:50:30.000+0000

**Description:**
The code in `__wt_leaf_page_can_split` uses `maxleafpage` to make a decision about whether a page is big. It likely intends to use `maxmempage` instead.

The maximum leaf page is likely to be 32k, whereas the maximum in-memory page is likely to be 10MB.

The code was introduced in WT-2954. We should do some more spelunking through history before making this change, to ensure that it's a bug and not expected. If we determine that it's expected the comment should be updated to be clearer.

---

## WT-5133: Replace wt_epoch with wt_clock where we can

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2019-09-24T18:30:58.000+0000
- **Updated:** 2022-04-05T01:18:59.000+0000

**Description:**
While investigating WT-5042, profiling revealed that `wt_epoch` was consuming 102 seconds out of 360 cumulative seconds. A quick replacement with `wt_clock` and `WT_CLOCKDIFF` calls reduced this to 1.3 seconds - a two-order-of-magnitude improvement.

All uses of `wt_epoch` in the codebase should be examined and any that use it in a start/stop/WT_TIMEDIFF manner should replace it with `wt_clock` and `WT_CLOCKDIFF` calls.

---

## WT-5147: fast-path search isn't implemented for the read-committed isolation level

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2019-10-02T00:29:54.000+0000
- **Updated:** 2022-07-22T00:48:58.000+0000

**Description:**
In WT-5134, we turned off fast-path searching (the search variant where we check the cursor's currently pinned page before searching from the root of the tree) for the read-committed isolation level.

This shouldn't be necessary: there are underlying transactional functions that are handling transaction IDs differently, which shouldn't be the case.

---

## WT-5180: Exclude .git from the evergreen artifact tar ball

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** neweng, quick-win
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2019-10-14T23:48:43.000+0000
- **Updated:** 2025-04-09T07:14:50.000+0000

**Description:**
The .git folder is occasionally causing permission issues when extracting the tar ball and unexpectedly failing subsequent tasks. The detailed agent log shows repeated failures with "permission denied" errors when trying to extract `.git/objects/` files, leading to 10 failed S3 get attempts before task failure.

---

## WT-5332: Investigate the impact of slow checkpoints using the new debug mode

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2019-12-19T04:45:57.000+0000
- **Updated:** 2022-04-05T00:56:23.000+0000

**Description:**
Following on from WT-4921 where a debug mode was added that slows checkpoint creation we should try and investigate what impact that has on wiredtiger and if there is any major fallout. See WT-4921 for details. This could be done as a wtperf workload.

---

## WT-5390: Document wiredtiger structs memory padding / management in the developer docs.

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2020-01-06T00:53:21.000+0000
- **Updated:** 2022-04-05T01:25:14.000+0000

**Description:**
Creating this ticket as a follow on from discussion on a recent Pull request: https://github.com/wiredtiger/wiredtiger/pull/5066#discussion_r361033384

---

## WT-5396: Review how WiredTiger uses WT_PUBLISH and WT_ORDERED_WRITE

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-01-07T05:25:04.000+0000
- **Updated:** 2022-04-05T01:21:17.000+0000

**Description:**
There has been some contention about when and how to control cases where it's important the order in which updates to different member variables are visible to other threads.

A recent example of this was in WT-5119, where we added the `WT_ORDERED_WRITE` macro, which has the same semantic as `WT_PUBLISH`.

Let's use this ticket to decide how to manage ordering in such cases, and be consistent moving forward. There is disagreement about whether an sfence implies that reads will also be ordered in respect to the sfence operation.

---

## WT-5399: Python: Fix Session.strerror()

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2020-01-08T21:24:15.000+0000
- **Updated:** 2022-07-22T00:50:00.000+0000

**Description:**
We expect the following code inserted into the test suite to work:
```python
def test_strerror(self):
    err = wiredtiger.WT_NOTFOUND
    notfound_str = 'WT_NOTFOUND: item not found'
    self.assertEqual(wiredtiger.wiredtiger_strerror(err), notfound_str)
    self.assertEqual(self.session.strerror(err), notfound_str)
```
But it doesn't, the second assertEqual fails with `WiredTigerError: Unknown error 656412725`. It probably has to do with the return type not being an integer. There's a SWIG rule that checks return values and if not zero and not WT_NOTFOUND, a exception is generated.

---

## WT-5430: Write out debug log records for operations that do not commit

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Louis Williams
- **Created:** 2020-01-13T19:52:01.000+0000
- **Updated:** 2022-04-05T01:09:03.000+0000

**Description:**
Currently, debug logging only writes out log records for operations that commit. To aid in debugging, it would be helpful to have information about readers, too.

---

## WT-5472: Add statistic that tracks when salvage builds big internal pages

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-01-23T03:41:48.000+0000
- **Updated:** 2022-04-05T00:59:38.000+0000

**Description:**
In WT-5437, we added code that ignores the page size when building internal pages after salvage to avoid getting cache stuck full failures during salvage. It would be useful to add a statistic that is incremented when the page is kept artificially small - see `__slvg_row_build_internal`.

This should only be relevant until we've addressed the issue in a different way - which is captured in WT-5447.

---

## WT-5494: Request for example usages of wt utility in documentation

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Maria van Keulen
- **Created:** 2020-01-29T15:36:17.000+0000
- **Updated:** 2022-04-05T01:23:36.000+0000

**Description:**
After learning how to use the `wt` utility, a request was made for example usages of `wt` calls to be included in the documentation. For example, when learning how to use `wt dump`, the format of the URI needing to be `table:xxx` or `colgroup:xxx` was not immediately obvious. Examples of the syntax for each command call would help speed up the learning process.

---

## WT-5498: Investigate ftdc stalls when trying to delete checkpoint during backup cursor execution.

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Ravi Giri
- **Created:** 2020-01-30T05:01:48.000+0000
- **Updated:** 2022-04-05T01:19:22.000+0000

**Description:**
No description

---

## WT-5511: Document the usage of split generation code for concurrent access of page index

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2020-02-03T03:01:23.000+0000
- **Updated:** 2022-04-05T01:17:13.000+0000

**Description:**
After having to deal with the page index code, it was suggested to create a ticket for documenting the code. The macros in question are `WT_ENTER_PAGE_INDEX`, `WT_LEAVE_PAGE_INDEX`, and `WT_WITH_PAGE_INDEX`. It will be useful to document how we use the generation number to access page index, while a concurrent thread might be splitting a page and writing a new index.

---

## WT-5514: Summarise the search changes and outline how the search works in durable history

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** durable-history
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-02-03T05:09:11.000+0000
- **Updated:** 2022-04-05T01:27:07.000+0000

**Description:**
We are now very close to the MVP of durable history. In durable history, we have made big changes in terms of searching. It is worth documenting that for knowledge sharing.

---

## WT-5528: Create an on-boarding document of Should-Read WT Wiki Pages

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** neweng, wiki-documentation
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew McMenemy
- **Created:** 2020-02-04T23:40:07.000+0000
- **Updated:** 2022-04-05T01:03:30.000+0000

**Description:**
We have a lot of great Wiki articles that need to be more visible. Create a new document that can act as a Goto for new members of the team. Things like Devbox setup, creating your first WiredTiger ticket, triaging your first Build Fail ticket, etc., how Evergreen works, etc.

---

## WT-5561: Add __wt_fsync histogram statistics

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Brian Lane
- **Created:** 2020-02-11T02:57:48.000+0000
- **Updated:** 2022-04-05T01:11:18.000+0000

**Description:**
This issue is to collect and expose a histogram for `__wt_fsync`.

---

## WT-5586: Update WT package on PyPi to include compressor libs

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Brian Lane
- **Created:** 2020-02-13T04:09:55.000+0000
- **Updated:** 2022-04-05T00:58:53.000+0000

**Description:**
Making the python install from PyPi easier is desirable. Currently, the compressor libs need to be pre-installed. We could look into making a WT wheel that bundles the libs to make the installation easier for users.

---

## WT-5592: WiredTiger assumes URI arguments contain printable characters

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2020-02-13T19:59:55.000+0000
- **Updated:** 2022-07-22T00:49:42.000+0000

**Description:**
WiredTiger assumes URI arguments contain printable characters, generally it uses '%s' to display them.

We might want to disallow non-printable characters in URI strings at the API level in the same way we reserve the 'WiredTiger' prefix.

---

## WT-5599: Explore discarding obsolete updates when checkpointing

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2020-02-14T00:50:18.000+0000
- **Updated:** 2022-11-14T00:42:23.000+0000

**Description:**
When walking the update chain during a checkpoint if there are updates older than the oldest, potentially they can be discarded by calling `__wt_update_obsolete_check()`. This will help free some cache of the obsolete content as part of the checkpoint. We already do this check as part of eviction.

We will need to study if it is safe to call `__wt_update_obsolete_check()` as part of the checkpoint, because we do not have exclusive access to the page.

---

## WT-5646: Python interface for cursors should raise an exception on cursor error.

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2020-02-21T13:57:29.000+0000
- **Updated:** 2022-07-22T00:49:18.000+0000

**Description:**
Python implements a shorthand for cursors that allow them to be used in `for` loops. If `Cursor.next` returns something other than 0 or `WT_NOTFOUND`, that error is lost. See the `__next__` function in `class IterableCursor` in the SWIG interface spec: `lang/python/wiredtiger.i`.

---

## WT-5709: In the Python test suite, explore adding a timeout on all test functions

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2020-02-28T16:38:45.000+0000
- **Updated:** 2022-04-05T01:11:32.000+0000

**Description:**
It would be nice to have a timeout on test functions to detect any sort of hang. Not sure it can be easily adapted to our situation - we use `concurrencytest.ConcurrentTestSuite`, and we'd like to specify the timeout globally without decorating individual functions. Though it would be good to override timeouts on an individual basis.

---

## WT-5793: Remove WT_REC_VISIBLE_ALL flag

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-03-09T09:44:58.000+0000
- **Updated:** 2022-11-08T14:56:14.000+0000

**Description:**
In durable history, we should write the first committed value to disk no matter if it is globally visible or not. Thus, we can remove this flag and all its usage.

---

## WT-5802: Reduce runtime of Python history store tests

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-03-10T04:13:20.000+0000
- **Updated:** 2022-04-05T00:57:33.000+0000

**Description:**
The history store (test_hsXX.py) Python tests currently take a long time to run - especially on Windows. Review the tests and reduce the amount of work they do where possible, and figure out whether it's worth splitting the history store tests into more buckets in test/evergreen.yml to reduce the automated testing run span.

---

## WT-5818: Add ability for a cursor to not participate in cursor copy debug functionality

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2020-03-11T23:27:15.000+0000
- **Updated:** 2022-07-22T00:50:10.000+0000

**Description:**
WT-5574 saw an issue with cursor copy debugging functionality, with a suggested enhancement of adding the ability for cursors to not participate in the feature.

The suggestion: have a non-public flag on the cursor indicating that this cursor is not participating in cursor copy. Looking to the farther future, it's possible that after `debug_mode=cursor_copy` gets more use, we may in fact want to expose this as a feature on `WT_SESSION->open_cursor` configuration. For WT-5574 a minimal fix was made. This ticket is filed to address a more proper fix.

---

## WT-5832: Detect potential corruption as part of recovery/rollback to stable

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alex Cameron
- **Created:** 2020-03-12T05:35:26.000+0000
- **Updated:** 2025-10-20T17:03:44.569+0000

**Description:**
As part of WT-5786, we began omitting data files that aren't large enough to have a descriptor block (clearly corrupted) in rollback to stable. We need to do this for `repair_unfinished_indexes.js` so that MongoD is able to startup successfully in repair mode. Since we iterate over all data files in recovery and potentially run rollback to stable on them, we have more opportunities to detect corruption so we should do something to signal loudly to MongoDB that there is something wrong without someone explicitly knowing that they should run repair.

---

## WT-5924: Integrate alphabetic Clang Tidy check into PR testing

- **Status:** Backlog
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alex Cameron
- **Created:** 2020-03-25T09:52:47.000+0000
- **Updated:** 2022-04-05T01:05:21.000+0000

**Description:**
Our most recent pair of interns investigated the use of Clang Tidy to enforce WiredTiger's code style around the alphabetic ordering of variable declarations. This ticket is to track the work required to integrate this as part of our PR testing.

What needs to be done:
1. Migrate changes over to a separate fork of LLVM under the WiredTiger Github org.
2. Cleanup the code according to PR comments.
3. Build statically linked versions of Clang Tidy for Linux and MacOS and upload them to an S3 bucket.
4. Integrate them into `dist/` scripts, using `s_clang_format` as a reference.
5. Add these new scripts to the Evergreen PR testing.

---

## WT-5942: Improve how we track which updates are restored during reconciliation

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-03-30T02:18:07.000+0000
- **Updated:** 2022-11-14T03:54:21.000+0000

**Description:**
In WT-5527, we made the change to track the updates that need to be restored at each update chain, each splitted page, and each page level, which is quite complex.

Investigate whether we can simplify and improve the implementation.

---

## WT-5947: Investigate if we need to free the updates in scrub eviction

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-03-30T23:58:01.000+0000
- **Updated:** 2022-11-14T03:54:26.000+0000

**Description:**
The goal of `WT_REC_SCRUB` is to keep content in cache, but minimize how much work needs to be done to flush back dirty data moving forward.

In WT-5527, we choose to free the updates that have been moved to the data store and history store in scrub eviction. This may not result in any correctness issues but may lead to performance degradations. We need to investigate its behaviour and decide what to do.

---

## WT-5996: Review WT_SESSION_NO_LOGGING and if other flags should be retained when calling RTS

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2020-04-09T05:12:57.000+0000
- **Updated:** 2022-04-05T01:00:23.000+0000

**Description:**
Server restart hangs during recovery as part of rollback to stable checkpoint. The reason for the wait forever is that checkpoint is waiting for a logged record to be written to disk, but there is no log server during the recovery phase leading to waiting forever.

The issue was fixed by retaining the `WT_SESSION_NO_LOGGING` flag of the calling session when opening the rollback to stable internal session during recovery.

Before adding rollback to stable code as part of DH changes, there existed a checkpoint that used to perform at the end of recovery without problems. As part of this ticket, we need to find out the difference between recovery checkpoint and rollback to stable checkpoint that was leading to the wait.

---

## WT-6005: Create a python test that validates the new version check performed on start

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2020-04-14T02:06:27.000+0000
- **Updated:** 2023-05-03T01:54:55.000+0000

**Description:**
In WT-5630 we added functionality to wiredtiger that prevents it starting on too low of a version. It would be worth creating a python test that validates all three scenarios and errors returned within the new function `__turtle_validate_version` in meta_turtle.c.

The three scenarios are:
1. A corrupt turtle file, or turtle file missing the version string. (done as part of WT-6004)
2. A version string that isn't parseable. (done as part of WT-6004)
3. A version string that isn't compatible with the current WiredTiger version.

The python test will need to open a database, close it. Replace the turtle file with the corrupted/modified version. Then re-open the database and expect the error messages defined in `__turtle_validate_version`.

---

## WT-6012: Enhance test/format to support -R and -C option at the same time

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2020-04-15T20:11:47.000+0000
- **Updated:** 2022-04-05T01:30:44.000+0000

**Description:**
The newly added -R option to the format test program doesn't support setting options to `wiredtiger_open`, that is, the "-C" command line option and the "wiredtiger.config" CONFIG file option, are silently ignored.

This makes it difficult to do -R runs with any additional configuration, for example, adding "-C 'verbose=(log)'" on the command line won't work.

---

## WT-6024: Make testing binaries relocatable across machines and folders

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2020-04-20T01:31:52.000+0000
- **Updated:** 2022-04-05T01:16:17.000+0000

**Description:**
The current WiredTiger source code build and Autotools configure generate testing binaries that can be executed with the in-place directory structure on the same machine where those binaries are built. However, if the same source (including build) directory is copied over onto a different machine, or to a different directory on the same machine, calling those testing binaries would run into problems failing to locate the required dynamic libraries, as the RPATH is set to a hardcoded directory by the initial build.

In order to make testing binaries relocatable across machines and folders, one possibility is to enable a portable RPATH setting using $ORIGIN (instead of hardcoded value), ideally during the building phase.

---

## WT-6028: Signal in the API when a wiredtiger_open call fails due to compatibility_version

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alex Cameron
- **Created:** 2020-04-21T01:06:45.000+0000
- **Updated:** 2022-04-05T00:51:36.000+0000

**Description:**
Restarting an older version of MongoDB on a newer database without following the expected downgrade procedure results in repeated `wiredtiger_open` calls that spam error logs.

We should signal somehow in the API whether an error in `wiredtiger_open` has occurred due to the supplied compatibility version or for some other reason. If it failed for a non-compatibility version related reason, then MongoDB should not follow up with additional calls to `wiredtiger_open`.

Some ideas include:
* Keeping the "Version incompatibility detected:" prefix but explicitly documenting it as part of our API.
* Return a different error code for this category of error (preferable if possible).

---

## WT-6037: Performance degradation because of open/close history store cursor to cache dhandle

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alex Cameron
- **Created:** 2020-04-22T03:39:09.000+0000
- **Updated:** 2025-07-15T23:08:21.252+0000

**Description:**
As part of WT-5918, a performance regression was introduced in WT-5785. We should understand why this change causes a performance drop and recover it if possible.

---

## WT-6076: Extend format to run with 'S' modifies occasionally.

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2020-04-28T03:21:29.000+0000
- **Updated:** 2023-05-03T02:06:01.000+0000

**Description:**
Follow on work from WT-6051. To extend test coverage and get format to run with different formats of modifies from now on, at least 'S' modifies so we catch bugs like the one in WT-6051. Currently it only uses 'u'. This could require some string manipulation in format which may not be ideal.

---
