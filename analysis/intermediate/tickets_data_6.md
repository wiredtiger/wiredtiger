# WiredTiger Tickets Data - Group 6 (WT-10865 to WT-12294)

## WT-10865: Enhance s_string to check for spelling errors in python comments

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2023-03-31T16:31:08.000+0000
- **Updated:** 2023-04-01T21:08:11.000+0000

**Description:**
Code reviews sometimes catch spelling errors and typos in the comments of our Python test suite. We shouldn't need to rely on human review for this. 

Enhance the `s_string` to spell check our Python comments. `aspell` has a `–mode=comment` option that is supposed to only check lines that start with `#`.

Implementing this ticket will likely require the tedious work of fixing a any spelling errors uncovered by the additional spell checking.

---

## WT-10886: Update testy logging and link logs to dashboard

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** testy
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Ruby Chen
- **Created:** 2023-04-05T05:59:26.000+0000
- **Updated:** 2023-04-05T06:01:51.000+0000

**Description:**
There are currently a handful of logs in the testy framework, with this ticket we want to have logs all throughout the testy framework to make debugging any issues or crashes easier, and to have more informative updates. It would be good to have different levels of logging that can be filtered by CloudWatch logs. 

This ticket will add logging in all the sensible places of the testy framework, then add these logs to testy dashboard through widgets and log querying.

---

## WT-10891: Running wt utility outside of the test/format directory fails unintuitively

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Marc Butler
- **Created:** 2023-04-06T06:18:51.000+0000
- **Updated:** 2023-04-09T02:04:16.000+0000

**Description:**
If the wt utility is run in the root of the build directory (for example) it fails with a dlopen error about a missing shared object file for the reverse collator. This is due to a relative path provided to `dlopen` that is valid only if the current working directory is `test/format`.

Currently in the code the constant EXTPATH specifies this path, it is defined in each tool:
- test/cppsuite/src/common/constants.h:96
- test/format/format.h:36
- bench/wtperf/wtperf.h:44

---

## WT-10896: Test dist/s_docs with doxygen 1.9.3 (or drop support?)

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2023-04-07T15:51:41.000+0000
- **Updated:** 2023-04-09T02:05:50.000+0000

**Description:**
Given that the s_docs script works with specific versions 1.8.17 or 1.9.3, we should have an evergreen test that runs on a system with 1.9.3 installed. Otherwise errors like WT-10895 are likely to happen. An alternative is to require only 1.8.17.

---

## WT-10926: Review all the disabled code without a FIXME

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-04-13T03:04:13.000+0000
- **Updated:** 2023-04-13T20:51:59.000+0000

**Description:**
There are a few `#if 0` in the code without a comment or a FIXME tag. It would be great to review them and create a ticket when appropriate.

---

## WT-10936: Make test/checkpoint predictable for column store

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** Column Store, Test Checkpoint
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2023-04-14T01:48:45.000+0000
- **Updated:** 2026-01-05T02:07:10.209+0000

**Description:**
This is follow on work from WT-9914. Consider adding column store support:
* Column store support - I have added these tests in evergreen but commented them out for now. We need to debug why the column store doesn't work and enable the tests. Likely the column store has an issue with this test as the test works on random keys in a given key range, and the column store fills in the missing key range.

---

## WT-10955: Make many-collection-test easier to debug

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-04-19T01:37:02.000+0000
- **Updated:** 2023-04-19T06:36:38.000+0000

**Description:**
The `many-collection-test` takes a long time to setup as it needs to build MongoDB, generate a database with many rows, etc.

It would be great to be able to provide (it might be already possible):
* A MongoDB binary so we don't have to compile it again
* A testing mode so we don't need to generate a big database to execute the test

---

## WT-10956: Investigate performance change in test/format after mirror branch (zseries)

- **Status:** Backlog
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** perf-regression
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mick Graham
- **Created:** 2023-04-19T06:30:15.000+0000
- **Updated:** 2023-09-04T03:16:01.000+0000

**Description:**
While investigating WT-10435, a performance regression was noticed in test/format with mirroring not enabled. Comparisons between pre-mirror and post-mirror commits showed: Pre mirror (x86): ~262 seconds, Post mirror (x86): ~197 seconds (faster), Pre mirror (zseries): ~130 seconds, Post mirror (zseries): ~168 seconds (slower). The zseries regression was significant enough to affect whether WT-10435 was hit or not. More tests are needed on other platforms as well.

---

## WT-10982: s-all should not run on multiple variants

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod, evergreen, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-04-24T00:59:19.000+0000
- **Updated:** 2025-05-01T03:02:30.000+0000

**Description:**
The current task `s-all` is tagged with the `pull_request` and executed on multiple variants, Ubuntu and Ubuntu ASAN. Having the task executed on Ubuntu should be good enough.

We could declare a dedicated tag for s-all and just add it to the desired variant.

---

## WT-10991: Add "general" handler callbacks to Python SWIG interface

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, tiered-storage
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2023-04-25T20:51:19.000+0000
- **Updated:** 2025-12-03T06:07:46.097+0000

**Description:**
Pretty soon we'll probably want to be using the `handle_general` callback to indicate things like "flush_tier complete", or more details about flush_tier or other system state changes. We already have events we can't track in python, like `WT_EVENT_COMPACT_CHECK`. (Also, the `handle_progress` callback is not being set, so there's no way to test it in Python.)

We could imagine a shiny new capability that allows us to have arbitrary python functions attached to the callbacks. For the `handle_general` case, we'd probably break things down by the operation type. For tiered storage callbacks, it probably just makes sense to record the results in a file with entries like:
`Apr 24 2023 01:42:23 PM GMT-0400: WT_EVENT_FLUSH_TIER_COMPLETE: s3:/bucket12345/prefix_WiredTiger-0000000001.backup`

---

## WT-10993: Don't use internal WiredTiger structures in the cache_resize.cpp test

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** cppsuite
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-04-26T04:28:05.000+0000
- **Updated:** 2023-04-27T12:52:25.000+0000

**Description:**
The test cache_resize.cpp contains code that accesses `((WT_SESSION_IMPL *)session)->txn->id` directly. The transaction id is then supposed to be used as part of the validation stage. However, this is not working and the fix for this test depends on WT-9339. On top of this, it is generally bad practice to rely on internal structures at the application level and it prevents WT-10965 from progressing.

For these reasons, the logic of the test should be redesigned without the usage of internal structures.

---

## WT-11004: Prevent tiered objects from being overwritten in s3, gcp, azure

- **Status:** Open
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** code-quality, tiered-storage
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2023-04-28T15:27:37.000+0000
- **Updated:** 2026-01-02T04:43:54.034+0000

**Description:**
As a followup on WT-10988, we should make sure that all of our cloud drivers abide by the write once read many protocol. We should not overwrite any object that is already in the cloud. See FIXMEs in the python test suite for this ticket.

---

## WT-11013: Clean up obsolete config items in api_data.py

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2023-05-01T23:49:47.000+0000
- **Updated:** 2023-05-02T20:16:24.000+0000

**Description:**
api_data currently contains some obsolete config items such as `assert.write_timestamp`, that should be cleaned up. The scope of this ticket is to review the contents of `api_data.py` and remove any dead/unused config items. To limit scope, only reviewing whether config items are parsed via functions like `__wt_config_gets` is recommended, and not whether the resulting flags are used in the code base.

Note that obsolete config items which are present in the metadata should *not* be removed; please see WT-10210 for more context.

---

## WT-11014: Rename the test files and src folder rollback_to_stable to rts

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality, rts
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-05-02T02:01:17.000+0000
- **Updated:** 2023-05-02T20:16:40.000+0000

**Description:**
Just to save some typing.

---

## WT-11032: Adjust test/format operation percentages after turning on predictable replay

- **Status:** Open
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** Test Format
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2023-05-04T13:19:57.000+0000
- **Updated:** 2025-03-19T23:34:16.000+0000

**Description:**
Currently, when test/format is configuring its runs, if it sees that predictable replay is on, it sets percentages for certain operations (modify, truncate) to 0. A side effect of this is that when taking a CONFIG from a non-predictable run and making it predictable, the percentages no longer add up to 100. This causes test format to disallow the run.

Possible fixes: recognize the situation and forcibly adjust the percentages, or when one of the disallowed operations comes up, just ignore it or replace it with a read.

---

## WT-11033: Allow test/format to do modify operations with predictable replay

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2023-05-04T13:42:55.000+0000
- **Updated:** 2023-05-04T20:33:04.000+0000

**Description:**
One casualty of predictable replay in test/format is that modify operations are not allowed. This can be fixed by creating a global array of pre-generated modify vectors (seeded with the global "data" random generator) and using the thread's data RNG to pick which to use. This approach doesn't make predictable a special case and maintains high performance since we don't generate a vector for every modify op.

---

## WT-11037: Evaluate enabling per file stats for history store

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** feature, supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2023-05-05T02:52:53.000+0000
- **Updated:** 2023-06-16T18:50:47.000+0000

**Description:**
We have Oplog stats enabled for Server that get collected in FTDC, and they are useful in analysing perf issues. Consider doing so for History Store as well. There might be performance considerations, or these stats might not be useful as we have several system level stats around history store. So need to weigh benefits against the costs.

---

## WT-11038: Expose WiredTiger #defines in python tests

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2023-05-07T22:34:41.000+0000
- **Updated:** 2023-05-22T01:39:44.000+0000

**Description:**
Our python test test_hs09.py contains hardcoded integers for update types that are no longer accurate (WT_UPDATE_BIRTHMARK no longer exists and WT_UPDATE_TOMBSTONE/WT_UPDATE_STANDARD have changed to 4 and 3 respectively). We need to update this test to use the proper values, preferably by making use of the WT_UPDATE_* definitions themselves rather than raw integers. A review of our python tests should also be done to detect and fix any other cases where we're using hardcoded numbers.

---

## WT-11058: format.sh unused verbose function and outdated usage function

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, test/format
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-05-11T13:03:11.000+0000
- **Updated:** 2023-05-11T21:25:45.000+0000

**Description:**
The format.sh script contains a verbose function that became unused since WT-8614.

Furthermore, the `usage` function is missing the following options:
* -d directory directory of format binary
* -T           turn on format tracing (defaults to off) (Added by WT-8372)

---

## WT-11059: No-op logging for complex tables needed

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2023-05-11T13:43:45.000+0000
- **Updated:** 2023-05-22T20:55:06.000+0000

**Description:**
In WT-10987, code was added to write a log record for truncate even when there was no work to do. That code currently only works for simple tables that have a btree handle readily available.

For a complex table, the dhandle in the `start` or `stop` cursors references the complex table. The `dhandle->handle` field is NULL. The `wt_log_op` function uses `S2BT`, which is `dhandle->handle`, to determine if the backing table has logging enabled. The fix would be to go through the schema code to decompose the complex table into its column group or index parts and check logging on those.

---

## WT-11061: Fix formatting of block comment describing __wt_session_lock_dhandle()

- **Status:** Open
- **Type:** Task
- **Priority:** Trivial - P5
- **Labels:** code-quality, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2023-05-11T20:39:28.000+0000
- **Updated:** 2023-05-11T21:26:25.000+0000

**Description:**
The block comment at the beginning of `__wt_session_lock_dhandle()` is getting mangled by `s_comment.py`.

Fix it so that it is easier to read and understand the list of "how different operations synchronize".

---

## WT-11093: Memory leaks in error paths realloc failure

- **Status:** Open
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Marc Butler
- **Created:** 2023-05-17T01:57:42.000+0000
- **Updated:** 2023-05-21T21:19:02.000+0000

**Description:**
Error paths in `src/utilities/util_load.c` and `src/utilities/util_load_json.c` assume that when realloc fails (returns NULL) any pre-existing allocated memory is freed. This is not the behavior of realloc, which *may* or *may not* free the memory if `size==0` (C11 7.22.3.4). As of C17 even this usage is deprecated, and as of C23 it is undefined. In the error paths where pointer argument may not be NULL when realloc fails, we should explicitly free the pointer.

---

## WT-11096: Improve logs related to sweep server

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-05-18T03:52:05.000+0000
- **Updated:** 2023-05-21T21:19:18.000+0000

**Description:**
The investigation of BF-28485 (and other tickets) could have been easier with more logs related to the sweep server. This ticket should look into:
* The creation of a verbose category dedicated to the sweep server
* More logs related to the activity of the sweep server

---

## WT-11097: Layering violation and potential dead code in wt_gen_drain

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-05-18T04:32:11.000+0000
- **Updated:** 2023-05-21T21:19:45.000+0000

**Description:**
In `__wt_gen_drain`, there is a check on the `generation` variable to know whether it's `WT_GEN_EVICT` or `WT_GEN_CHECKPOINT` to enable more logs when about to timeout. Two issues:
* The generation code should not reference specific users. It is supposed to be a generic implementation. We should look into removing those checks.
* The other issue is the potential dead code related to the `WT_GEN_CHECKPOINT` section. Indeed, it seems that `__wt_gen_drain` is called by `__wt_gen_next_drain` which is only called with `WT_GEN_EVICT`.

---

## WT-11100: Resolve confusion about exclusive lock requirement in __wt_conn_dhandle_close

- **Status:** Open
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** Donald Anderson
- **Reporter:** Donald Anderson
- **Created:** 2023-05-18T13:29:09.000+0000
- **Updated:** 2023-05-19T13:25:02.000+0000

**Description:**
When sweep is calling `sweep_expire`, it walks through all dhandles and for each one locks exclusively (via `WT_WITH_DHANDLE_WRITE_LOCK_NOWAIT`) on its way to calling `wt_conn_dhandle_close`. After `sweep_expire` returns, sweep calls `sweep_discard`, which again walks through all dhandles and may call `wt_conn_dhandle_close`. This one is not locked exclusively — it should also be wrapped in a call to `WT_WITH_DHANDLE_WRITE_LOCK_NOWAIT`. Additionally there is a conflicting comment in `wt_conn_dhandle_close` that says "we are holding an exclusive lock on the handle" yet also says "We don't have the sweep server acquire the handle's rwlock". This ticket is to clear up the confusion, either clarify the comment or add exclusive locking during sweep discard or both.

---

## WT-11103: Evaluate effectively of running 2 sets of unit tests in PR builds

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2023-05-19T01:47:30.000+0000
- **Updated:** 2023-05-21T21:20:35.000+0000

**Description:**
WT-10968 introduced a set of non-standalone unit tests to the PR builds, in addition to the existing set of standalone unit tests in the `ubuntu2004` builder. It would be worthwhile to evaluate the effectiveness of running 2 x sets of unit tests in PR builds, after a few months time keeping both running. We can remove the set of standalone unit tests if it turns out they do not add additional values on top of the set of non-standalone unit tests.

---

## WT-11104: Assess the history store cursor's visibility semantics

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sean Watt
- **Created:** 2023-05-19T02:10:25.000+0000
- **Updated:** 2023-06-16T18:52:04.000+0000

**Description:**
WT-11017 found an apparent out-of-order key while doing a key order check on the history store cursor. This was a false positive from the diagnostic check. To hit this scenario the history store cursor would call next or prev and cross a page boundary. Other threads would simultaneously remove the lastkey seen and insert new key(s) at the edge of the page. This occurs due to the history store cursor using read uncommitted isolation. We should reassess the behaviour of the history store cursor and determine whether we should allow the cursor to see values that appear to be out of order.

---

## WT-11107: Verify steps that cause OOO keys during insertion and deletion races

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2023-05-22T02:42:38.000+0000
- **Updated:** 2023-06-22T23:49:48.000+0000

**Description:**
WT-10961 deals with an issue where a deletion and insertion race and can cause a key to be inserted out of order. To debug WT-10961, an analysis of a few failures was done and a theory developed. Though the steps proposed in the theory are in the general ballpark of the root cause, we need to verify further.

WT-10961 is going ahead with a conservative fix that deals with the issue in general. But with this ticket, we will dig deeper into verifying the exact steps that could cause the races and the incorrect insertions, and post that if needed optimise the fix for WT-10961.

---

## WT-11110: Fix s_style in finding all paired typos

- **Status:** Open
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** Tools
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Ruby Chen
- **Created:** 2023-05-22T23:31:47.000+0000
- **Updated:** 2025-03-19T23:37:20.000+0000

**Description:**
There was a fix done for paired typos in s_style in WT-8929, however this also seemed to break s_style / the fix was not complete. A paired typo "the the" in timestamp.c was only caught on a mongodb-6.0 backport prior to this change. The original ticket on develop or the mongodb-7.0 backport did not catch this typo (see WT-11051).

---

## WT-11111: Replace interrogating /proc/cpuinfo by invoking nproc in evergreen tasks

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Marc Butler
- **Created:** 2023-05-23T04:15:37.000+0000
- **Updated:** 2023-05-23T20:23:20.000+0000

**Description:**
The number of cores is currently determined by grepping `/proc/cpuinfo` in evergreen.yml. This could be simplified by replacing with `nproc`. Also, grepping `/proc/cpuinfo` will return the number of processors reported by the system, which is *not* necessarily the number of processors available to the process, which could be restricted with a mechanism such as *cgroups*. While this scenario seems unlikely, it should be considered/validated.

---

## WT-11139: Enhance gdb dump script to support dumping on-disk page contents

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2023-05-30T07:54:16.000+0000
- **Updated:** 2023-05-30T20:53:36.000+0000

**Description:**
The existing gdb dump script supports dumping in-memory btree along with all the in-memory updates from both the insert list and update chains. It would be good that this script supports dumping the on-disk page contents to identify any problems with the disk image that is created for the in-memory page.

---

## WT-11149: Spike: Investigate improving consistency of lock usage with the txn_global structure

- **Status:** Backlog
- **Type:** New Feature
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2023-06-02T01:29:06.000+0000
- **Updated:** 2023-07-19T05:19:17.000+0000

**Description:**
As part of the shared variable review project we have identified rather inconsistent usage of locking with respect to the `txn_global` structure. Making this consistent would remove a number of confusing `WT_WRITE_BARRIER` and `WT_READ_BARRIER` usages.

// This ticket needs fleshing out.

---

## WT-11174: Investigate using thread.join during wiredtiger shutdown

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2023-06-09T00:36:39.000+0000
- **Updated:** 2023-10-19T02:29:12.000+0000

**Description:**
During `__wt_connection_close` we currently set the `WT_CONN_CLOSING` flag and then place a `FULL_BARRIER`. It's not immediately clear what the purpose of this barrier is. One of the goals of PM-3221 is to identify places where we can remove unclear uses of barriers and replace them with more common patterns such as locks. This seems like a candidate for removing the FULL_BARRIER and instead having `__wt_connection_close` wait for all other threads to join before progressing. Since this is taking place on shutdown we don't think there's any performance impacts to be considered.

---

## WT-11179: Extend to format : Test runs and repeatedly shuts down verifying everything it can each time

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality, post-mortem
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2023-06-13T00:37:29.000+0000
- **Updated:** 2024-11-19T21:35:19.000+0000

**Description:**
test/format currently does crash recovery testing or not crash restart testing, but it only ever does a single restart. A recent bug (WT-10551) required multiple restarts to reproduce in local testing. We should explore having test/format restart multiple times against the same database, to make it more likely to uncover similar bugs.

The original bug involved a backup not capturing all data, so some form of correctness checking is necessary.

---

## WT-11185: Prototype tiered storage compaction

- **Status:** Open
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** Sasha Fedorova
- **Reporter:** Sasha Fedorova
- **Created:** 2023-06-14T22:46:08.000+0000
- **Updated:** 2026-01-02T04:43:24.398+0000

**Description:**
The goal of this work is to prototype tiered storage compaction, measure and identify performance bottlenecks and research ideal algorithms for our context. There are two parts to this work:
1. Track blocks discarded from previous objects.
2. Compact by eliminating those blocks and rewriting previous objects.

The strategy is to begin with the simplest possible implementation as a baseline, measure it, analyze bottlenecks, and then research better algorithms.

---

## WT-11191: Take the latest artifacts when setting up the spawn host

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2023-06-15T06:49:51.000+0000
- **Updated:** 2023-10-10T23:28:26.000+0000

**Description:**
The spawn host setup script created in WT-11188 is only intended to handle the happy path. This ticket will add error handling for edge cases or complex problems we identify during earlier tickets.

An example of the type of error handling we'll require is covering the case when we spawn a host for a task with multiple executions. This will leave multiple possible tarballs we could extract but we should only extract the tarball of the latest execution.

---

## WT-11200: Create a session stash history buffer to track how and when a page gets freed

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sean Watt
- **Created:** 2023-06-19T06:34:31.000+0000
- **Updated:** 2025-03-25T02:40:23.000+0000

**Description:**
WT-11007 introduced some diagnostic information to track split history within pages to improve the debuggability of bugs involving split generations. To improve this further we should add a similar history buffer to the session to track when we free the split-gen stash. By tracking this we could verify which thread freed the memory in WT-10789 and when. The history would include fields like: name, func, gen, oldest, time_sec, line, len.

---

## WT-11213: Unexpected timestamp usage using dump/load wt commands

- **Status:** Open
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** code-quality, neweng, stability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-06-22T02:18:24.000+0000
- **Updated:** 2025-03-25T02:41:24.000+0000

**Description:**
When running `wt -h RUNDIR load -f dump_wt.txt` after a `wt -h RUNDIR dump "table:T00001"`, an error occurs:

`file:T00001.wt: unexpected timestamp usage: no timestamp provided for an update to a table configured to always use timestamps once they are first used: Invalid argument`

This leads to `aborting WiredTiger library`. A detailed reproduction config and stack trace are attached to the ticket.

---

## WT-11214: Improve code coverage related to logging and timestamped txn in compatibility testing

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, compatibility
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-06-22T03:44:37.000+0000
- **Updated:** 2023-07-04T10:15:21.000+0000

**Description:**
Logging is always enabled in our compatibility testing. We should randomise this value to have more coverage. The suggested changes also remove the hardcoded `transaction.timestamps=0` for non-4.2 branches and let it be randomised.

---

## WT-11215: test/format: report aggregated configuration probabilities

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2023-06-22T23:38:04.000+0000
- **Updated:** 2023-06-26T18:24:54.000+0000

**Description:**
In WT-11194, reverse collation being present caused a timeout in Evergreen. The question of "why didn't we spot this earlier" was raised. Reverse collation should happen about 10% of the time, but there was speculation that other options (e.g. mirroring) would crowd it out.

To help understand whether this is happening, test/format could, at the end of a run, report how often it ended up choosing various configuration options. It would also be an option to modify test/format to generate a large number of configurations (without running them) and base some statistics on that.

---

## WT-11216: Move away from autoconf in compatibility testing for more branches

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-06-23T04:29:29.000+0000
- **Updated:** 2023-06-23T08:03:59.000+0000

**Description:**
The oldest branch tested is 4.2 and this is the only one not working with CMake. We are currently using autoconf for all versions less than 6.0.

---

## WT-11228: Usage messages in some csuite tests incorrect

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2023-06-27T17:27:47.000+0000
- **Updated:** 2023-06-29T00:45:21.000+0000

**Description:**
When running `schema_abort` with `-B`, the program correctly said it was illegal but the usage statement shows it as a valid argument. The usage statement also does not show any of the `-P` options.

This ticket should fix three things:
1. Remove the `-B` from the usage list.
2. Generate and display the args for `-P` and any other unprinted options.
3. Look at other test programs that formerly used `-B` for similar problems.

---

## WT-11231: Create a script to detect when triage team should be notified of WT stat changes

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2023-06-28T01:00:45.000+0000
- **Updated:** 2023-06-28T01:35:23.000+0000

**Description:**
We should notify the triage team when we change stats in WiredTiger, as the triage team may need to update T2 to handle these changes.

To address this we should create a task in the `infrequent-checks` evergreen buildvariant that walks recent commits, checks for any changes in a file such as stat.h, and confirms that the associated Jira ticket has the `Teams impacted` field set to `Triage and Release`. If not, the test fails and the team is notified to update the Jira tickets.

---

## WT-11243: Rename the inmem field in test_util.h to avoid confusion

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-06-30T02:29:17.000+0000
- **Updated:** 2023-06-30T05:49:40.000+0000

**Description:**
The `inmem` field in test_util.h is confusing and would lead the reader to assume it means the in-memory configuration while it is more related to the `transaction_sync` field. It would be great to rename it to avoid further confusion.

---

## WT-11244: Uninitialized bytes in __interceptor_pwrite during bulk loading in MSAN build

- **Status:** Open
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** neweng, quick-win
- **Components:** APIs
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-06-30T05:50:02.000+0000
- **Updated:** 2025-03-25T02:47:24.000+0000

**Description:**
This issue was encountered while running the reproducer for WT-11242 in a patch build. MemorySanitizer reports a use-of-uninitialized-value in `__posix_file_write` during bulk loading. The stack trace shows it originates from `__wt_bulk_insert_var` -> `__wt_rec_split_crossing_bnd` -> `__wt_rec_split` -> `__rec_split_write` -> `__rec_write` -> `__wt_blkcache_write` -> `__bm_write` -> `__block_write_off` -> `__wt_write` -> `__posix_file_write`.

---

## WT-11251: Avoid hardcoded values for failpoints

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-07-03T03:56:24.000+0000
- **Updated:** 2023-07-04T20:42:30.000+0000

**Description:**
To reproduce WT-11242, a hardcoded failpoint probability value had to be changed from 10 to 1000. We should be able to control that value through testing or randomise the value chosen, instead of relying on hardcoded values.

---

## WT-11264: Investigate sanitizer code path completeness

- **Status:** Open
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** dev-prod, sanitize
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Clarisse Cheah
- **Created:** 2023-07-04T04:30:20.000+0000
- **Updated:** 2023-12-07T03:02:08.000+0000

**Description:**
While debugging WT-10927, a Mongo patch build picked up on a memory leak that had previously flown under the radar of our PR testing and had successfully gotten merged into our codebase. This could be a sign that our memory sanitizer code paths aren't as complete as they could be, and that we should investigate this to hopefully improve our odds of catching more bugs before it hits MongoDB.

---

## WT-11266: Directories have different files to compare in format-predictable-test (7.0,develop)

- **Status:** Open
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** stability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-07-06T01:28:10.000+0000
- **Updated:** 2024-11-03T22:41:11.000+0000

**Description:**
The error occurred in a patch build in `wt_cmp_dir`. When the home directories have different files, the script gives up with: "Directories have different files to compare, stopping." The script currently exits immediately in this case rather than reporting missing files and doing the compare for common files.

---

## WT-11291: Review all external libraries WiredTiger depends on for MSan compatibility

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** platform
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2023-07-11T00:27:34.000+0000
- **Updated:** 2026-02-26T23:45:53.660+0000

**Description:**
As part of WT-11244 we found that MSan was returning false positives for tests that used external compressor libraries (snappy, zlib, etc) because while WiredTiger was compiled with MSan instrumentation the external libraries were not. This resulted in MSan reporting unsafe memory access that were in fact safe.

This ticket is to review all other external libraries that WiredTiger may (optionally) depend on, determine if they are useful to run in MSan testing, and if so ensure we provide MSan-instrumented versions of those libraries on the testing platforms. This ticket is intended only for investigation and spinning off new tickets as needed.

---

## WT-11293: Investigate whether a read barrier is needed in hazard.c

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** wt-atomic
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2023-07-11T03:28:54.000+0000
- **Updated:** 2024-03-21T17:33:56.000+0000

**Description:**
There's a `WT_READ_BARRIER()` in hazard.c after a state check and hazard pointer increment, with a comment saying "Callers require a barrier here so operations holding the hazard pointer see consistent data." We should investigate whether it is valid. If it's valid, should we move it to the caller instead of here?

---

## WT-11304: Investigate: Determine if variables contained within a lock are used without the lock being taken in some contexts

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2023-07-13T00:11:23.000+0000
- **Updated:** 2024-03-21T17:33:11.000+0000

**Description:**
This is part of the shared variable review project. We want to expand what structures are wrapped in locks but doing so would require those locks to be taken in all situations. The purpose of this ticket is to investigate whether it is possible to detect at compile time or via a dist/ script if a lock hasn't been taken.

In theory we could use a mechanism similar to WT-10898 if we can relate the struct that the shared variables are contained within to the lock in question, and track whether it is owned or not.

---

## WT-11375: Allow the S3 extension to use AWS sso

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** supportability, tiered-storage
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sean Watt
- **Created:** 2023-07-24T01:28:56.000+0000
- **Updated:** 2025-12-22T09:28:04.412+0000

**Description:**
The AWS credential provider checks for credentials in a defined order. Instead of specifying the `AWS_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` as environment variables, the SDK can retrieve the credentials from a profile in the `$HOME/.aws/config` file. These are the credentials handled by the AWS sso. This means the developer will only have to run `aws sso login` to retrieve the new temporary credentials.

---

## WT-11376: Allow the Azure extension to use Azure AD

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** supportability, tiered-storage
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sean Watt
- **Created:** 2023-07-24T01:38:27.000+0000
- **Updated:** 2025-12-22T09:27:49.163+0000

**Description:**
Currently, we're using a "connection string" that contains the Azure account credentials, however, this poses some security risks if the keys are leaked. Instead, we can use Azure Active Directory (Azure AD) to manage identities. This means each developer can login and test the extension without using the long-term keys using the Azure Identity client library for C++.

---

## WT-11377: Allow the GCP extension to use Application Default Credentials (ADC)

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** supportability, tiered-storage
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sean Watt
- **Created:** 2023-07-24T01:42:50.000+0000
- **Updated:** 2025-12-22T09:27:31.485+0000

**Description:**
Currently, we're using an authentication file that contains the GCP account credentials, however, this poses some security risks if the keys are leaked. Instead, we can use GCP's Application Default Credentials to manage identities. This means each developer can login and test the extension without using the long-term keys.

---

## WT-11378: Investigate perf impact of eviction algorithm for pages with a lot of small updates but not big enough to trigger forced eviction

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** cache, stability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2023-07-24T04:00:34.000+0000
- **Updated:** 2024-07-23T01:46:13.000+0000

**Description:**
We have seen in WT-11279 that an eviction takes more than 4 minutes. It takes very long time because it has a lot of updates need to be moved to the history store. Generally, a page that has a lot of updates should have been force evicted early on when it crosses the page size threshold. However, in this page, the total size of the page is still relatively small (only half of the threshold size). Therefore, it doesn't trigger forced eviction.

We either need to tune parameters or improve the eviction algorithm to help evict this kind of pages earlier to avoid the long eviction in the end.

---

## WT-11379: Add support for newer GCC and Clang versions

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2023-07-24T04:23:43.000+0000
- **Updated:** 2024-03-10T21:52:36.000+0000

**Description:**
In WiredTiger Evergreen project, we have a few tasks (`compile-gcc` and `compile-clang`) to test wiredtiger source code compilation using various versions of GCC and Clang compilers. Right now we test up to GCC version 9 and Clang version 8 (WT-4858).

Newer GCC and Clang versions were released over time and we should add support for them. Specifically:
* GCC 10-13
* Clang 9-16

Please note both Evergreen tasks are part of PR builds. The addition of the newer compiler versions should not slow down PR builds.

---

## WT-11383: Implement mechanism to check variable names are compared to correct macro names in WiredTiger codebase

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Monica Ng
- **Created:** 2023-07-24T23:54:53.000+0000
- **Updated:** 2023-07-25T00:36:17.000+0000

**Description:**
Recently, a bug was fixed in row_modify.c which compares a transaction ID to a timestamp. We should implement a mechanism to check for any other occurrences of this and to prevent incorrect comparisons going forward.

---

## WT-11384: Create a perf test to assess r/w latency while stressing the maximum page size at eviction

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-07-25T03:17:08.000+0000
- **Updated:** 2025-01-23T16:22:17.000+0000

**Description:**
This task aims to understand something observed increasingly: the maximum page size seen when being evicted. By stressing a page with many updates, we would make the page grow more and more which should ultimately be evicted. The size of the page when being evicted is tracked by the metric "ss wt cache maximum page size at eviction". We have seen in the field that this can go up to hundreds of MB which is unexpected and much more than the expected value (< 10 MB).

The test should reproduce the problem and eventually expose the root cause. The cppsuite could be used for this purpose. The next steps are to create a test to assess the r/w latency while stressing the maximum page size at eviction.

---

## WT-11385: Investigate how a page with a few entries can be created despite of the existence of pages with lots of entries

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-07-25T03:24:51.000+0000
- **Updated:** 2023-11-12T23:11:24.000+0000

**Description:**
In WT-8003, a comment shows that a page with a single entry was created which is not supposed to happen when other pages with many entries exist. We should have some code in reconciliation (`__rec_split_finish_process_prev`) that should avoid creating a page with a single entry.

A branch `wt-8003-duplicate-cpp-test-repro-skewed` can be used to reproduce the issue. In the dump.txt file from running wt verify/dump, you should observe the last row-store leaf containing one or a few values.

---

## WT-11388: Investigate volatility in the overflow-130k Btree Throughput performance charts

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality, stability
- **Components:** Test wtperf
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2023-07-26T00:36:53.000+0000
- **Updated:** 2023-10-18T20:24:02.000+0000

**Description:**
Our `overflow-130k Btree Throughput` performance charts show significant volatility from run to run, in some cases the stats change from 400,000 to 2,100,000 and then back across three consecutive commits.

This ticket is to investigate the overflow-130k test configuration, and if possible apply a fix to reduce volatility. Ideally we get volatility to less than 10% but any improvements are good. For this purpose the ticket is time-boxed to 5 story points.

---

## WT-11393: Move connection locks under a separate structure

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-readability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2023-07-28T03:34:12.000+0000
- **Updated:** 2023-08-01T23:04:12.000+0000

**Description:**
There are 11 locks on the connection structure. We could move those into their own struct and reduce the number of top level fields on the connection. This would make code like `WT_ASSERT_SPINLOCK_OWNED(session, &conn->checkpoint_lock)` look like `WT_ASSERT_SPINLOCK_OWNED(session, &conn->locks->checkpoint_lock)`. While not a big difference it is a step in the direction of reducing clutter on the connection.

Scope:
* Create a new struct under connection, move its locks under there
* Clean up callers

---

## WT-11394: Investigate utilizing pthread mutex correctness attributes in WiredTiger

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2023-07-28T05:27:28.000+0000
- **Updated:** 2023-07-31T05:12:51.000+0000

**Description:**
WiredTiger currently uses default pthread mutex attributes. The non-default settings provide error checking — `PTHREAD_MUTEX_ERRORCHECK` or `PTHREAD_MUTEX_RECURSIVE` could detect misuse of mutexes. WiredTiger does set an attribute in the case of adaptive mutexes so this functionality will need to work with that.

Scope:
* Enable the relevant `pthread_mutex_attribute` (see `mutex_inline.h` and `os_mtx_cond.c`)
* Check and understand the fallout if any
* Create a PR for further discussion

---

## WT-11403: Module to induce cache pressure along a workgen workload

- **Status:** Backlog
- **Type:** New Feature
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2023-07-31T13:55:41.000+0000
- **Updated:** 2023-11-26T20:44:38.000+0000

**Description:**
Write a Python module that any workgen workload can utilise to encourage cache pressure along with the workload.

Most workloads tested lead to a stable cache constitution or predictable eviction states. Testing the system when pushed out of the stable state — specifically concerning cache constitution and eviction — will allow replication of customer scenarios and help quantify how well we behave. This work is somewhat experimental in nature.

---

## WT-11404: Do not create tiered table's local file until first write

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** tiered-storage
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2023-07-31T19:21:19.000+0000
- **Updated:** 2025-12-22T09:27:14.622+0000

**Description:**
This is a suggestion for tiered tables to create the local files with their needed metadata on demand — that is, when first creating a `table:` configured with tiering, we do not create the empty `foo-0000000001.wtobj` file. This file and its metadata entry would be created on either the first write or as a side effect of the first eviction or checkpoint.

The advantages are clear in a shared tiered store scenario: less files in the file system, fewer open file handles, and avoiding wasteful file creation for tables that may never be written to again (e.g., during restore from backup or initial sync).

---

## WT-11422: Update the cppsuite to be able to generate modify operations

- **Status:** Open
- **Type:** New Feature
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** Test CPPsuite
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-08-03T10:57:51.000+0000
- **Updated:** 2023-08-09T21:11:34.000+0000

**Description:**
Update the framework to be able to configure threads to perform `modify` operations. This ticket should update existing tests with threads performing `modify` operations wherever suitable.

---

## WT-11446: Incorrect encoding for variable length negative int

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Yury Ershov
- **Created:** 2023-08-09T07:09:56.000+0000
- **Updated:** 2023-08-16T00:46:26.000+0000

**Description:**
WiredTiger has a set of functions for variable-length integer encoding in `intpack_inline.h`. For unsigned/positive numbers, the encoding correctly adjusts the range for multi-byte encoding. However, for negative numbers the pattern is only partially followed — the negative multi-byte encoding doesn't make a correction for `NEG_2BYTE_MIN`, unlike the implementation for positive numbers.

This is inconsistent with the implementation for positive numbers and uses slightly more space for certain ranges of values. Although negative numbers are never used in WiredTiger production code, the fix would involve adjusting the negative "multibyte" encoding in the same way as for positives (subtracting `NEG_2BYTE_MIN` on encode, adding it back on decode).

---

## WT-11485: Review WT's usage of casting

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mick Graham
- **Created:** 2023-08-16T03:25:31.000+0000
- **Updated:** 2023-08-16T03:25:49.000+0000

**Description:**
WT-10740 and its follow up WT-11484 highlight that how WT casts can cause issues with modern compilers. Some of this is how the API works so we can't change that, but other parts (e.g. with the WT_CELL_UNPACK* structs) are internal and we should review the strategy to be compliant with the compiler and be easy to reason about.

The outcome of this ticket is to identify a strategy for what is good / required / bad casting with modern C and then recommend steps forward to achieve that.

---

## WT-11502: Migrate upload_stats_atlas.py to wiredtiger repo

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sid Mahajan
- **Created:** 2023-08-17T22:58:22.000+0000
- **Updated:** 2023-08-20T21:23:29.000+0000

**Description:**
We should decide whether to migrate the upload_stats_atlas.py script from the automation-script repository to wiredtiger repository where it is utilized.

Acceptance criteria:
* Investigate the pros and cons of the migration.
* Migrate the script, if that is more reasonable.

---

## WT-11503: Improve the precision of WT_CEIL_POS Macro for decimal values

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sid Mahajan
- **Created:** 2023-08-18T04:49:53.000+0000
- **Updated:** 2025-03-26T05:28:26.000+0000

**Description:**
The `WT_CEIL_POS` Macro is currently created for a specific use case, resulting in decimal values within the range of [.125, .25, .375, .5, .625, .75, .875]. However, there is a potential issue when dealing with smaller decimal values. For instance, calling `WT_CEIL_POS(0.0000000000003)` may produce an unexpected result of 0 instead of 1.

Acceptance Criteria: Extend the Macro `WT_CEIL_POS` to fix the precision concern described above.

---

## WT-11513: Create a single function for workgen workloads

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Ruby Chen
- **Created:** 2023-08-21T03:51:50.000+0000
- **Updated:** 2023-11-21T06:49:57.000+0000

**Description:**
We currently do not have a workgen test suite (not all workgen workloads are passing / being used actively) so creating an evergreen function for workgen tests would be beneficial.

We can either investigate the usefulness of cleaning up the workgen workloads and creating a workgen test suite, or just create a function for the current workgen workloads in evergreen that are all under separate functions. Those tests include chunkcache-test, split stress test and skiplist-stress-test. These can be moved under one workgen function.

---

## WT-11552: Use system clock to measure test duration in wtperf

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2023-08-25T19:09:59.000+0000
- **Updated:** 2023-08-27T20:45:42.000+0000

**Description:**
`wtperf` reports performance in operations per second by counting the number of 1 second sleeps performed by the `execute_workload()` thread while waiting for the test to complete. We should consider whether it would meaningfully increase accuracy to instead compute the elapsed run time by reading the system clock at the beginning and end of the test and computing the difference.

The benefit would be improved accuracy. On the other hand, `wtperf` has used this approach for over 10 years, and we often use it to compare performance over time where consistency may be more important than absolute accuracy.

---

## WT-11719: C/C++ Style Guide Proposal: Terminating Multi-line Preprocessor Macros with Single-line Comments

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Yury Ershov
- **Created:** 2023-09-21T05:11:18.000+0000
- **Updated:** 2023-09-22T00:26:01.000+0000

**Description:**
In C/C++, preprocessor macros can span multiple lines using the backslash (\) at the end of each line. An accidental slash at the end of a macro can unintentionally extend the macro definition. This proposal suggests terminating multi-line preprocessor macros with a single-line comment (`//`) to mitigate such risks and simplify code maintenance.

Rationale: Error Prevention, Readability, Maintainability.

Proposed style: add a `// End of macro` style comment at the end of the last line of multi-line macros.

---

## WT-11734: Improve op_rate functionality in cppsuite

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** Test CPPsuite
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sean Watt
- **Created:** 2023-09-25T07:05:34.000+0000
- **Updated:** 2023-10-06T05:56:31.000+0000

**Description:**
The cppsuite provides a configuration field `op_rate` for database operations to determine how frequently they're performed. It acts as a throttle for each worker thread to sleep for the amount of time specified by `op_rate`. At the end of a test run, the thread manager attempts to join all worker threads. However, with the current implementation, if a thread is still sleeping for the time set by `op_rate`, the main test thread must wait until it wakes up. This is problematic if `op_rate` is set for an extended period of time (e.g. a 5 minute checkpoint interval in a 10 minute test).

---

## WT-11742: Find a better way to interact with stats using the Metrics Monitor in the cppsuite

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** Test CPPsuite
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-09-26T08:08:58.000+0000
- **Updated:** 2023-09-28T05:37:29.000+0000

**Description:**
In order to make the Metrics Monitor interact with a given stat, there is a lot of overhead to make it possible. For instance, for `WT_STAT_CONN_CACHE_HS_INSERT`, a variable, a mapping, and configuration plumbing all need to be added in multiple files. This is too much work and not viable in the long term. We cannot map each existing stat unless it is done automatically. This ticket should find a better way to monitor existing WT stats from the cppsuite.

---

## WT-11748: C Style Guide Proposal: Declaration of Variables at Point of Use

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Yury Ershov
- **Created:** 2023-09-27T07:42:13.000+0000
- **Updated:** 2024-09-24T22:49:06.000+0000

**Description:**
Traditionally in C, variables are often declared at the beginning of a block or function and WiredTiger's style guide follows it. This proposal suggests adopting a style where variables are declared at the point of use, as close as possible to where they are first needed.

Rationale: Readability and Maintenance, Scope Minimization, Error Reduction, Improved Maintenance, Potential Stack Optimization.

Exceptions: Function-wide variables like `ret`, variables used multiple times across the function and ones used in macros.

---

## WT-11750: Remove cast from SKIP_FIRST and SKIP_LAST.

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2023-09-28T03:33:05.000+0000
- **Updated:** 2023-09-28T05:34:49.000+0000

**Description:**
The `WT_SKIP_LAST` and `WT_SKIP_FIRST` macros cast to a `WT_INSERT_HEAD *`, none of the callers require this behaviour and burying a cast inside a macro is bad.

---

## WT-11768: Explore what we should do as our condition variable implementation diverges from the standard definition

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2023-10-03T22:56:11.000+0000
- **Updated:** 2023-10-09T00:43:09.000+0000

**Description:**
WiredTiger has its own implementation of "condition variable". However, the guarantee it provides is different to the definition of the standard condition variable.

The rules for a standard condition variable are that all operations are as-if atomic, so a wait cannot see a notification that came before. In other words, a condition variable guarantees that earlier notifications don't wake later waiters. While in our implementation, an earlier notification *will* wake a later waiter. This creates confusion.

We can either rename our condition variable or explore whether we can use the standard implementation instead.

---

## WT-11780: Clarify the isolation levels wiredtiger doc with a diagram

- **Status:** Needs Scheduling
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** unassigned
- **Reporter:** Sid Mahajan
- **Created:** 2023-10-05T05:44:48.000+0000
- **Updated:** 2025-08-01T15:54:29.553+0000

**Description:**
After discussing with Steve Liu, it was decided to update the following paragraph in the documentation on isolation levels which appears hard to understand and needs clarification/elaboration, specifically about write skew under snapshot isolation.

To improve understanding, a diagram should be created to accompany it.

Acceptance criteria: The paragraph in this ticket should be clear and easily understood by users, with a supporting diagram.

---

## WT-11790: C and C++ Style Guide Proposal: Use Braces for Multi-line if/for/while Statements

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Yury Ershov
- **Created:** 2023-10-06T05:21:36.000+0000
- **Updated:** 2023-10-17T13:42:18.000+0000

**Description:**
WiredTiger's current C Style guide doesn't require curly braces for multiline if/for/while statements. This proposal suggests: if an `if`, `while`, or `for` statement spans more than one line, the body must be enclosed in braces. For single-line statements the preferred style is not to use braces but they can be used if it improves readability or expressiveness.

Rationale: Helps prevent the dangling else problem, enhances code clarity, makes code easier to maintain, and with auto-formatting an introduced error can be harder to spot when there are no braces.

---

## WT-11796: Create a nice-looking and comprehensive README.md for WT on GitHub

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Yury Ershov
- **Created:** 2023-10-10T00:46:37.000+0000
- **Updated:** 2023-10-10T03:14:14.000+0000

**Description:**
WiredTiger currently has a plain-text README file that has just a few links in it.

Replace plain-text README with a modern-looking markdown README.md having at least the following sections:
* Project description (with reference to MongoDB)
* How to download and compile the project (for package maintainers)
* How to make changes and test (for developers, including running code generation and verification routines)
* How to contribute
* Links that the current README has

---

## WT-11848: Identify list of noisy perf tests for investigation

- **Status:** Open
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** stability
- **Components:** Test CPPsuite, Test wtperf
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2023-10-18T20:23:38.000+0000
- **Updated:** 2023-11-06T03:02:44.000+0000

**Description:**
Spawned from a retro item. Review recently closed changepoints from the changepoint trial, as well as review the Atlas charts to identify noisy perf tests. These are the tests that either spawn multiple false positive changepoints or are visually very bouncy on the Atlas charts.

Build a list of these tests and review the impact of their noisiness. Create tickets to investigate each ticket as needed.

---

## WT-11940: Review the use of __wt_yield (sched_yield)

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, wt-atomic
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Marc Butler
- **Created:** 2023-11-07T00:09:22.000+0000
- **Updated:** 2025-11-07T04:11:16.131+0000

**Description:**
WT makes use of the system API call `sched_yield()` through the portability wrapper `__wt_yield()`. However on Linux this call is non-deterministic unless used with the real-time scheduling policies `SCHED_FIFO` or `SCHED_RR`. WT threads use the default scheduling policy `SCHED_OTHER`. The default Linux manual page is scornful of the use of sched_yield with `SCHED_OTHER`: "sched_yield() is intended for use with real-time scheduling policies... Use of sched_yield() with nondeterministic scheduling policies such as SCHED_OTHER is unspecified and very likely means your application design is broken."

This ticket is ambiguous as it involves research and consensus building as a prerequisite to any actual coding required.

---

## WT-11968: Investigate if PowerPC atomic primitives provide sufficient memory barriers

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** wt-atomic
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2023-11-13T01:59:18.000+0000
- **Updated:** 2025-03-25T23:54:13.000+0000

**Description:**
WT-8959 looked at the x86 and ARM64 platforms to ensure that the atomic primitives provided sufficient memory barriers as needed by WiredTiger. WiredTiger expects a full memory barrier, which is bidirectional for all memory accesses and not just the memory location the primitive operates on.

We also need to confirm the same for PowerPC; it is suspected that atomic increment doesn't necessarily provide the memory barrier guarantees that we need. Depending upon the appetite this ticket can be generalised across a larger set of atomic primitives and more platforms.

---

## WT-11973: Review the full barrier in __wt_sleep

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality, wt-atomic
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mathias Stearn
- **Created:** 2023-11-13T10:00:37.000+0000
- **Updated:** 2023-12-06T21:19:13.000+0000

**Description:**
There is a full barrier in `__wt_sleep` with a comment saying "it is reasonable for someone to expect" a barrier when sleeping even if not guaranteed. We should instead make it explicit in all callers that need it rather than hiding it in the sleep. We can start by mechanically just adding a barrier to every call site, then remove the ones that are obviously unnecessary.

---

## WT-11974: Take advantage of the fact that __wt_random is equivalent to flipping a coin 32 times

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, perf
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mathias Stearn
- **Created:** 2023-11-13T10:23:08.000+0000
- **Updated:** 2023-12-06T21:36:01.000+0000

**Description:**
There are a few places calling `__wt_random` in a loop with repeated probabilities, to produce a logarithmic distribution. But that can be done more efficiently by taking the 32 bits returned and running them through count leading/trailing zeros/ones, which has an equivalent probability function to flipping a coin up to 32 times and counting the number of heads in a row (stopping at the first tails).

For example, the code to choose a skiplist depth with probability 1/4 of going deeper, could be just `tmp = count_leading_zeros(__wt_random()); depth = min((tmp / 2) + 1, 10);`.

---

## WT-11975: Remove full barriers in thread creation and join code

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, wt-atomic
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mathias Stearn
- **Created:** 2023-11-13T11:55:31.000+0000
- **Updated:** 2023-12-06T21:27:05.000+0000

**Description:**
We currently use full barriers before launching a thread and after joining one. The comments say that this is because the APIs don't guarantee them. However, the C++ spec makes it very explicit for both create and join. Checking C++ stdlib implementations confirms they are using the same functions we are without any fencing. The barriers are therefore unnecessary and should be removed (noting that those functions only provide acquire/release semantics, not full barriers, so any code relying on full barrier ordering across thread creation/join would need to be made explicit).

---

## WT-12010: Testy detects corruption flag in a log record during verify

- **Status:** Open
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** Test Testy
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-11-20T08:11:32.000+0000
- **Updated:** 2025-05-06T03:53:00.000+0000

**Description:**
Testy detected a corruption flag in a log record during verify. The errors indicate:
- Log record has flag corruption 0x4176
- Log record has unused[0] corruption 0x45
- Log record has unused[1] corruption 0x4e
- Log file truncated at position 4096
- pread failed to read 128 bytes at offset 128
- Recovery failed: WT_ERROR

The EC2 instance launched successfully but the validation failed with `wt: WT_TRY_SALVAGE: database corruption detected`.

---

## WT-12035: Atomic flag set/clear should use dedicated RMW ops rather than CAS loop

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, wt-atomic
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mathias Stearn
- **Created:** 2023-11-23T16:00:31.000+0000
- **Updated:** 2023-11-27T02:33:48.000+0000

**Description:**
The definitions of `FLD_SET_ATOMIC_16` and `FLD_CLR_ATOMIC_16` in `src/include/hardware.h` are currently using CAS loops, but it is better to use the direct atomic RMW ops on x86 and armv8.1 to atomically set and clear bits. They should be a bit faster in the uncontended case and are much faster when contended.

---

## WT-12037: Slow file opens on Windows

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** stability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2023-11-23T23:03:16.000+0000
- **Updated:** 2026-01-08T05:21:17.597+0000

**Description:**
In scenarios where a Windows system is under high load and some amount of lock contention is ongoing, file opens can become extremely slow (approx. 40 seconds). This is a performance issue, and is so slow that it's causing test timeouts. A workaround is present in the code (added in WT-12036), and removing that would be part of the scope for this ticket (in addition to whatever bugfix needs to be made).

---

## WT-12045: Disable hyperthreading for x86 perf runs

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod, perf
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mick Graham
- **Created:** 2023-11-26T22:22:46.000+0000
- **Updated:** 2023-12-28T22:34:10.000+0000

**Description:**
A brief examination of our x86 evergreen hosts shows that hyperthreading is enabled. As per the MongoDB production notes, the recommendation is that hyperthreading is disabled for MongoDB.

We should run our perf tests on x86 with it disabled. We should also consider if we run a set of non-standalone tests with it which match what the MongoDB usage would be.

---

## WT-12067: Improve/Fix CRC calculation and testing on zSeries

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2023-12-01T03:44:20.000+0000
- **Updated:** 2023-12-05T07:58:09.000+0000

**Description:**
While working on WT-11899 to add a `wiredtiger_crc32c_with_seed_func` API, issues were found with zSeries due to its big endianness. The main tasks are:
1. Check if we can implement hardware acceleration for `__wt_checksum_with_seed_hw` for the zSeries.
2. Investigate why `__wt_crc32c_le` gives the correct result for CRC over chunks while `__wt_checksum_with_seed_sw` doesn't. Fix as needed and re-enable tests disabled for zSeries.
3. Note that existing CRC tests are not currently running on zSeries (tracked in WT-12057).
4. Fix `test/unittest/tests/test_crc32.cpp` which currently fails on zSeries even before the changes from WT-11899.

---

## WT-12291: __wt_file_zero doesn't need to allocate and zero a buffer

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mathias Stearn
- **Created:** 2024-01-16T16:11:54.000+0000
- **Updated:** 2024-01-22T12:06:02.000+0000

**Description:**
`__wt_file_zero` currently allocates and zeros a scratch buffer of up to 1MB. Instead we should declare a global like `_Alignas(4096) static char zeros[1024*1024];` and use a semi-magical zero buffer. This will have nice properties:
* A single zero buffer for the whole process rather than one per call.
* It will be in the .bss section so it won't take up any space in the binary.
* At least on Linux, when the virtual memory gets populated, it will all be backed by the single special "zero page", so it won't actually take up any physical memory.

It is very important that it isn't declared `const` but is treated as-if it is and never written to, even with zeros.

---

## WT-12293: Optimize our crc32 implementation for x86

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2024-01-17T01:29:51.000+0000
- **Updated:** 2024-01-22T00:59:31.000+0000

**Description:**
On x86 (at least on Intel), the crc32c instruction has a 3 cycle latency, but 3 can execute in parallel as long as they are independent, so the standard pattern is to do each chunk as 3 parallel streams and do a merge operation. See WT-2121 for some discussion. It may be worth reopening that and trying again since the test infrastructure is already set up. An Intel-authored BSD-or-GPL licensed implementation is available in the Linux kernel source.

This ticket is to explore the feasibility of this option and see how much performance gain we can potentially achieve.

---

## WT-12294: Implement generic validation of WT locking hierarchy

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2024-01-17T01:45:22.000+0000
- **Updated:** 2024-01-21T19:26:29.000+0000

**Description:**
Hierarchical locking is a well-known technique for avoiding deadlocks. We have a hierarchy for WiredTiger's major locks (e.g., the checkpoint lock must be acquired before the schema lock).

This ticket proposes changing our lock implementation to enforce this hierarchy in diagnostic mode. Each session would keep track of the "level" of the previous lock it acquired and assert that new lock acquisitions are at a lower "level". Currently some WT lock macros include assertions to make sure other locks aren't held, but this isn't a general solution.

Items for consideration include: sessions don't need to hold all locks in the hierarchy; support for both mutexes and reader-writer locks; support for re-entrant lock acquisition; handling of smaller local-purpose locks that might not need hierarchy tracking.

---
