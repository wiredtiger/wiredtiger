# WiredTiger Tickets Data - Group 2 (WT-6100 to WT-7495)

## WT-6100: Find a way to do "eatmydata" on Windows to speed up test suite.

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2020-05-01T01:50:53.000+0000
- **Updated:** 2022-04-05T00:55:02.000+0000

**Description:**
Related to WT-5801, it would be nice to have a way to have {{FlushFileBuffers}} be a no-op on Windows when running the test suite or other tests.  Like the UNIX "eatmydata" trick, we need to do this without changing source code, only changing the way that it is executed.  I don't believe that preloading a DLL with replacement symbols even possible in Windows, but there are probably some other tricks available.  See discussion in WT-5801.  It looks like this could buy us a 10-15% reduction in run time for the test suite.

---

## WT-6112: Review the utility code and make sure it works as an external application

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-05-03T23:25:38.000+0000
- **Updated:** 2022-04-05T01:05:51.000+0000

**Description:**
We are using an internal flag {{WT_SESSION_IGNORE_HS_TOMBSTONE}} in utility to expose contents in the history store. This breaks the convention that utility works as an external application without using internal apis. We need to review this and clean this up.

---

## WT-6119: History store verification does not verify the table itself

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2020-05-04T20:22:33.000+0000
- **Updated:** 2023-02-07T19:08:54.000+0000

**Description:**
History store verification checks the relationships between the history store and tables when the verifying of the metadata via the {{verify_metadata=true}} setting to {{wiredtiger_open}}. However, that setting does not verify the table itself. There is no path to do that currently. This ticket should add that call.

It is not that easy to do because the ordering of recovery, creating or opening the history store table and verify, which needs exclusive access and a closed table return EBUSY in many places. Once recovery has run, the history store ends up with references.

---

## WT-6143: Document the metadata stored along with a key value pair on a page

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2020-05-06T23:58:32.000+0000
- **Updated:** 2022-04-05T00:53:43.000+0000

**Description:**
With durable history, we store more metadata in a key/value cell and what we store is based on some conditions. It will be useful to have a document describing what goes in a cell and when.
It might also be useful to describe what are the overheads to a MongoDB document by the time it gets to be stored on disk by WiredTiger.

---

## WT-6262: Extend operation time tracking stats to report per dhandle

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-05-19T00:46:32.000+0000
- **Updated:** 2022-04-05T01:07:11.000+0000

**Description:**
It would be useful to add per table versions of the operation time histogram statistics. It should be a simple matter of adding the statistics [here|https://github.com/wiredtiger/wiredtiger/blob/develop/dist/stat_data.py#L501] to the dhandle statistics as well, and adding tracking for the DHANDLE versions to the macros [here|https://github.com/wiredtiger/wiredtiger/blob/develop/src/include/stat.h#L252].

---

## WT-6304: add option to redirect WiredTiger verbose messages to the WiredTiger log

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2020-05-25T17:07:32.000+0000
- **Updated:** 2022-04-05T01:30:38.000+0000

**Description:**
In WT-6302, we moved the format test program's operations tracing into a WiredTiger log so operations tracing could scale to longer runs. While a separate database is used by default for operation tracing, the primary WiredTiger database can optionally be used instead.

It would be useful to add a configuration where WiredTiger verbose messages could be redirected to the WiredTiger log in the same way. This would (1) give us a single trace stream with individual thread IDs and timestamps, allowing us to trace format operations both inside and outside of WiredTiger, and (2) allow verbose messages to scale with longer runs, that is, log archival would allow us to retain just the most recent set of verbose messages, so it would be possible to turn on verbose messages on long WiredTiger runs.

---

## WT-6316: Need test for backup, versions and logpath

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2020-05-27T22:13:11.000+0000
- **Updated:** 2022-04-05T01:24:12.000+0000

**Description:**
In WT-6015 we hit a bug in version checking with a backup. The initial fix in WT-5930 was not sufficient when using the log path. With a log path, and using a backup directory between releases, we did not detect the bad compatibility versions before creating the metadata and turtle file, therefore, when eventually the database was opened on a compatible release version it did not actually run recovery and reapply the records.

WT-6015 fixed it. This ticket is to create a test to verify this condition.

Basically the test should follow {{test_bug023.py}} but try to open multiple times, and use a log path.

---

## WT-6321: Upgrade Evergreen Windows distribution to vs2019

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** Evergreen
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2020-05-28T06:34:55.000+0000
- **Updated:** 2025-04-09T07:14:06.000+0000

**Description:**
In WiredTiger Evergreen projects, our macOS tests currently run on {{macos-1012}} distro, and Windows tests on {{windows-64-vs2017-test}} distro. As there are distros with newer OS versions available in Evergreen, we should consider updating to the newer version distros for our tests to match MongoDB projects and the community use cases.

---

## WT-6391: Improve set_timestamp API to ensure consistent stable and oldest usage.

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-06-09T06:09:51.000+0000
- **Updated:** 2022-04-05T01:05:42.000+0000

**Description:**
The stable and oldest timestamps are set by applications and are used internally to manage the behavior of WiredTiger.

We have some knowledge about how MongoDB is using those timestamps, and assumptions built into the WiredTiger code about those usages. We've encountered issues in the past with that approach for two reasons:
* Usage changes over time, so those assumptions can become wrong.
* When we write standalone testing applications, they don't necessarily comply with the assumptions.

We should make it so that the stable and oldest timestamps need to both be set when using timestamps in WiredTiger, and enforce that in the API.

There is a bootstrapping problem that makes that harder than it sounds. Applications want to be able to set oldest and stable independently, and in whichever order is convenient on startup. We should figure out how to allow that. Perhaps adding checks in when the first user-driven checkpoint is completed? That's not as helpful as doing the checking in the API, but may be the best choice we have.

---

## WT-6420: Stop restricting dirty cache usage with small caches

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** SEKB
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-06-15T03:34:36.000+0000
- **Updated:** 2022-04-05T00:59:48.000+0000

**Description:**
WiredTiger sets a limit on how much dirty content is allowed in cache based on a percentage of the total cache size. That works well for reasonably large caches, but does not scale down well.

We should either restrict how small that value can be or configure a threshold at which the dirty cache configurations are considered.

This will also be relevant for the new eviction configuration options related to the volume of updates allowed in cache that are being introduced in WT-6175.

---

## WT-6431: Ensure rollback to stable handles corrupted files as expected

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, code-quality, group-e, supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2020-06-17T00:53:45.000+0000
- **Updated:** 2024-05-02T03:32:32.000+0000

**Description:**
Rollback-to-stable reads unverified files, we should test that rollback-to-stable can gracefully handle corrupted files, otherwise the application will never have a chance to repair.

---

## WT-6437: Add statistics tracking history store insert types

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** neweng, supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-06-17T09:17:21.000+0000
- **Updated:** 2022-04-05T00:54:23.000+0000

**Description:**
We should add statistics that track rare(r) history store insertion types, that will help us in debugging failures in the field. If we see some of those things in use it could give us a pointer to where the problem lies.

Some examples:
Statistic tracking when something is moved to the history store from the data file due to a prepared update being evicted.
Statistic tracking when a prepared update is read back into cache.
Statistic tracking when an update with an explicit tombstone is moved to the history store (i.e: stop doesn't match start of the next newer thing in the chain).
Statistics tracking when out-of-order timestamps cause fixups (in-memory, in data file and in history store).
Statistics tracking when mixed-mode timestamps cause cleanups (I think we have at least some of these).
Statistics showing when a read from the history store is from a fixed up out-of-order record (start == stop).

---

## WT-6459: Remove the extra memory copy in __wt_hs_find_upd

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-06-21T23:00:49.000+0000
- **Updated:** 2022-04-05T01:08:02.000+0000

**Description:**
I think we can get rid of this memory copy as the comment suggested in __wt_hs_find_upd:
```
    /*
     * Potential optimization: We can likely get rid of this copy and the update allocation above.
     * We already have buffers containing the modify values so there's no good reason to allocate an
     * update other than to work with our modify vector implementation.
     */
    WT_ERR(__wt_buf_set(session, &upd_value->buf, hs_value->data, hs_value->size));
```
The upd_value->bug should have been pointing to the onpage value when we enter this function. We can resolve modify directly using that buffer. I attempted once but there are failing tests. I didn't bother to spend more time on that as we were close to the release.

---

## WT-6489: Refactor __wt_hs_insert_updates

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-07-01T01:29:28.000+0000
- **Updated:** 2022-04-05T01:06:52.000+0000

**Description:**
__wt_hs_insert_updates has grown into a very big function with complex logic. We should break it apart into smaller more manageable functions.

---

## WT-6500: History store tombstones with transaction id 0 can theoretically cause use-after-free

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alex Cameron
- **Created:** 2020-07-02T23:00:48.000+0000
- **Updated:** 2022-04-05T01:00:50.000+0000

**Description:**
While working on WT-6453, I noticed this potential problem. As far as I know, we've never seen this actually happen either in standalone WiredTiger testing or in MongoDB.

[Here|https://github.com/wiredtiger/wiredtiger/blob/develop/src/include/cursor.i#L414], we populate the transaction's {{pinned_id}} with the oldest running transaction at the time of beginning a cursor operation. This stops us from moving the {{oldest_id}} forward past this point. This is the primary mechanism that stops other threads from considering our updates obsolete and freeing them from underneath us. A globally visible update can't come after we've begun reading the chain because if it came after we started, then by definition it can't be globally visible.

Our use of id=0 in history store tombstones breaks this assumption that new things won't somehow "become" globally visible after the fact. Consider this example:

oldest_ts=6, oldest_id=150
U3@t0,id=0 -> U2@t5,id=100 (reader here) -> U1@t4,id=50

The {{oldest_id}} will normally stop subsequent updates from being considered globally visible leading to obsoletion. This is because if we open new transactions, their ids will be greater than the one we've pinned. The use of (t0,id=0) sidesteps this assumption and the update chain can be considered obsolete despite readers looking at these updates.

---

## WT-6516: Fix conditional detecting wasted reconciliation calls

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-07-09T02:02:19.000+0000
- **Updated:** 2022-04-05T01:17:54.000+0000

**Description:**
As part of WT-6488 we have [added a new conditional|https://github.com/wiredtiger/wiredtiger/pull/5873/files#diff-a6c8d99189e5865e35b893d6792f1165R200] to {{__reconcile}} that is very complex, and reaches into internal reconciliation data structures used to layout pages (e.g: {{r->multi_next}}). We should either move those checks to inside the reconciliation code so we detect the failure earlier, or in the least update the conditional to be more maintainable and not use assumptions about the internal reconciliation structure.

---

## WT-6531: Refactor incremental backup config parsing functions

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2020-07-16T15:33:40.000+0000
- **Updated:** 2022-04-05T01:13:24.000+0000

**Description:**
In reviewing WT-6215 [~keith.smith] noticed that there is overlap in parsing the configuration lines for backup information.

{{cursor/cur_backup_incr.c:curbackup_incr_blkmod}} and {{meta/meta_ckpt.c:ckpt_load_blk_mods}} have very similar parsing code paths.

These two functions have subtle differences. They compare against different strings and they fill in different data structures. A review of these function should be done to figure out a way to factor that out into one function.

---

## WT-6536: slowdown during run of test_wt2853_perf

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2020-07-21T18:13:52.000+0000
- **Updated:** 2022-07-22T00:49:47.000+0000

**Description:**
test_wt2853_perf, when modified to use 2 reader threads and 2 writer threads (or other combinations that aren't merely 1R1W), occasionally shows a performance slowdown after about 40K entries are inserted. The slowdown lasts for several seconds, and then the pace picks up again, and doesn't exhibit the slowdown again.

Background: In WT-5945, I started to do an investigation on this, but it got beyond the scope of that ticket. But anyone working on this should probably read that ticket for more context.

Also, this test case uses WT indices, which MongoDB does not. However WT indices are a pretty thin layer - It wouldn't be too hard to rewrite and simulate the use of indices, and see if that changes anything.

---

## WT-6541: Explore removing evict priority

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alex Cameron
- **Created:** 2020-07-22T23:52:44.000+0000
- **Updated:** 2022-11-14T03:54:12.000+0000

**Description:**
At the moment, we have a concept of {{evict_priority}} to make metadata file pages "stick" to the cache until we're evicting aggressively.

This is hard to get right and sometimes causes us trouble. A recent example is WT-6499 where the metadata file pages were solely responsible for eviction kicking into gear but this "stickiness" factor stopped them from getting evicted below our target.

We should explore whether it's reasonable to remove {{evict_priority}} and whether we're still getting real performance gains with this technique.

---

## WT-6545: Ensure Truncate operation does not fast truncate pages with active prepared updates.

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Vamsi Boyapati
- **Created:** 2020-07-24T02:18:21.000+0000
- **Updated:** 2022-09-14T00:51:00.000+0000

**Description:**
A page cannot be fast truncated if it has any active prepared updates (i.e. prepared updated that are neither committed or rollbacked). 
The scope of this ticket is to check whether currently it is done. If not, need to make changes to comply.

---

## WT-6565: Onpage value may be duplicated on the update chain for in-memory database

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-08-03T23:53:21.000+0000
- **Updated:** 2022-04-05T01:09:17.000+0000

**Description:**
With durable history, we always write the latest version to disk. This causes us to duplicate the on-disk value on the update chain for the in-memory database if there are older versions we cannot discard.

U@20 -> U@10

Suppose we have an insert list like above and the oldest timestamp is 5, after reconciliation, we will have

U@20 -> U@10 on the update chain and disk value U@20.

We consume more memory after reconciliation.

---

## WT-6574: Allow writing modifies to the history store in some edge cases

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-08-06T13:37:10.000+0000
- **Updated:** 2022-04-05T01:06:11.000+0000

**Description:**
In WT-6185, we disabled writing modifies to the history store in the following cases:

* There is out of order timestamp
* There is multiple operations using the same transaction id and timestamp
* We write prepare update to the data store

We should consider to rework the algorithm so we can write modifies to the history store for these cases again.

---

## WT-6590: Allow eviction of clean pages during reconciliation

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2020-08-12T19:31:49.000+0000
- **Updated:** 2022-11-14T03:54:40.000+0000

**Description:**
WT-6463 allows a session to perform eviction while it has an open history store cursor. To do this safely, this change introduced [a new check|https://github.com/wiredtiger/wiredtiger/blob/b1ec827e413fb17fe04e7f59b2472679c97367cb/src/include/cache.i#L462] in {{wt_cache_eviction_check()}} to prevent the session from calling eviction if it is currently doing reconciliation. This check is required because eviction could wind up doing reconciliation, and the reconciliation code isn't re-entrant.

[~sulabh.mahajan] observed that we should still be able to evict clean pages here, since they won't require reconciliation.  So it is possible that we could restructure this check and/or the eviction code to allow only clean page evictions when we are already in reconciliation.

We should investigate whether this would provide a substantive improvement in eviction, and if so figure out how to make the necessary changes.

The fix here probably isn't simple as eliminating the check in {{wt_cache_eviction_check()}}. That check serves the second purpose of preventing us from re-entering eviction.  My preliminary work on WT-6463 found that if we let {{wt_cache_eviction_check()}} call into eviction via {{wt_cache_eviction_worker()}}, the session can hang.

---

## WT-6601: Clang Format doesn't handle TAILQ macros well

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alex Cameron
- **Created:** 2020-08-18T00:07:59.000+0000
- **Updated:** 2022-04-05T01:17:41.000+0000

**Description:**
We had a few examples of where Clang Format has put unwanted line breaks in the middle of a struct member definition or moved the pointer alignment in an inconsistent position.

We should investigate some options on how we can do better here.

This is always going to be a challenge because macros aren't part of normal C syntax so unless the formatter works as a preprocessor (it doesn't), then it just looks like a function call has been placed where a type should be.

---

## WT-6614: verify tests exist for all parts of the API configurations

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2020-08-23T14:48:24.000+0000
- **Updated:** 2022-04-05T00:46:26.000+0000

**Description:**
We should have some way of ensuring that all parts of the API configuration possibilities are tested.  A simple manual approach would be to systematically list all API options and identifying which ones are tested (or not), and then working to keep it up to date.

A more advanced method could enforce that the coverage is up to date.  So if add a new API option is added without a corresponding Python test, that gets flagged during PR testing.  This could be aided by either tagging test functions with some annotation or better, for the Python testing itself to "track" which API configuration strings have been used and compare it against the list in {{dist/api_data.py}}.

---

## WT-6627: Unexpected WriteConflictException during insert benchmark with transactions

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-e
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mark Callaghan
- **Created:** 2020-08-27T17:47:21.000+0000
- **Updated:** 2025-10-14T19:29:01.307+0000

**Description:**
I get intermittent WriteConflictException errors while running the insert benchmark with transactions enabled and 1 client. This is unexpected because there is only one client, the only unique index is on _id which has ObjectId values and I have not experienced any problem like this for MongoDB without transactions, or for MySQL/Postgres.

With some debugging printfs the error comes from code in __wt_page_in_func trying to forcibly evict too big pages.

My requests are:
1. Don't make this visible to the user. The error occurs when I run with j:False as that allows a higher insert rate. There is no error with j:True. I am not surprised by this. However, the inserts are done by one client session and the database is small (this occurs 5 minutes into a test). So I prefer that MongoDB keep up with the client.
2. If this is visible to the user then be more truth in the error message. There is a larger discussion to be had about mapping all WT_ROLLBACK errors to WriteConflictException.

---

## WT-6631: Break up __rec_append_orig_value into two functions to simplify code

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** restructure-history-store
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-08-28T00:10:41.000+0000
- **Updated:** 2022-04-05T01:00:08.000+0000

**Description:**
This part of the code has become very complicated in __rec_upd_select because we want to reuse __rec_append_orig_value instead of directly appending the onpage value here. We can simplify this code by always appending the onpage value in this section. To be able to also reuse some code, we can break up __rec_append_orig_value into two separate functions. The first function decides whether we should append the onpage value. The second function just appends the onpage value to the end of the update chain. We can reuse the second function here in this section of code and avoid all this complex logic.

---

## WT-6638: Add testing to ensure we do not regress the database size

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2020-08-31T05:15:15.000+0000
- **Updated:** 2022-04-05T01:02:24.000+0000

**Description:**
Recently we have seen a few instances of the regression in the database size on disk for the linkbench workload. It will be very useful to have a standalone wiredtiger test to make sure that we are not regressing again.

For reference size regression tickets: WT-6532, WT-6251

---

## WT-6646: Implement ENOSPC fault injection for WiredTiger

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2020-09-02T22:27:15.000+0000
- **Updated:** 2022-04-05T00:52:58.000+0000

**Description:**
We should provide infrastructure that can be used to inject ENOSPC errors in WiredTiger tests.

WT-4065 discusses a few approaches for doing this.

A reasonable approach would be to build on the existing {{fail_fs}} file system extension.  A simple implementation would simply return ENOSPC to all space-allocating operations after a trigger point (e.g., after X MB of data has been written).  A more sophisticated approach would track the total size of all files and return ENOSPC to operations that would exceed a trigger threshold.  This would mimic the observed behavior where a fallocate request can fail to preallocate a lot of space to a log file, but subsequent (smaller) allocating writes succeed.

---

## WT-6647: Retro Action Item: Document the Code review and coding guidelines for testing for each change

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2020-09-03T00:30:44.000+0000
- **Updated:** 2022-04-05T01:00:55.000+0000

**Description:**
Document the Code review and coding guidelines for testing for each change in wiki.

After review with Leads, publish for team to follow.

---

## WT-6651: Write test to verify ACID guarantees after ENOSPC failure

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2020-09-03T17:47:41.000+0000
- **Updated:** 2022-04-05T00:51:31.000+0000

**Description:**
WiredTiger stops when it runs out of storage space.  Informal testing shows that it can resume operation when more storage space is added (WT-4065).  But we should have an automated test that make sure this is the case and also ensures that all of operations that completed before an ENOSPC failure are properly preserved after recovery.

We should implement a test similar to {{random_abort}} that uses WT-6546 to inject out-of-space errors and then verifies the database correctness after recovery.

---

## WT-6699: Create Evergreen task for modified LSWA workload

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alex Cameron
- **Created:** 2020-09-15T01:24:25.000+0000
- **Updated:** 2022-04-05T01:04:25.000+0000

**Description:**
Run the new workload in Evergreen.

We can use the existing entry for LargeScaleLongLived in {{evergreen.yml}} as a reference.

One important point is that we'll need to customize MongoDB's configuration in DSI (all workloads currently share the same one).

---

## WT-6744: Avoid duplication in test/format failure CONFIG set

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2020-09-29T19:36:55.000+0000
- **Updated:** 2022-04-26T15:20:50.000+0000

**Description:**
WT-5645 introduced a new Evergreen task cycling through a set of test/format CONFIGs (under {{test/format/failure_configs}}) that failed previously and resolved by certain WT tickets. If one of those CONFIGs were failed again in a newer testing run (indicating a new issue or breakage captured) with a new ticket created, we'd like to avoid the same CONFIG being added into the failure CONFIG set and causing test coverage duplication.

The current naming convention for the set of failure CONFIGs is {{CONFIG.WT-xxxx}}.

---

## WT-6757: Add tests for edge cases involving last block of incremental backup

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2020-09-30T18:39:34.000+0000
- **Updated:** 2022-04-05T01:17:22.000+0000

**Description:**
We should add test cases to verify that block-incremental backup behaves correctly when the last block of a file changes size between successive incremental backups.  Note that "block" in this description refers to the block sizes that incremental backup uses to track changes in WT files.

I believe what should happen in these cases is:
1. If the block size changes but the block contents don't change, the block is not marked as dirty.
2. If the block changes size and data in the block changes, then the block should be marked as dirty
3. If the block doesn't change size, but its data changes, then it should be marked as dirty.

#1 should only be possible when the file is truncated.  If the file grows, then by definition we should have written new data to it and the final block will be dirty.

---

## WT-6758: Documentation: add rows to doc "pointer" page for MongoDB releases

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2020-09-30T20:06:09.000+0000
- **Updated:** 2022-04-05T00:57:07.000+0000

**Description:**
Anyone searching the web for WiredTiger documentation will probably end up on [this page|https://source.wiredtiger.com/]. It may seem confusing that the released versions of MongoDB do not use either of the two "released" versions listed, nor do they use the "development branch".

What probably makes sense is to have additional rows that point to recent MongoDB versions, or point to pages on the mongodb.com site that can reference the version-specific documentation. Currently it seems overly difficult to find an accurate version of the documentation corresponding to the most frequently used use cases.

---

## WT-6759: Create automatic check for non-atomic struct assignments

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2020-10-01T00:02:28.000+0000
- **Updated:** 2022-04-05T00:44:52.000+0000

**Description:**
There are places in the WiredTiger code (e.g., wt_lsn), where we carefully construct structs that fit in 64 bits, and then use C assignment operations to copy the struct from one variable to another.  Because 64-bit loads and stores are atomic on 64-bit Intel processors, we assume that these struct assignments are also atomic.

Unfortunately this turns out to be a bad assumption. The C language doesn't guarantee that structure assignment will be atomic (regardless the size of the struct) and we discovered in WT-6643 that ASAN builds sometimes use a byte-wise memcpy to implement such assignments, introducing bad race conditions.

Now that we've found the problem in WT-6643, we can fix it. But we should have a tool or script that will automatically find other instances of this error. This will prevent new occurrences of this problem from being introduced and will help find other places where we may have similar errors.

---

## WT-6760: Investigate compiler-based tools for syntax & usage checks

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2020-10-01T00:53:11.000+0000
- **Updated:** 2022-04-05T01:23:41.000+0000

**Description:**
We should investigate compiler-based tools to perform the code analysis we currently do with shell scripts in {{dist/}}.  If we had more flexible tools that actually understand C, it would be easier to introduce new checks as we discover issues we want to avoid in our code.

Our current scripts are based on clever use of {{grep}} and {{sed}}.  Some of them are made possible by the style and formatting that {{clang-format}} enforces. There are limitations to what can be detected in this way. {{libclang}} lets you build custom code analysis tools using the AST generated by the clang parser.  It has Python bindings and there is an existing ecosystem of tools built using it.

---

## WT-6777: Add incremental backup performance focused test

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2020-10-07T18:26:00.000+0000
- **Updated:** 2022-04-05T00:45:06.000+0000

**Description:**
In addition to our correctness testing, we should add testing for incremental backup and performance. This could be a variant or part of {{test/csuite/incr_backup}} or it could be its own new standalone test.

The test should create several/many files that are fairly large and compare the amount of time it takes to do a full backup against an incremental. It likely wants to use a smaller granularity in order to force a lot of bits to be needed. It should compare times while modifying a hot key (or small number of keys) with a full backup. With a hot key the incremental should be much faster.

---

## WT-6787: Add more request mixes to "remove eMRCf" workload

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2020-10-09T18:54:12.000+0000
- **Updated:** 2022-04-05T01:12:02.000+0000

**Description:**
The current workload for testing WT performance with eMRC=true described in WT-6661 and WT-6776, test a couple of different workload mixes:
 * 100% insert
 * 50/50 mix of find and update
 * 90/10 mix of find and update
 * 100% delete

All of these phases operate on a single collection.

We might discover different issues with a different mix of benchmark operations. This ticket is not intended to implement and test all of the above. The goal is to identify a small number of useful ways to extend the benchmark, implement them, and run the corresponding tests.

---

## WT-6795: Remove random_directio debugging from WT once solved

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2020-10-12T15:55:07.000+0000
- **Updated:** 2024-01-18T21:26:07.000+0000

**Description:**
In debugging a very rare failure for {{random_directio}}, I've added a bunch of {{log_printf}} calls. Most are in {{random_directio}} itself, but there are a few inside the library and those need to be removed once the problem is resolved because they will affect any/all library users.

All of the library internal debugging is in {{os_posix/os_fs.c}}. Search for calls in that file to {{wt_log_printf}} and remove them when it is time.

---

## WT-6807: Windows doesn't move existing files out of the way when creating a new file with the same name

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Gregory Wlodarek
- **Created:** 2020-10-14T15:22:39.000+0000
- **Updated:** 2022-06-17T03:23:15.000+0000

**Description:**
Looking at some code, it's expected that WiredTiger renames any existing files out of the way when trying to create new files. This does happen to a certain extent, just not on Windows machines from my observation, and so I just wanted to find some clarification on the behaviour here or if this is indeed a bug.

On my Linux machine running the test case below fails and I get:
"unexpected file conflicting_rename found, renamed to conflicting_rename.1"
But on Windows, there is no renaming and the test simply passes.

---

## WT-6810: Add data source statistics for some if not all connection level statistics

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2020-10-15T05:33:32.000+0000
- **Updated:** 2022-04-05T01:04:30.000+0000

**Description:**
A large number of wiredtiger statistics are only attached to the connection, which means when debugging a mongodb failure the collStats command won't assist in understanding the issue at a collection specific level. As such we should extend most if not all collection level statistics to be also at the data source level.

Work here:
- Review all connection level statistics that aren't already included in the data source statistics.
- Add all relevant ones to the data source.

---

## WT-6814: Retro Action Item:Write a wiki page on upgrade/downgrade

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** unassigned
- **Reporter:** Deepti Hasija
- **Created:** 2020-10-20T05:02:30.000+0000
- **Updated:** 2023-04-19T08:42:38.000+0000

**Description:**
No description

---

## WT-6837: Don't insert globally hidden modifies into the history store.

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2020-10-27T05:32:46.000+0000
- **Updated:** 2022-04-05T00:49:07.000+0000

**Description:**
As a result of WT-6811, in some scenarios we may be inserting globally hidden modifies into the history. i.e. if on the update chain there is a globally visible modify, we will insert every modify until we see a full value.

---

## WT-6865: Improve timestamp usage assertion code

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dh50proj
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-11-03T06:01:10.000+0000
- **Updated:** 2022-04-05T01:20:10.000+0000

**Description:**
The timestamp [abort code|https://github.com/wiredtiger/wiredtiger/blame/develop/dist/api_data.py#L172] configures timestamp checking at the data handle (table) level. However, the code translates into checking for timestamp usage based on the data handle that happens to be in the session when the transaction is committed.

Then every table that had updates in the transaction regardless of whether it was configured for timestamp checking will have the checking applied.

It's a difficult problem to solve, since timestamps aren't generally assigned until the transaction is committed, but when the transaction is being committed there is no easy way to tie the updates back to the tables they are associated with.

---

## WT-6878: Improve configuration string handling in dhandle open

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-11-05T03:45:06.000+0000
- **Updated:** 2022-04-05T00:48:57.000+0000

**Description:**
The {{\\_\\_wt_conn_dhandle_open}} method accepts a stack of configuration strings as an argument, but only appears to use them in the salvage case (for which it passes them through to {{\\_\\_wt_btree_open}}.

The actual configuration string is stashed in {{dhandle->cfg}}. The presence of two different configuration strings is confusing. We should review the code to make the distinction clear and make the code simpler.

---

## WT-6918: lldb cannot attach to processes in MacOS - Hang analyzer

- **Status:** Backlog
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Ravi Giri
- **Created:** 2020-11-12T10:28:42.000+0000
- **Updated:** 2022-04-05T01:14:07.000+0000

**Description:**
This issue was found while working on the Hang analyzer script.

https://spruce.mongodb.com/task/wiredtiger_macos_1012_test_hang_analyzer_csuite_patch_59adb44e6d123dad25858ca12664ff77a7dbf469_5f97c0dc562343445f468731_20_10_27_06_40_28?execution=1

https://spruce.mongodb.com/task/wiredtiger_macos_1012_test_hang_analyzer_unit_test_patch_59adb44e6d123dad25858ca12664ff77a7dbf469_5f97c0dc562343445f468731_20_10_27_06_40_28?execution=1

---

## WT-6919: Windows cannot find the debug symbols - Hang analyzer.

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Ravi Giri
- **Created:** 2020-11-12T10:39:05.000+0000
- **Updated:** 2022-04-05T00:44:07.000+0000

**Description:**
This issue was found while working on the Hang analyzer script.

https://spruce.mongodb.com/task/wiredtiger_windows_64_test_hang_analyzer_unit_test_patch_59adb44e6d123dad25858ca12664ff77a7dbf469_5f97c0dc562343445f468731_20_10_27_06_40_28?execution=1

---

## WT-6920: Identify and fix references to non-existent functions in documentation

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2020-11-13T19:10:03.000+0000
- **Updated:** 2022-04-05T00:44:03.000+0000

**Description:**
In rare occasions, there are references in the documentation to API names that do not exist.  For example, in the custom data source page, if you search for "WT_SESSION::msg_printf" and "WT_EXTENSION_API::config", you'll see that these strings exist, but there is no link, since there are no functions by those names.

Ideally, we'd not only fix these, but put into place a way to identify such broken references. If we can't enforce checking for broken references, we should at least scan and fix any we see.

---

## WT-6930: Improve the test/format timestamp usage to better match MDB server usage

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2020-11-18T06:56:29.000+0000
- **Updated:** 2022-04-05T00:42:40.000+0000

**Description:**
Timestamp usage in test/format is not up to the customer use cases where the WT_NOTFOUND issue is raised. Check and improve the timestamp usage according to the customer use cases for better testing.

---

## WT-6940: Add ability to provide fail points separate to main API

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** stress-testing
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2020-11-23T00:38:40.000+0000
- **Updated:** 2023-05-03T18:47:10.000+0000

**Description:**
Whereas API to configure the fail points would be implemented along with the rest of the WiredTiger API in {{api_config.py}}, we would not like to clutter the API with individual fail points. Individual fail points can be defined in a separate definition file, {{fault-inj-def}} and then {{api_config.py}} can access the definitions by including that file. There might be some code required to be written to be able to manage API and definitions spread into multiple files.

---

## WT-6941: Implement code to parse fail points into internal data structures

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** stress-testing
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2020-11-23T00:39:34.000+0000
- **Updated:** 2023-05-03T17:17:30.000+0000

**Description:**
Design and code the internal (session scoped) data structures that save the current state of fault points configured. The data structures need to work with the API to be able to set/get the faults and their states. Consider that a fault could be:
* enabled / disabled
* All the faults could be reset to their default state
* A fault might carry a user-provided value
* A fault might carry a state of either being disabled after being hit, or remain enabled all the time (or disable after hitting N times)
* A fault could carry a probability with which it gets hit (fault triggers 1 out of N times that it gets hit)

We will need to work along with API definition to be able to provide the data structure that can efficiently manage the faults.

---

## WT-6942: Implement and test fail points which forces eviction of pages on release from a cursor

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** stress-testing
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2020-11-23T00:40:20.000+0000
- **Updated:** 2023-05-03T17:12:50.000+0000

**Description:**
Add a fault point to evict a page when a cursor stops referencing it. Write a test to drive the change. Consider what configuration options would make sense. Is the fault going to be turned off after hitting once, or do we want a series of pages to be evicted as the cursor moves along? Do we want to evict all the places the fault hits or do we want to do with a certain probability. Do we need one fail point or multiple such fail points, etc.

Also, consider what cursor operation(s) we want the fault to be associated with.

---

## WT-6943: Implement and test fail points causes sporadic failures in reconciliation at some interesting point

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** stress-testing
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2020-11-23T00:41:03.000+0000
- **Updated:** 2023-05-03T17:19:34.000+0000

**Description:**
We have heavily modified reconciliation for durable history. Reconciliation of a page can fail silently for several reasons and cause hard to debug eviction issues. It will be useful to identify some common locations where we can fail (or have failed in the past, or seen bugs in the past) and add fail points to trigger during testing. Follow the test-driven approach and write some tests, or modify existing tests to make sure that the system behaves as expected even when reconciliation fails.

---

## WT-6944: Design: Tune fail point functionality to ensure failures are as obvious as possible

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** stress-testing
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2020-11-23T00:42:15.000+0000
- **Updated:** 2023-05-03T17:18:48.000+0000

**Description:**
Spend some time designing a highly debuggable and consistent system around fault point management. The idea is that there is a consistent behaviour around each fail point, which is predictable and observable (logs). Think about a logging mechanism when a fail point:
* is hit
* is enabled/disabled
* is configured to execute at random frequency
* is set to a value

Consider what might be required as part of hitting a fault point, eg: do we generate a core differently if we were to panic, do we need to provide the fault point writer means to output a writer-defined log, etc. Also, make sure we do not overwhelm the logging system.

The ticket is limited to doing the design work alone.

---

## WT-6945: Implement: Tune fail point functionality to ensure failures are as obvious as possible

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** stress-testing
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2020-11-23T00:43:10.000+0000
- **Updated:** 2023-05-03T17:25:54.000+0000

**Description:**
This ticket implements what was designed as part of WT-6944.

---

## WT-6954: Generate RELEASE_INFO file with the git hash where the build is initated

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2020-11-24T12:25:08.000+0000
- **Updated:** 2022-04-05T00:52:53.000+0000

**Description:**
As part of continue WT release with any commit, to identify the user build version, add the git hash of the commit where the build was triggered.

---

## WT-6955: Test restart performance with large history store due to a pinned stable timestamp

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2020-11-24T15:24:51.000+0000
- **Updated:** 2022-04-05T00:42:31.000+0000

**Description:**
I recently looked at MongoDB performance when it can't advance the majority commit point and it accumulates a lot of updates in the WT history store and oplog. (See WT-6776 and WT-6786.)

We should test a related corner case.  In a PSA replica set where the secondary has failed and the history store grows a lot because MongoDB isn't advancing the stable timestamp, what happens if the primary then fails?  In particular, what is the effect on the recovery time for the primary.

As I understand, WT will run rollback-to-stable and need to get rid of most of that accumulated state in the history store.  So this is essentially asking how long RTS takes with a large (multi GB) HS file.

---

## WT-6977: Retro Action Item : Write about "Converting WiredTiger into C++ project"

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2020-12-01T07:57:21.000+0000
- **Updated:** 2022-04-05T01:13:10.000+0000

**Description:**
Write something around "Converting WiredTiger into C++ project" and walk everyone through it. The next steps will be decided after that!

---

## WT-6985: Verify the current key history store key order for every data store key order check

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2020-12-02T09:03:32.000+0000
- **Updated:** 2022-04-05T00:55:50.000+0000

**Description:**
Currently, we rarely check the cursor key order for HS. Perform a history store key verification for every datastore key to verify the updates in the history store whether they are in order not?

This may add some overhead to scan the HS for every datastore key. I feel this overhead shouldn't be a problem as the verification is performed only in a DIAGNOSTIC mode.

---

## WT-6987: Create test(s) to verify that ENOSPC errors are always reported

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2020-12-04T23:28:12.000+0000
- **Updated:** 2023-05-03T02:05:03.000+0000

**Description:**
In WT-6645, we agreed that WiredTiger should always report to the user/client when it sees an out-of-space ({{ENOSPC}}) error.  We now need tests to ensure that we are doing this consistently and correctly.

Here is the requirement:

ENOSPC errors should always be reported.
1. If an ENOSPC error affects the correctness of an API operation (e.g., causing an call to fail or WT to panic), the error should be reported to the application using existing mechanisms, such as error return codes or a handler for {{WT_PANIC}}.
2. If an ENOSPC error does not affect correctness (e.g., a failure during preallocation of a log file), it should, at a minimum, be reported as a log message.

WT-6646 discusses a couple of possible approaches to injecting {{ENOSPC}} errors.

---

## WT-6988: Replace python test suite eviction loops with debug eviction cursor

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2020-12-06T21:37:29.000+0000
- **Updated:** 2022-04-05T01:07:27.000+0000

**Description:**
A large number of python tests written for durable history and perhaps earlier use a loop that is some variation of inserting a large amount of data to force eviction. Since WT-6563 we've now got the ability to evict pages using the new debug api 'release_evict'. As such we should be able to replace the majority of these loops. This will make the test cases more deterministic and substantially faster.

Some but not all of the test cases that can be fixed are:
* test_hs06.py
* test_hs07.py
* test_hs12.py
* test_hs13.py
* test_hs15.py
* test_hs18.py

---

## WT-7016: Add new verbose messages to find out the usage of history store

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2020-12-15T00:44:43.000+0000
- **Updated:** 2022-04-05T01:27:31.000+0000

**Description:**
History store is used to save all the historical versions of the data that are required by the old readers. Currently, there are no verbose messages that exist as part of the history store to find out how it accessed.

As part of this ticket, add new verbose messages to the history store to find out its usage will help in identifying any bugs that exist.

---

## WT-7017: Document and share "Convert WiredTiger into C++ project"

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2020-12-15T05:05:04.000+0000
- **Updated:** 2022-04-05T01:20:05.000+0000

**Description:**
No description

---

## WT-7018: Add "Write gen" page in architecture guide

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2020-12-15T05:11:45.000+0000
- **Updated:** 2022-04-05T01:19:08.000+0000

**Description:**
Also include "Write gen" changes done as part of Live import project to be included in architecture guide.

If that's already captured and part of another ticket please duplicate this ticket.

Note: This has come up in retrospective for RA team.

---

## WT-7021: Review the design of the global operation timeout

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2020-12-16T02:27:00.000+0000
- **Updated:** 2022-04-05T00:50:24.000+0000

**Description:**
Currently, we have a timeout mechanism to rollback the operations that take too long, which may leads to cache stuck. The way it works is that user can set a default timeout in wiredtiger open and the default timeout will be applied to all api calls, including the cursor operations, session operations like verify, salvage, checkpoint, and etc., and connection operations like rollback to stable, and etc.

This mechanism is initially designed to help reduce the cache stuck in test/format caused by slow cursor operations so the default timeout is set to a relatively small value 2 seconds. Since this default timeout also applies to other session operations, which usually takes substantially more time to finish, the timeout mechanism may result in unexpected failures for these operations.

We need to review the design of the timeout mechanism to explore other alternatives and minimize the impact to the api.

---

## WT-7037: Investigate more compact representation for small history store records

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2020-12-22T22:24:32.000+0000
- **Updated:** 2022-04-05T01:16:46.000+0000

**Description:**
WT-6681 showed that a small modify record in the history store—corresponding to changing a few bytes of a MongoDB document—requires about 70 bytes to represent the resulting key and value in the history store.

We should investigate ways to store this information more compactly. This is only going to be a problem for customers or use cases that wind up with a lot of small updates in the history store. But addressing this would require a change to the data format of the history store, so it might be easier make the change sooner, before durable history use is widespread.

---

## WT-7042: large_scale_long_lived and large_scale_model genny workloads potentially faulty

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2020-12-23T04:35:08.000+0000
- **Updated:** 2022-04-05T00:43:14.000+0000

**Description:**
While working on PM-1986 [~alex.cameron] wrote a new workload based on {{large_scale_long_lived}} and {{large_scale_model}} workloads. The filters we use on {{_id}} for reads and updates assume {{_id}} to be numeric, whereas the load phase lets {{_id}} to be defaulted to it's {{ObjectID}} representation. We created WT-7041 to fix this bug in the new workload.

Having based the new workload on {{large_scale_long_lived}} and {{large_scale_model}}, it is very likely that these workloads are not doing the right thing at all, and doing reads/writes with filters that do not match any documents.

This ticket is to check if the workloads have been faulty and if yes, fix them.

---

## WT-7048: Review diagnostic assertions and add informational log messages

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2020-12-29T00:43:08.000+0000
- **Updated:** 2022-04-05T00:48:39.000+0000

**Description:**
We have many assertions in the WiredTiger code, but they only report any type of issue when HAVE_DIAGNOSTIC is enabled. It would often be useful to know whether an unexpected condition has been encountered in release builds as well (so we would get more information about failures in the field).

We should review all places in the code where we use WT_ASSERT, and add an informational message where appropriate.

---

## WT-7052: Investigate WT cache eviction improvements based on newer cache replacement algorithms

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2021-01-04T22:14:37.000+0000
- **Updated:** 2022-04-05T01:10:02.000+0000

**Description:**
We should investigate improvements to the WT cache eviction algorithm based on newer cache replacement algorithms.

WiredTiger approximates a widely-used cache replacement algorithm—LRU with midpoint insertion—to provide scan resistance. An obvious drawback to midpoint insertion is that a large one-time scan can still evict half of the contents of the cache. Newer cache algorithms, such as Multi Queue, ARC, LIRS, etc., have attempted to address this by dynamically changing responding to changes of locality in the request stream. None of these algorithms would directly map to the WT cache architecture, but it would still be worth investigating whether they could be adapted to improve WT eviction.

---

## WT-7061: Write "split" internal doc, to be added to Architecture Guide

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2021-01-07T19:27:37.000+0000
- **Updated:** 2022-04-05T00:51:26.000+0000

**Description:**
The page splitting implementation is described somewhat in code comments, but could use some documentation in the new Architecture Guide. This has purposely been left out of the scope of PM-1529, being deemed important but not urgent.

Specifically, it would be useful to describe the algorithms and protection mechanisms for splitting internal pages while threads may be using that internal page's leaf pages.

---

## WT-7082: Log a message or create a statistic to record a failure case when modifying the update chain

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2021-01-13T02:33:07.000+0000
- **Updated:** 2022-04-05T00:48:34.000+0000

**Description:**
We would like to either log a message or increase a statistic if we hit the failure case that modifying the update chain fails after we have successfully inserted the updates onto the update chain.

This is a follow on ticket of WT-6763, in which we discovered an edge case that after we have successfully inserted updates onto the update chain, we can still fail in {{__wt_txn_modify}}. In the failure case, we freed the updates already inserted onto the update chain in error handling, which causes a read after free issue.

We should monitor how often such case happen.

---

## WT-7096: Improve the mechanism that collects cache usage stats for the history store

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haseeb Bokhari
- **Created:** 2021-01-19T05:29:55.000+0000
- **Updated:** 2022-04-05T00:52:20.000+0000

**Description:**
We are currently collecting cache usage information for history store while in {{__evict_update_work()}} because the history store dhandle isn't always available to eviction. This is not an ideal way to collect stats as the cached version of stats is used elsewhere by eviction for making decisions and using an outdated version of cache usage can lead to subtle and hard to debug issues.

The purpose of this ticket is to explore and implement a more robust method to acquire history store cache stats whenever required. This can involve resolving issues around directly accessing underlying history store btree to acquire latest cache usage data (hint: {{__wt_get_hs_btree()}}).

---

## WT-7098: Improve autonomy in evergreen to run tests that use randomness

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2021-01-20T02:21:55.000+0000
- **Updated:** 2022-04-05T00:49:02.000+0000

**Description:**
Now with the capability of randomising the seed for random number generation in the WT-6981 changes, we now run an extra set of tests in Evergreen, called unit-test-random-seed. The unit-test-random-seed test suite only runs tests that needs to utilise the random number generator called *suite_random* and we randomise the seed with the *-R* command. Currently the test suite needs to be constantly updated manually, whenever a new test wants to use *suite_random*, ensuring test coverage.

Goal: This ticket is created to ensure to improve autonomy in the unit-test-random-seed, through finding a method to not need to maintain the list of tests manually but instead run all tests that use *suite_random*.

---

## WT-7115: Consider always running prototypes.py as part of make

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2021-01-22T20:13:49.000+0000
- **Updated:** 2022-04-05T00:50:14.000+0000

**Description:**
(See WT-7114, where we needed to back out a change to run prototypes).

For this ticket, let's consider running the {{dist/prototypes.py}} script every time we run make. That will keep {{src/include/extern.h}} up to date always, which is convenient for development. The issue with doing this is that it requires (probably for the first time) that python be available as a requirement to build WiredTiger, even if you don't need the python interface (and didn't specify {{--enable-python}}).

---

## WT-7142: Add comments explaining mixed mode testing to test_hs18

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Tammy Bailey
- **Created:** 2021-02-01T02:11:17.000+0000
- **Updated:** 2022-04-05T00:43:23.000+0000

**Description:**
While working on PM-1814, we spent a bit of time trying to figure out what the various mixed mode transaction tests were attempting to accomplish and the differences between them. In particular, the following tests could use explanation:
* {{test_multiple_older_readers}}
* {{test_multiple_older_readers_with_multiple_mixed_mode}}
* {{test_read_timestamp_weirdness}}

---

## WT-7157: Investigate 'wt downgrade' hanging when reconfiguring to incompatible version

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-b
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alison Felizzi
- **Created:** 2021-02-04T00:25:33.000+0000
- **Updated:** 2022-07-13T11:04:58.000+0000

**Description:**
When running 'wt downgrade' on a database that is initially configured with a minimum/maximum version, the reconfigure process ends up hanging/spinning indefinitely if trying to reconfigure to an incompatible version (thus 'wt downgrade' never exits).

From a small pre-investigation, after '{{__log_open_verify}}' throws the above error, we end up spinning in the '{{_wt_log_slot_switch}}' function ({{src/log/log_slot.c}}) as '{{F_ISSET(myslot, WT_MYSLOT_CLOSE)'}} evaluates to true regardless of any error (except a session panic).

---

## WT-7170: History Store truncates when stable timestamp is not set

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** bug-classification-activity-phase-2, group-b
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2021-02-08T00:14:34.000+0000
- **Updated:** 2022-07-13T11:05:29.000+0000

**Description:**
When running some of the python tests, (test_hs09, test_hs11, test_hs14), the stable timestamp is not set at all during the tests, and thus causes the history store file to be truncated.

Rollback-to-stable skips a collection with timestamped updates, when stable timestamp is either zero or not set. The sweep phase of rollback-to-stable scans the whole of history store for any timestamped updates which are not stable i.e. versions greater than the stable timestamp and discards them. This results in truncating whole of history store in cases when stable timestamp is not set.

The sweep phase of rollback-to-stable executes only during the shutdown, not during recovery.

This ticket is created to discuss this behaviour and to finalise the expected behaviour.

---

## WT-7194: Create test for drop and cursor->close interaction

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2021-02-12T00:34:44.000+0000
- **Updated:** 2022-04-05T01:24:55.000+0000

**Description:**
WT-7192 fixes a kind of tricky condition of closing a cursor opened on a dhandle that has been previously dropped.  It would be good to have a test case to trigger the original failure.  That is, develop a test that does some combination of cursor open/close and drops so that WT built without the fix fails with an assertion and WT with the fix works.

There are some challenges - the failures were only seen with address sanitizer running.  I suspect that setting `WT_SESSION_CURSOR_SWEEP_COUNTDOWN` to a small number, like 1, and `WT_SESSION_CURSOR_SWEEP_MIN/MAX` to large numbers would help it to run the sweep on every close and might trigger without the sanitizer.

---

## WT-7203: Add WT diagnostic mode test for conflicting session use by a thread

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2021-02-12T23:08:17.000+0000
- **Updated:** 2023-05-03T02:11:37.000+0000

**Description:**
It is generally not safe to actively use multiple WT sessions from a single thread.  For example, if a thread has an open transaction on Session1, it should not make WT calls with Session2.  This can lead to deadlock or cache stuck.

These issues can be hard to detect in a complex application. To facilitate debugging such issues, WT could implement some sanity checking in diagnostic mode.

One way to do this might be to maintain a table mapping threadIDs to the last session used by the thread. On a new API call, WT would check the calling thread's entry in this table and return/log an error if it corresponds to a session with an active transaction that is different than one in the call.

---

## WT-7212: Improve handling of mixed mode and out-of-order operations in history store reconciliation code

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haseeb Bokhari
- **Created:** 2021-02-16T23:43:09.000+0000
- **Updated:** 2022-04-05T01:11:27.000+0000

**Description:**
In the current code, we handle mixed mode operations (non-timestamped updates) and out-of-order operations a bit differently during history store reconciliation. For example, with mixed mode operation, we do not modify the start and stop timestamps of the entries in the data store update chain and reinsert the existing HS values with 0 timestamp in {{\\_\\_wt_hs_insert_updates}}(). Whereas, for out-of-order timestamp operations, we modify the the start and stop timestamps for the updates on the chain and reinsert the updates in history store in {{__hs_insert_record()}}.

The aim of this ticket is to refactor the code and make the code common for both scenarios.

Acceptance criteria:
1 - PR tests are passing
2 - Format stress tests are passing
3 - MongoDB patch build is passing.

---

## WT-7213: Evergreen PR testing compilation missing '-Werror'

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2021-02-16T23:47:44.000+0000
- **Updated:** 2022-04-05T01:10:11.000+0000

**Description:**
A recent merge of a stress testing ticket has highlighted a gap in our PR testing and potentially a larger overall issue.

The PR testing compilation passes the flags: {{ADD_CFLAGS="-ggdb -fPIC"}} and the long-test compilation on the waterfall job passes: {{ADD_CFLAGS="-g -Werror"}}.

The {{-Werror}} flag should've been passed in the PR testing compilation job.

We should add {{-Werror}} throughout our {{evergreen.yml}} compilation jobs and review the existing compilations to ensure that flags are consistent where it makes sense for them to be consistent.

---

## WT-7247: Separate session frame management from api macros

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2021-02-26T00:45:01.000+0000
- **Updated:** 2025-10-20T17:03:45.767+0000

**Description:**
Currently, the internal cursor calls, such as history cursor, and metadata cursor, also go through the code path in the api macros. They need to call the api macros because they need the code in the api macros that manages the session frame. However, this makes them also being treated as a user api call, which is not ideal.

We should explore separating the functionalities of managing the api macros from the api macros and not calling the api macros in the internal cursor calls.

This ticket is to do some investigation and prototyping to explore its feasibility.

---

## WT-7248: Stricter assert to ensure we don't return api call with open hs cursors

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2021-02-26T02:52:44.000+0000
- **Updated:** 2022-04-05T01:10:57.000+0000

**Description:**
This ticket is to make the assert that ensures we don't return api calls if there is open hs cursors more strict after we have resolved WT-7247.

---

## WT-7251: Add more testing for snapshot based visibility of out-of-order updates

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haseeb Bokhari
- **Created:** 2021-02-28T22:30:10.000+0000
- **Updated:** 2022-04-05T01:29:01.000+0000

**Description:**
Under current design, out-of-order timestamped updates result in fixing up of existing newer updates with older timestamps (hint {{__hs_fixup_out_of_order_from_pos()}}). We intend to keep the transaction ids on the updates intact so that older readers can still read based on transaction based visibility rules.

In WT-7200, we observed that a bug in this logic was introduced but none of the existing WiredTiger tests was able to catch it. The aim of this ticket is to write a more rigorous test for out-of-order timestamp fix-up logic.

---

## WT-7259: Add statistics from WT extensions

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2021-03-01T20:53:19.000+0000
- **Updated:** 2022-04-05T01:28:46.000+0000

**Description:**
We should think about the ability to create statistics in WT extensions. In the tiered storage implementation, we envision having some "plug-ins" that encapsulate cloud operations, so maybe three different extensions "S3", "GCP", "Azure". We certainly would like to have some statistics, so we can track things like network latency and errors.

Is it appropriate to have those statistics in the extensions themselves, which would probably require some changes to the statistics infrastructure to accommodate this use case? Or perhaps have some way to increment stats via some proxy callback code.

---

## WT-7283: Document definitions and use cases for "mixed mode" "out of order" and "ghost" timestamps

- **Status:** Backlog
- **Type:** Documentation
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2021-03-10T22:25:48.000+0000
- **Updated:** 2022-04-05T01:17:32.000+0000

**Description:**
New (and not so new) developers may want to know the definition of various timestamp-related terms that we use in WT engineering and in our Jira discussions. We should document this someplace (probably the Architecture Guide).

Three terms that come to mind are:
 - Mixed mode timestamp
 - Out of order timestamp
 - Ghost timestamp (e.g., see SERVER-45147)

In addition to explaining the meaning, we should document the expected behavior from WT, and the use case(s) they are intended to support.

---

## WT-7306: Add compatibility tests for Windows platform

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Ravi Giri
- **Created:** 2021-03-17T03:00:21.000+0000
- **Updated:** 2022-04-05T01:08:37.000+0000

**Description:**
MongoDB multi-version tests failed in windows machines with a WiredTiger error about unsupported file version. WiredTiger has compatibility tests but currently, these tests are run only in `ubuntu` machines. This ticket is created to add compatibility tests in the Windows machine.

---

## WT-7310: dupekey error on uncommited write should return WT_ROLLBACK not WT_DUPLICATE_KEY

- **Status:** Blocked
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, bug-classification-activity-phase-2-todo, group-d
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mathias Stearn
- **Created:** 2021-03-18T15:30:34.000+0000
- **Updated:** 2022-07-15T18:48:06.000+0000

**Description:**
When getting back a {{WT_DUPLICATE_KEY}}, a caller of a cursor write operation should be able to assume that a record with that key already exists. However, if the transaction that wrote the conflicting entry is uncommitted, it doesn't have that guarantee, because that write could always roll back.

The reproduction shows that when session2 tries to insert key=1 that was already inserted (but not committed) by session1, it gets WT_DUPLICATE_KEY instead of WT_ROLLBACK as expected.

---

## WT-7347: Review to remove the compare in __curhs_search_near

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2021-03-22T23:36:36.000+0000
- **Updated:** 2022-04-05T00:41:57.000+0000

**Description:**
We added a compare in __curhs_search_near when the history store cursor is initially landed on the exact key. However, we should be able to get rid of it if we can let the previous function calls to tell us if we have moved the underlying file cursor or not. Review this code to see if we want to do that or not.

---

## WT-7362: Allow batching multiple table alterations for the same config change

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Gregory Wlodarek
- **Created:** 2021-03-24T15:46:14.000+0000
- **Updated:** 2022-04-05T00:48:16.000+0000

**Description:**
On startup recovery in MDB, there are times when we need to modify the table log settings. It's known that this is an expensive operation as WT will fsync after a {{WT_SESSION::alter()}} call on each table.

We've had complaints from users that have a large number of WT files about long startup times when the table log changes need to take place.

In hopes of improving this, I was wondering how feasible it would be to have WT allow batching multiple table alterations for the same config change. Right now we can only alter one table at a time in {{WT_SESSION::alter()}}. But I'm hoping that allowing batching of these changes will improve things and allow us to fsync less often.

---

## WT-7408: API to return row and byte counts for objects and cursor ranges

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Bostic
- **Created:** 2021-04-14T17:10:49.000+0000
- **Updated:** 2025-06-18T10:18:10.464+0000

**Description:**
WiredTiger doesn't include an interface to return row- or byte-count information for objects or key ranges. This is generally useful, for example when choosing a join order.

The referenced branch implements an approach to adding this functionality.

The first part of the change is to add leaf page row- and byte-counts to each address cookie stored in the internal pages during reconciliation, and those counts are then subsequently aggregated into each new internal page to the root. At checkpoint, the counts are stored with the metadata information for the object.

The second part of the change is a new API: WT_SESSION.range_stat() taking either the name of an object or a pair of cursors that specify a range within the object.

The algorithm for estimating the cursor range information is to descend the tree to find the first internal page where the cursors diverge into different sub-trees. At that point, the row- and byte-counts between the two cursor positions on that page are aggregated and returned.

---

## WT-7418: test/format assert gets hit verifying imported table

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** bug-classification-activity-phase-2, group-d, tf
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2021-04-19T11:32:13.000+0000
- **Updated:** 2022-07-18T21:18:35.000+0000

**Description:**
As a follow on ticket from WT-7253, a test/format assert is being hit with:
`t: FAILED: verify_import/163: iteration == IMPORT_ENTRIES`

The test/format is importing a new table with 1000 entries. After successful import, we should expect 1000 entries when iterating through a cursor. However the stack trace shows WT_ROLLBACK as the return.

---

## WT-7443: Add error message when bulk cursor can't get exclusive access to dhandle

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2021-04-23T21:24:03.000+0000
- **Updated:** 2023-12-12T05:11:45.000+0000

**Description:**
Opening a cursor for bulk load can fail for a pair of similar reasons:
1. The table isn't newly created
2. The table is already open (i.e., we can't get exclusive access to the dhandle)

In the first case, WT fails with {{EINVAL}} and returns an error message. In second case WT only returns {{EINVAL}}, except on LSM which provides a similar error message.

We should be consistent across all file formats. Since it would be better to provide an explanation to the application, we should change row stores, tiered tables, etc. to follow the example of LSM.

This will require updating {{test_bulk01.py}} to expect the error message.

---

## WT-7482: Architecture Guide updates for PM-2293

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Deepti Hasija
- **Created:** 2021-05-04T04:18:36.000+0000
- **Updated:** 2022-04-05T01:09:42.000+0000

**Description:**
Please investigate if this project requires changes to the architecture guide.

---

## WT-7495: Cursor update can use pinned page search to update the key

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2021-05-06T13:19:37.000+0000
- **Updated:** 2022-04-05T00:56:04.000+0000

**Description:**
Using a pinned page search to find a proper location to update a key can yield good performance than searching the entire tree. The local pinned page search is currently used when the updates are getting inserted into the history store.

Using of pinned page search can be beneficial to the cursor update operation also when the update is getting added to data store.

---
