# WiredTiger Tickets Data - Group 5 (WT-9810 to WT-10855)

## WT-9810: Create a test application that stresses truncate and checkpoint

- **Status:** Backlog
- **Type:** Workload
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2022-08-26
- **Updated:** 2023-04-04

**Description:**
The truncate implementation in WiredTiger has an optimization where it can mark entire pages deleted, rather than marking individual records as deleted. The truncate implementation leads to have a bunch of new interesting states possible for pages in the cache.

Further, checkpoints at a timestamp require that interesting states of those pages can be reconstructed.

We should implement a targeted stress test that includes truncate and checkpoint operations. There will also need to be other operations present to ensure there is content to be truncated, and an interesting interleaving of operations.

The application should use timestamps - since re-reading content that has been truncated is more possible with timestamps, and again leads to some of the interesting scenarios.

An example of a case the test should cover is described in WT-9776, and another is covered by test/suite/test_truncate18.py

Adding a new CPP suite test application seems like a good fit for implementing this.

---

## WT-9858: Add custom data source test to WiredTiger

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2022-09-14
- **Updated:** 2023-04-11

**Description:**
Currently WiredTiger has no code tests that actively tests the data source cursor. There exists a ex_data_source.c that shows how you can configure a custom data source, but doesn't actively test the data source. This ticket aims to add coverage to custom data sources, and properly test out the custom session, and cursor opening by creating a csuite test.

I would use the ex_data_source.c as reference.

---

## WT-9859: Deduplicate the page skip code for deleted pages

- **Status:** Backlog
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** David Holland
- **Created:** 2022-09-14
- **Updated:** 2022-09-14

**Description:**
There are two functions used for skipping deleted pages during tree walk: wt_btcur_skip_page, used by cursor_next/prev, and wt_delete_page_skip, used implicitly by everything that isn't RTS, including, redundantly, cursor_next/prev.

They are not quite the same but do mostly the same checks, including locking the ref to check the page-delete information, which isn't free.

Point 1: it isn't clear if they really need to be different. The most significant difference is that wt_btcur_skip_page also checks the time aggregate to allow skipping pages that are not deleted as such but have no visible content anyway; maybe there's a reason that's not suitable for other tree walks (besides RTS) but if so it should be discovered and documented.

Point 2: the common checks should be deduplicated. In addition to not checking the page-delete information twice, we should really only lock the ref once. This suggests that if the time aggregate check is not suitable for other tree walks it should probably become a flag rather than a custom skip function.

Note 1: wt_btcur_skip_page unconditionally returns false on FLCS trees, because on FLCS iteration has to step through otherwise nonexistent areas of its key space returning zero. This is not necessary for the deleted-page checks; FLCS doesn't support fast truncate because of this iteration issue, so deleted pages don't appear at all anyway. However, it is almost certainly needed to shortcut the time aggregate check. While in the abstract FLCS pages should contain zeros rather than nonzero values with stop times, a nonzero value with a stop time is considerably cheaper to store than a zero value combined with a history store entry for the nonzero value. Therefore, stop times do appear and need to be accounted for in the rest of the system. Consequently, if the time aggregate check gets moved to wt_delete_page_skip the VLCS check needs to go with it.

Note 2: wt_btcur_skip_page returns false unconditionally for all internal pages. This applies only to the time aggregate check; the time aggregate isn't necessarily up to date even if the page isn't marked dirty. Therefore, if the time aggregate check gets moved to wt_delete_page_skip some form of this logic needs to go with it.

Note 3: while internal pages are never fast-truncated, they _can_ be deleted, e.g. if they reconcile empty. In these cases ref->page_del will always be null, that is, the deletion is always globally visible, and they can always be skipped.

Note 4: currently wt_btcur_skip_page does not handle the visible_all case, because I guess it's never called from a visible_all context; be aware of that if transplanting parts of it.

Note 5: wt_btcur_skip_page contains a comment about locking the ref to access the time aggregate, but the time aggregate is accessed via wt_ref_addr_copy, which is AFAIK supposed to be safe without locking the ref. Locking the ref is, however, required to access ref->page_del.

---

## WT-9875: Index cursor CRUD functions do not check if the key operation is set

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2022-09-16
- **Updated:** 2023-11-21

**Description:**
Index cursors are a special type of cursor that has it's keys at the values of the table. The wiredtiger documentation describes this feature quite well https://source.wiredtiger.com/11.0.0/schema.html#schema_indices.

Currently the standard behaviour of a cursor read, insert type operation is that it checks if the key is set, and returns back an error if the key is not set. There is currently a bug within the index cursors implemenetations where we do not do this check, eventually running into an assert getting hit.

This ticket should delve into the cur_index.c file and look at possible operations that require a key, and add the `__cursor_checkkey` such that we don't hit the assert. The developer will also need to write a test to make sure this is all correct now.

---

## WT-9880: Create a hierarchy for incompatible settings in test/format

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality, test/format
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-09-19
- **Updated:** 2022-09-22

**Description:**
test/format verifies incompatible settings when generating/parsing a configuration. The hierarchy was partially removed through WT-9771.

As part of this ticket, one should document this hierarchy which is defined in the `config_transaction` function present in the test/format/config.c file. Once this hierarchy is defined, this may impact the changes done in WT-9771.

Motivation: To improve our testing coverage.

Acceptance Criteria: Make sure test/format does not generate configurations with incompatible settings. Add a comment to the code explaining the chosen hierarchy.

---

## WT-9883: test/format table_ops positioned variable doesn't reflect the cursor position properly

- **Status:** Open
- **Type:** Build Failure
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** Test Format
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2022-09-20
- **Updated:** 2025-03-25

**Description:**
Currently the *table_ops* function grabs any table cursor and performs operations on the cursor. The table_ops function is used by all operation threads to perform different CRUD operations to stress WiredTiger.

There currently is a bug where test/format assumes that the table cursor that is fetched will always be unpositioned from the start, through the *positioned* variable being set to FALSE. The problem is that this is not necessary true at all due to *snap_verify*. *snap_verify* grabs any table cursor and checks if any snapslot isolation mismatches. It does this through doing a cursor->search() call and then verifying the contents of that search. After cursor->search(), *snap_verify* does not free up the position of the cursor. This leads to a bug where *table_ops* shouldn't be assuming that the cursor it fetches is not positioned.

---

## WT-9884: Remove the default (read) timestamp used in session.query_timestamp()

- **Status:** Backlog
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sean Watt
- **Created:** 2022-09-20
- **Updated:** 2022-11-09

**Description:**
Currently WiredTiger documentation states that the setting multiple commit timestamps feature "is not compatible with prepared transactions, which must use only a single commit timestamp." We intend to remove using read as the default configuration and instead return an error if no configuration string is provided.

Motivation: To align the timestamp API with the way MongoDB uses it.

Acceptance Criteria: There are no current tests that use session.query_timestamp() in this way so testing should not be affected. Update the documentation to reflect these changes.

---

## WT-9887: Error path testing for statistics handler on open/close

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2022-09-20
- **Updated:** 2024-05-21

**Description:**
When WT-9670 gets merged, it adds a new event handler that allows the user to access statistics while `wiredtiger_open` is still running (particularly recovery/RTS) as well as during close. While `wiredtiger_open` is still running, any session opened via `conn->open_session` will be severely restricted in the APIs it can call.

Ticket WT-9670 has correct functional code added into `test/csuite/timestamp_abort` to gather statistics during that time.

This ticket should create or modify some test to use the event handler in an incorrect manner. It should try to call disallowed APIs or open a non-statistics cursor and make sure the proper errors are returned.

The concept is simple but it is not a simple addition to a python test due to the use of the event handler and forking a thread to perform the operations (WiredTiger is not re-entrant - checking that too would be a good idea). So this may need to be a C-suite test or some other mechanism.

---

## WT-9929: Investigate the generation of traces when the IOPS are getting slow

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-09-29
- **Updated:** 2024-05-21

**Description:**
Investigate if we can generate traces when the IOPS are getting slow. We need to define what "slow" is.

Motivation: It would be great for the support team to know when there are I/O issues. Depending on the workload and the type of disk, those traces could help the investigation.

Acceptance Criteria: Make sure traces are generated.

---

## WT-9941: Spike to improve unit test coverage

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** unassigned
- **Reporter:** Jeremy Thorp
- **Created:** 2022-10-04
- **Updated:** 2023-03-29

**Description:**
No description

---

## WT-9949: Set core file pattern on static hosts at beginning of Evergreen tasks

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2022-10-05
- **Updated:** 2023-04-04

**Description:**
Static Evergreen hosts (e.g. PPC or zSeries) aren't ephemeral, and config changes made by both our project and others will stick around between tasks.

The "core file pattern" (/proc/sys/kernel/core_pattern) is one such config change. This file tells Linux how to dump a process's memory and generate a core file, and can either be an executable to call (e.g. systemd-coredump) or a simple file pattern that will end up in the current directory.

Unfortunately, other tasks that run on these hosts sometimes change the core pattern. When we get a test failure, the core file will thus end up somewhere other than we expect (i.e. the current working directory). It can be manually fixed by running `echo 'dump_%e.%p.core' > /proc/sys/kernel/core_pattern` as root, so we should look into automatically doing this for some tasks.

Two notes: we may not have permissions to touch that file, and it's not a problem on dynamic hosts (e.g. Ubuntu ARM or x86) since those spin up a new machine each time.

---

## WT-9951: Add flexibility to the format-failure-configs-test task

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-10-06
- **Updated:** 2023-07-19

**Description:**
We have a failure_configs folder that contains different test/format configs that failed in the past and could reproduce an issue. Those configurations are executed through the `format-failure-configs-test` task in Evergreen which is executed on the `ubuntu2004` variant.

A test/format configuration may reproduce a failure on a specific variant. Limiting them to be executed on `ubuntu2004` does not guarantee that the failure has not been reintroduced.

Furthermore, it is not always the case that a configuration fails on the first run. Sometimes it needs multiple executions and running parallel jobs may be required too.

We should execute the test/format configurations on the right variant, where the issue was detected or execute the Evergreen task on different variants. To address the likelihood of reproducing the issue, we could use the `format.sh` script and use the `-j` and `-t` options to vary the number of jobs and the time.

---

## WT-9952: Assess correct usage of setting multiple commit timestamps

- **Status:** Backlog
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sean Watt
- **Created:** 2022-10-06
- **Updated:** 2022-11-27

**Description:**
WiredTiger documentation states that the setting multiple commit timestamps feature "is not compatible with prepared transactions, which must use only a single commit timestamp." However, this is not currently enforced by WiredTiger. For example, it is possible to create a transaction with the following timestamps:
1. Begin transaction
2. Prepare transaction with prepare_timestamp = 10
3. Set the commit_timestamp = 20
4. Set the commit_timestamp = 15
5. Commit transaction with durable_timestamp = 30

It is currently up to the user to know that this is an invalid use of timestamps. We should make it more clear to the user if they violate this constraint.

Acceptance Criteria: There should be no fallout after adding stricter checks around setting multiple commit timestamps. We should extend timestamp tests or add new ones if we add warning messages or asserts. If we decide not to tighten the constraints in wiredtiger the documentation should be more explicit about this scenario.

---

## WT-9962: Add contributors information to the WiredTiger repository

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2022-10-09
- **Updated:** 2023-03-22

**Description:**
As an open source project, WiredTiger should be more welcoming to external contributors, by giving some information about how to get started.

We should add a reference to a "CONTRIBUTING" document to the main GitHub landing page README.

The contributing page could follow the model of the equivalent MongoDB page. It could even reference the same wiki page, but it should add caveats about WiredTiger being a different project, with different coding standards.

---

## WT-9972: Understand why s-clang_tidy is no longer used

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** dev-prod, quick-win
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-10-12
- **Updated:** 2022-12-19

**Description:**
The s_clang-tidy does not seem to be used at all. It would be great to:
- Figure out if we should use it.
- If we decide to use it, add it to `s_all` or `evergreen.yml`. If not, remove it.

The following tickets might help to get some history about this file: WT-4934, PM-1398

---

## WT-9976: Update clang_format to enforce with C++ coding guidelines

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** cpp-guidelines, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-10-12
- **Updated:** 2022-10-12

**Description:**
We need to create a tool and/or update clang_format to ensure our C++ code is correctly formatted.

---

## WT-9977: Update Workgen with the new C++ coding guidelines

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** cpp-guidelines, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-10-12
- **Updated:** 2023-03-28

**Description:**
No description

---

## WT-9978: Update the S3 extension with the new C++ coding guidelines

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** cpp-guidelines, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-10-12
- **Updated:** 2023-08-21

**Description:**
No description

---

## WT-9979: Update the timestamp simulator with the new C++ coding guidelines

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** cpp-guidelines, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-10-12
- **Updated:** 2022-10-12

**Description:**
No description

---

## WT-9980: Update the cppsuite with the new C++ coding guidelines

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** cpp-guidelines, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-10-12
- **Updated:** 2022-10-12

**Description:**
No description

---

## WT-9981: Update the cpp unit tests with the new C++ coding guidelines

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** cpp-guidelines, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-10-12
- **Updated:** 2022-10-12

**Description:**
No description

---

## WT-9986: Fix JSON cursor bug triggered by allocator changes

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** Donald Anderson
- **Reporter:** Donald Anderson
- **Created:** 2022-10-13
- **Updated:** 2023-04-04

**Description:**
A result of modifying the allocator slightly in WT-9950 is that the JSON cursor code broke. See the commented out scenarios in test_jsondump01.py. The old code may be relying on realloced memory being completely cleared. In `__wt_json_alloc_unpack`, after the call to `__wt_realloc_noclear`, a fix of doing a memset of 0 may suffice, but we should understand why it needs to be cleared.

---

## WT-10006: Catch2 fails to build and raises "raising cygheap base mismatch detected" on Windows

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2022-10-17
- **Updated:** 2025-03-23

**Description:**
The problem signature is:
```
[2022/10/16 23:12:54.489]   git checkout -b <new-branch-name>
[2022/10/16 23:12:54.489] HEAD is now at 216713a4 v2.13.8
[2022/10/16 23:12:54.489]       0 [main] sh (2388) C:\Program Files\Git\usr\bin\sh.exe: *** fatal error - cygheap base mismatch detected - 0xEF6410/0xFB6410.
```
This might be a tooling issue - the error seems to originate in the git shell running on cygwin. Might be one for the build team.

Test failure message: `FAILED: catch2-populate-prefix/src/catch2-populate-stamp/catch2-populate-download`

---

## WT-10028: Allow changing block allocation algorithm with WT_SESSION::alter

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** APIs, Block Manager
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jordi Olivares Provencio
- **Created:** 2022-10-25
- **Updated:** 2026-03-02

**Description:**
In WT there's the notion of choosing the block allocation algorithm to use for a given table. However this can only be set during creation in the `WT_SESSION::create` method. We wish to be able to set it afterwards with a call to `WT_SESSION::alter`.

Motivation: In cases with a very high Oplog churn rate the Oplog can cause file extensions. WT can then proceed to select pages near the end of the file even if there are large chunks of empty blocks earlier in the file. This blocks file truncation as there's always data near the end. Selecting a different block allocation algorithm would help in those cases.

Acceptance Criteria: When we can select the block allocation algorithm to use for a table at runtime.

---

## WT-10034: Ensure wt can be built on all the available distros

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** dev-prod, supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-10-27
- **Updated:** 2024-01-04

**Description:**
When preparing a binary for arm64 amazon linux 2 platform, the corresponding distro doesn't have the environment to build WT. We need to ensure all the distros can readily build WiredTiger if required.

---

## WT-10045: Update WT_ASSERT to take a failure_reason string

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2022-10-27
- **Updated:** 2023-06-19

**Description:**
All WT_ASSERT macros other than WT_ASSERT take a string describing the impact of a failed assertion. This ticket will add a new argument to WT_ASSERT for this purpose. For this ticket we'll update all WT_ASSERT calls to take an empty string for the failure_reason. Correctly populating this argument across the ~1000 calls to WT_ASSERT in the code base will take place in later tickets.

---

## WT-10048: Add operation tracking support for truncate operation in cpp test framework

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Monica Ng
- **Created:** 2022-10-27
- **Updated:** 2022-10-27

**Description:**
WT-10032 adds support for the truncate operation in the C++ test framework. We should also implement support to track and validate truncate operations. This requires some more considerations though.

For other operations like insert, update, and delete it was more straightforward as we only had to keep track of one key. The operation tracking for truncate could be implemented by iterating through all the keys in the desired range and then marking each as deleted but this does not seem efficient at all. There may also be scenarios where we don't specify a start/stop key on the truncate range, requiring a traversal through a large portion of the table.

---

## WT-10079: Automate the Python compatibility test update step for cutting WT releases

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod, open-source-release
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2022-11-02
- **Updated:** 2023-05-29

**Description:**
When cutting an open-source WiredTiger release, there's one step to update the Python compatibility tests (i.e. `test/suite/test_compat0[1-4].py`) in order to extend the log version compatibility testing coverage for the newer release version. Right now it's a manual update step that requires careful checking and application of the documented procedure, which is tedious and error-prone.

We should aim to automate this manual update step with a script that can be invoked when cutting an open-source release, to help achieve release automation.

We should also evaluate to see if it makes sense to rotate some older release versions out from the set of Python compatibility tests when adding new versions in.

---

## WT-10099: Establish performance metrics to monitor workload rates over time

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Tammy Bailey
- **Created:** 2022-11-03
- **Updated:** 2023-02-22

**Description:**
We will monitor workload rates for significant changes over version updates. This ticket is to establish:
- what performance metrics we will monitor
- what constitutes a significant change
- how we will save and view this information

---

## WT-10100: Update the WiredTiger test triage wiki

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Tammy Bailey
- **Created:** 2022-11-03
- **Updated:** 2023-02-22

**Description:**
Update the test triage wiki page to reflect the new requirements for monitoring the test framework.

---

## WT-10121: Improve the testing around standalone and non-standalone

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-11-08
- **Updated:** 2022-11-09

**Description:**
We need to decide and improve the frequency and coverage of the tests run in standalone and non-standalone config.

---

## WT-10130: Review wtperf_run.sh for removal

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Trivial - P5
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2022-11-09
- **Updated:** 2022-11-11

**Description:**
We have a handful of scripts such as `bench/wtperf/runners/wtperf_run.sh` that were previously used in Jenkins but no longer seem to be used. We should review these scripts, confirm if they're still needed, and delete as required.

The scripts to review are the following:
- wtperf_run.sh
- wtperf_track.sh
- wtperf_ckpt.sh
- wtperf_xray.sh

Acceptance Criteria: The scripts are removed, or we can justify why we should keep them.

---

## WT-10145: Enable 'page_stats_2022' flag in standalone builds

- **Status:** Backlog
- **Type:** Task
- **Priority:** Trivial - P5
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Gregory Wlodarek
- **Created:** 2022-11-11
- **Updated:** 2022-11-13

**Description:**
We should enable the 'page_stats_2022' flag to be true by default in standalone builds once we have done enough testing in our local environment.

---

## WT-10154: Improve public wiki description of good Jira ticket content

- **Status:** Open
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** dev-prod, quick-win
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2022-11-14
- **Updated:** 2022-12-19

**Description:**
We recently created a new public wiki page to help guide high quality ticket creation.

We should improve that page so that it's actually helpful and complete.

---

## WT-10156: Upgrade/downgrade testing for record count

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** Monica Ng
- **Reporter:** Vamsi Boyapati
- **Created:** 2022-11-14
- **Updated:** 2023-05-03

**Description:**
Supporting the record count feature at the table level will involve data format change. Add testing to support upgrade/downgrade between the versions lower version not supporting and higher version supporting.

Acceptance Criteria: Testing for both upgrade and downgrade scenarios.

---

## WT-10158: Add test for record count with only updates

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Vamsi Boyapati
- **Created:** 2022-11-14
- **Updated:** 2023-05-03

**Description:**
No description

---

## WT-10177: Automate updates to documentation landing page to support release

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod, open-source-release
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2022-11-17
- **Updated:** 2022-11-17

**Description:**
In the open-source release process, there's one step to edit the documentation landing page in `src/docs/top/main.dox`, which needs to be merged into the code base before running some of the later steps. It's currently a manual editing step that can be automated.

The existing `dist/s_release_docs` script (used by a later step of the release process) seems to have covered the required automation but has the side effect of touching the documentation Git repository. We should check and see if it makes sense to customize this script to support the needed automation in this ticket.

---

## WT-10182: Add configuration to s3_store to turn off file caching

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, supportability, tiered-storage
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2022-11-17
- **Updated:** 2025-12-03

**Description:**
There should be a way to "unconfigure" the use of a file based cache for storage sources. In the longer term, we may have a different layer of the system (block_cache?) handle caching of blocks in files or fragments of files. In the shorter term, we need our microbenchmarks to run without caching.

Note that loadable extensions have a different way of doing configuration than the rest of WT, typically any options need to be specified when the .so/.dll is loaded into memory.

---

## WT-10200: Consider removing deleted ref cleanup during checkpoint

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2022-11-21
- **Updated:** 2023-06-16

**Description:**
We recently fixed a bug in WiredTiger related to checkpoint causing splits up the tree in WT-9477. That fix was necessary because checkpoint walk doesn't handle structural changes in the tree.

There is code in `split_parent` that avoids copying across WT_REF structures associated with deleted pages. That code currently allows for the WT_REF structures to be freed while a checkpoint is happening if and only if the split (and therefore free) is done by the checkpoint thread.

That reasoning sounds very similar to the prior reasoning in WT-9477, we should review whether it is safe and necessary for checkpoint to free such WT_REF structures during a split.

Definition of done: Confidence in the correctness of freeing deleting refs while a btree is being checkpointed.

---

## WT-10208: Consider ways to free statistics array for dormant data handles

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2022-11-23
- **Updated:** 2025-10-29

**Description:**
Data handles (dhandles) for files have a statistics array. Statistics are not allocated for other kinds of data handles. There are currently 265 statistics tracked for each data handle. Each statistic uses 8 bytes, so sizeof(WT_DSRC_STATS) == 2120 bytes. But each file dhandle actually has 23 of these allocated - so a file dhandle has 48760 bytes, just for statistics.

When a collection stops being actively used for a time, the dhandle sweep thread will close the file and btree. However, it must keep the dhandle around because there are references to it from session caches. When there are hundreds of thousands of collections (each would have at least two file dhandles) the memory cost adds up significantly.

It seems like at the time we close the files, we could free the storage for the stats. Freeing this memory timely is a simple change that could have a big effect when there is a sweep lag.

---

## WT-10210: Create a way to remove obsolete config fields from WT metadata

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2022-11-23
- **Updated:** 2022-12-12

**Description:**
When we discontinue or remove a feature from WiredTiger, we cannot also remove any associated config values from the metadata, as it causes issues with upgrade and downgrade. Today there are about a dozen of these obsolete config options in `api_data.py`, which we are carrying forward for backward compatibility.

There are at least two problems: once a config option exists in the metadata, we have to keep it forever in `api_data.py`; and obsolete config options often exist in the WT metadata, storing extra metadata for options we no longer support.

A tangential concern is that the permanent nature of metadata config options interferes with agile development.

---

## WT-10222: Track and Evaluate Pull Request building time

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2022-11-25
- **Updated:** 2023-10-18

**Description:**
Pull Request building/testing time is a critical metric that affects engineer efficiency. We should have a way to track this metric, its trend over time, define a certain threshold, and alert when the threshold is reached.

It would be nice to have a prescribed way to evaluate the metric data, and suggest feedback on what change/improvement should be made.

---

## WT-10224: Create a common testing runner/framework

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2022-11-25
- **Updated:** 2022-11-27

**Description:**
In the WiredTiger codebase, we support quite a few different testing frameworks: C suite, Python suite, test/format, CPP stress, etc. Each testing framework has its own test runner that engineers need to run separately in their local development environment.

Creating a common testing runner/framework is expected to bring benefits:
- Run a single command that can cover all needed tests from various testing frameworks
- Changes that apply to most/all testing frameworks can now have an obvious place to be made

The resmoke used in mongo server codebase is a good example of such a common testing framework.

---

## WT-10227: Unnecessary deleted page instantiations

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** David Holland
- **Created:** 2022-11-27
- **Updated:** 2022-11-28

**Description:**
One of the common causes of needing to instantiate a deleted page is that cursor search will unconditionally read it in order to return WT_NOTFOUND. This is unavoidable if the cursor is being positioned in order to write; however, for read it seems like we ought to be able to detect that the page is deleted and return WT_NOTFOUND directly without reading it in.

(Note that cursor_next and cursor_prev will skip over deleted pages; but explicit search does not.)

This is not entirely trivial and might turn out to be more impossible than I thought, but it potentially allows saving a fair amount of work and is therefore worth considering.

---

## WT-10228: Terminology reform for "proxy cell"

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** David Holland
- **Created:** 2022-11-27
- **Updated:** 2022-11-28

**Description:**
At one point the cells used by fast-truncate were called "proxy cells" and some traces of this terminology remain, e.g. WT_CHILD_PROXY in rec_child.c.

Getting rid of the last of them (e.g. changing WT_CHILD_PROXY to WT_CHILD_DELETED) will make the code clearer.

This is far from urgent or impactful but it's also not a large amount of work.

---

## WT-10244: Unresolved issue in many-dhandle-stress.py

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** stability, workgen
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-11-29
- **Updated:** 2022-11-30

**Description:**
In the file, we can read the following:

```
# Updated the range_partition to False, because workgen has some issues with range_partition true.
# Revert it back after WT-7332.
```

However, WT-7332 is now marked as fixed. It is unclear if Workgen can now handle `range_partition` or if it's still not the case.

---

## WT-10252: Define a Workgen operation that can insert/update random k/v pairs of random sizes

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** workgen
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-11-30
- **Updated:** 2023-05-03

**Description:**
After WT-10238 is done, Workgen will be able to see new tables on the fly.

In PM-2712, we want to be able to insert/update random key/value pairs in the newly created tables. Each pair needs to be of a random size.

As of now, an operation does not need to be associated with a table when constructed which sets the `_random_table` to `true`. However, we still need to indicate the `Key` and `Value` objects in the constructor which limits us. This ticket should implement a solution where this is no longer required and the key/value pair can be generated on the fly with random sizes.

---

## WT-10280: More detailed statistics for RTS

- **Status:** Backlog
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2022-12-02
- **Updated:** 2023-05-03

**Description:**
Stats on the volume of data rolled back and the lowest stable timestamp across all removed updates (probably per-collection) would be a good addition to the existing RTS logs.

Another improvement highlighted on a recent help ticket would be an RTS summary including at least:
- total time spent in RTS
- number of pages traversed
- number of keys rolled back

And possibly some extra items, if reasonable.

---

## WT-10282: "Debug" optimisation level not applied to MSAN builds

- **Status:** Backlog
- **Type:** Bug
- **Priority:** Major - P3
- **Labels:** dev-prod, quick-win
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2022-12-05
- **Updated:** 2025-03-25

**Description:**
While investigating another issue, a lot of variables were optimised out on an MSAN build, and stepping through code in GDB was jumping all over the place. It looks as though the debug optimisation level fixes previously made aren't being applied correctly - investigate and fix.

---

## WT-10308: Missing test cases in packing-test.c

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality, neweng
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2022-12-07
- **Updated:** 2022-12-08

**Description:**
It seems that there are missing test cases in packing-test.c:

```c
#if 0
    /* TODO: need a WT_ITEM */
    check("u", r"\x42" * 20)
    check("uu", r"\x42" * 10, r"\x42" * 10)
#endif
```

Please check if more tests are required or if this is tested somewhere else already.

---

## WT-10313: Create auto test test/format config script

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** Jie Chen
- **Reporter:** Jie Chen
- **Created:** 2022-12-08
- **Updated:** 2023-05-03

**Description:**
This ticket aims to create a script to automate the process of finding the best config reproducer out of all the BFGs. The script will grab the configs from each BFG, and attempt to reproduce the test/format failure. The end result should be that we find the best config to reproduce the failure.

The script should look at all the available test/format failures in Jira with test/format name and attempt to reproduce the problem. The script will run on a host machine.

Definition of Done: A basic script that finds the best config out of all the BFGs on a test/format failure ticket.

---

## WT-10322: Investigate refactoring common functionality in WT storage source extensions

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2022-12-11
- **Updated:** 2023-03-29

**Description:**
At the completion of PM-2681 we will have four storage store extensions: DIR store, S3 Store, Azure store, and GCP Store.

There would be a significant amount of code that would be common across the extensions. The object store extensions (S3, Azure, GCP) would all have a separate class to implement the interaction with their cloud provider. Then each would have similar code to translate between "object" semantics and the "filesystem" semantics. We can explore means to share the translation code between the extensions and reduce code duplication.

---

## WT-10337: Add basic cache read tracing

- **Status:** Needs Scheduling
- **Type:** New Feature
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** unassigned
- **Reporter:** Michael Cahill
- **Created:** 2022-12-15
- **Updated:** 2024-04-23

**Description:**
The first step in investigating whether cache modelling can be applied to WiredTiger's cache is to add tracing. Initially, add a verbose mode that logs a message every time a page is read. Write scripts to parse those messages and do basic sanity checking.

---

## WT-10339: Improve tests, benchmarks to emulate session pooling

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2022-12-15
- **Updated:** 2022-12-20

**Description:**
It would be good to have an option to wtperf, workgen, and/or other test or benchmark programs to more closely emulate some characteristics of MongoDB session management.

mongod uses a pool of sessions for its operations. Generally, some work unit grabs a session from the pool, does a bunch of operations that may involve opening cursors, then closes cursors (which would be cached at the WT level), then returns the session to the pool. Sessions are added and taken from the pool in LIFO order.

Our current test programs and benchmarks have a different model from MongoDB. We generally spin off a bunch of threads, and each one opens its own session that is normally kept open for the duration of the run. It would be great if we had the option to have these threads periodically "change sessions", taking them from a pool.

---

## WT-10388: Investigate tools to check shell code portability

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Chen
- **Created:** 2022-12-20
- **Updated:** 2022-12-22

**Description:**
In the WiredTiger code repository, there are some shell scripts (e.g. `dist/s_all`) that are set to call system shell (`#!/bin/sh`), which could be "translated" into varied types of shells in different OS systems/distributions, e.g. bash, dash, zsh. Having a way to check shell changes being portable across different types of shells before code merge is useful.

shellcheck has a "portability" feature that seems relevant. We can explore other tools as well.

---

## WT-10396: Use stat cursor instead of a separate api to retrieve the record count

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2022-12-20
- **Updated:** 2022-12-20

**Description:**
In the new design doc, we have decided to use stat cursor instead of a new api to retrieve the table stats.

---

## WT-10427: Investigate the cause of cursor_copy causing failures in test/format

- **Status:** Open
- **Type:** Build Failure
- **Priority:** Major - P3
- **Labels:** stability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jasmine Bi
- **Created:** 2022-12-22
- **Updated:** 2022-12-28

**Description:**
The issue is linked with WT-9506 (adding debug mode configs to test/format), where adding cursor_copy in the configurations causes failures. The developer would want to investigate the root cause of the issue and correctly add cursor_copy to debug mode test configs.

Definition of done: The ticket will be complete once test/format can randomly turn on/off cursor_copy by adding it to the configure_debug_mode function in the wts.c file.

---

## WT-10454: Review FIXMEs and their associated tickets.

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2023-01-04
- **Updated:** 2023-01-04

**Description:**
WiredTiger has ~94 FIXME's in the codebase as the time of creating this ticket. We should check to make sure that the ticket being referenced is not closed and is going to be done. And if a FIXME doesn't reference a ticket one should be created or it should be removed.

The scope of this ticket is to review the FIXMEs in the codebase for validity. It can be broken down into chunks if required.

---

## WT-10455: Cleanup TODOs in the WiredTiger codebase

- **Status:** Open
- **Type:** Task
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Luke Pearson
- **Created:** 2023-01-04
- **Updated:** 2023-06-16

**Description:**
TODOs aren't considered the style correct way of leaving a piece of work for a future ticket. The preferred mechanism is `FIXME-WT-0000` which is also checked by a script so tickets can't be merged without cleaning up their respective `FIXME` comment.

Despite TODO not being stylistic we have around ~230 TODOs in the codebase. We should consider removing, or replacing with FIXMEs and an associated ticket.

---

## WT-10457: Modify data format to support statistics cursor for byte and record counts

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** Monica Ng
- **Reporter:** Monica Ng
- **Created:** 2023-01-04
- **Updated:** 2025-06-10

**Description:**
We will need to make changes to the existing data format to enable the persistence of record count and byte size statistics for files. The exact implementation details are still being determined and will be populated as we have more information.

---

## WT-10470: Review benchmarks and hardware used for automated performance testing

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2023-01-09
- **Updated:** 2023-06-16

**Description:**
The WiredTiger automated performance tests aim to give signal about introduced performance regressions. They are currently run using "regular" Evergreen hosts, which don't provide predictable I/O performance.

We should:
- Review the hardware used when running performance tests, and ensure it is capable of generating repeatable results.
- Review the workloads being run in our automated performance testing, and ensure that the workloads could give predictable results.
- Clearly identify tests that exceed the hardware capability and identify the value they provide.

---

## WT-10477: Include page_del_committed in visibility check for page_del structures

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Will Korteland
- **Created:** 2023-01-09
- **Updated:** 2023-01-10

**Description:**
As part of WT-9847, we observed that the bug wouldn't have occurred in the first place if `wt_txn_visible_all` had checked `wt_page_del_committed` for a `page_del` structure. We should at least investigate whether this is reasonable to include, since it's a cleaner approach than anything dealing with `page_del` structures having to check two things for visibility.

---

## WT-10481: Change WT_STAT_NONE to use max uint64 and change stats to use uint64_t

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Chenhao Qu
- **Created:** 2023-01-10
- **Updated:** 2023-01-10

**Description:**
Currently, we use -1 for WT_STAT_NONE and we have to define the stats as int64_t, this has caused in many places we have to do type cast from uint64_t to int64_t. If we can use max uint64_t as WT_STAT_NONE, we can avoid the type casts.

---

## WT-10484: Verify WiredTiger versions 11.0 and 10.0 are running on Windows

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** dev-prod, open-source-release
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-01-10
- **Updated:** 2023-01-18

**Description:**
It seems that WiredTiger cannot be installed correctly from PyPi. See this message for context.

---

## WT-10554: Make Windows build process/documentation better

- **Status:** Open
- **Type:** Documentation
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-01-31
- **Updated:** 2023-02-01

**Description:**
The documentation related to how to build on Windows seems outdated, it needs to be reviewed and updated as needed.

---

## WT-10612: Add a new WT_TIME_POINT structure to hold transaction id and both commit and durable timestamps

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Haribabu Kommi
- **Created:** 2023-02-15
- **Updated:** 2023-02-16

**Description:**
Currently, to check for the visibility of an update using the `__wt_txn_visible` function, we pass the transaction id and both commit and durable timestamps as individual parameters.

By creating a new WT_TIME_POINT structure that holds the transaction id and both commit and durable timestamps, it will be easy to pass this structure wherever we need to check for the visibility and it simplifies all the callers of both `__wt_txn_visible` and `__wt_txn_visible_all` functions. This structure can also be used in other places where the individual parameters are passed.

---

## WT-10634: Documentation and test changes corresponding to bulk operations

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** bulk-operations, supportability
- **Components:** none
- **Assignee:** Jeremy Thorp
- **Reporter:** Etienne Petrel
- **Created:** 2023-02-22
- **Updated:** 2023-07-07

**Description:**
The investigation of WT-10545 led to results that need to be investigated further.

When a bulk cursor closes, the dhandle associated with the file is closed and a checkpoint on the file is performed. This is not a system-wide checkpoint but a single-file one. If this is followed by a crash, the data inserted by the bulk operations is not present after a restart despite the single-file checkpoint.

Goals:
- Create a test that verifies the expected behaviour
- Update the documentation if required

---

## WT-10639: Investigate the tests left behind in random_directio

- **Status:** Open
- **Type:** Technical Debt
- **Priority:** Major - P3
- **Labels:** code-quality, directio
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-02-23
- **Updated:** 2024-03-12

**Description:**
A bunch of test scenarios were created as part of WT-4225:
```
# Here are successively tougher schema tests that do not yet
# reliably pass.  'verbose' can be added to any.
#$RUN_TEST -T $threads -S create,create_check       || exit 1
#$RUN_TEST -T $threads -S create,drop,drop_check    || exit 1
#$RUN_TEST -T $threads -S create,rename             || exit 1
#$RUN_TEST -T $threads -S create,rename,drop_check  || exit 1
#$RUN_TEST -T $threads -S all,verbose               || exit 1
```
The goal of this ticket is to check whether they are still worth adding and enable them if that's the case. Rename has been removed and no longer a part of above tests.

---

## WT-10641: Explore adding statistics for pages requested and read in cache by application threads

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2023-02-23
- **Updated:** 2023-02-27

**Description:**
We could get a cache hit/miss estimate using the current statistics, but those statistics are across all the threads, internal as well as external. This means that internal operations like checkpoint, history store management, writing metadata, etc are also reflected in these statistics.

The application can get a better insight into a cache hit/miss ratio if we were to also compute "by application threads" version of these statistics. It was also pointed out that having these or similar statistics per query would be even more helpful.

---

## WT-10651: Investigate methods to install google cloud dependencies on evergreen machines

- **Status:** Backlog
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** none
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2023-02-28
- **Updated:** 2023-05-04

**Description:**
WT-10502 installs google cloud's external dependencies through doing a curl command, and building the binaries onto the system. This method is not preferred because it can mangle with the system's pre-built installed binaries.

The purpose of this ticket is to investigate better methods of fetching google cloud's external dependencies:
1. Asking the evergreen build team to install all three packages into the evergreen machines.
2. Investigate if it is possible to install the packages via WiredTiger's cmake build system via usage of submodules, externalProject or fetchContent.

---

## WT-10668: Investigate what diagnostic correctness checking could be added to the skip list and other lock free data structures

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mick Graham
- **Created:** 2023-03-01
- **Updated:** 2024-11-19

**Description:**
Investigate what diagnostic correctness checking could be added to the skip list and other lock free data structures. This is time-boxed to five days.

For the skip list, some initial thoughts: if the search result is valid, the next_stack structure should have insert_list entries with lower level key larger than or equal to the higher level key and all the keys in the next_stack should be smaller than or equal to the search key. We can also verify that the next key of the first key in the next_stack should be larger than the search key. In addition, we can walk every level of the insert list to verify the keys are in order.

---

## WT-10669: Review WT perf tests to ensure they cover MongoDB like use cases with appropriate concurrency

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mick Graham
- **Created:** 2023-03-01
- **Updated:** 2024-11-19

**Description:**
Review WT perf tests to ensure they cover MongoDB like use cases with appropriate concurrency.

---

## WT-10675: Add open_session config to set session name

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2023-03-01
- **Updated:** 2023-03-01

**Description:**
In WT-10340, we added messages for sessions that "did not run a sweep for 60 minutes". The message uses the `WT_SESSION->name` to show a helpful message. But this session field is only set when running an internal WT session; it is not user settable. We should consider having a `WT_CONNECTION->open_session` config like `"name=..."` to allow applications to set names, and perhaps have the session name be visible in more messages.

The specific example to motivate this was noticed by MongoDB. MDB has a session that is used to flush the journal. It doesn't have any cursor activity, so can't really be a "rogue" session. The message pops up after about a minute from when mongod starts. It would be good if the message was tagged with the session name so we would know that it was benign.

---

## WT-10694: Compile third party lib on Windows

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** cmake, windows
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-03-03
- **Updated:** 2023-03-05

**Description:**
WT-10661 describes an issue compiling compressors on Windows.

This ticket should focus on other libraries such as sodium and memkind and any other available when the ticket is done. The solution could be the same.

---

## WT-10696: GDB fails to load source files when compiling with gcc in mongodbtoolchain v4

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** dev-prod
- **Components:** Developer Productivity
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sean Watt
- **Created:** 2023-03-03
- **Updated:** 2025-03-25

**Description:**
GDB seems to have trouble locating the correct source files from a coredump after compiling with GCC 11.2 in the v4 mongodbtoolchain. Using clang in the v4 toolchain provides a sufficient workaround for compiling tests locally. However, the concern is that we may have the same trouble from a dropped core if a test in evergreen compiles with the above GCC.

Example:
```
#3  0x00007feb94758378 in __wt_txn_commit (session=0x7feb94a0f010, cfg=0x0) at /home/ubuntu/wiredtiger/build/time_inline.h:1530
1530    /home/ubuntu/wiredtiger/build/time_inline.h: No such file or directory.
```

---

## WT-10718: Investigate the conditions to open a checkpoint cursor

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** checkpoint_cursor, code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-03-07
- **Updated:** 2023-04-03

**Description:**
The following condition may be modified if we don't need to compare the oldest/stable timestamps with the snapshot time:

```c
if (first_snapshot_time != snapshot_time || ds_time > snapshot_time ||
  hs_time > snapshot_time || stable_time > snapshot_time ||
  oldest_time > snapshot_time)
```

The reasoning is that the stable/oldest timestamps should not matter. If a checkpoint has been created at a certain time, we should be able to open the data at that time regardless of the oldest/stable timestamp values.

---

## WT-10768: Create a dedicated command for wt util to explore a file

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** supportability, wt_util
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-03-17
- **Updated:** 2023-04-04

**Description:**
WT-10727 made the dump command from the wt util tool interactive when the `-e` option is specified.

It seems that this is a great candidate to be a command on its own which could be called `live`:
```
./wt live file:<file_name>
```

It would be great to support the various options supported by the `dump` command such as: `-c checkpoint`, `-j` (JSON format), `-p` (pretty-print), `-t timestamp`, `-x` (hexadecimal encoding).

---

## WT-10769: The -E option of format.sh is not working as expected

- **Status:** Open
- **Type:** Bug
- **Priority:** Minor - P4
- **Labels:** code-quality, neweng
- **Components:** Test Format
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-03-17
- **Updated:** 2025-03-25

**Description:**
The `format.sh` offers the possibility to skip errors through the `-E` option.

In WT-5820, the call to the function `skip_known_errors` was removed.

The ticket should investigate if it's worth it to put back this feature or remove the dead code.

---

## WT-10782: Create a script to check for trailing whitespaces in python and evergreen files

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Jie Chen
- **Created:** 2023-03-20
- **Updated:** 2023-03-22

**Description:**
Currently s_all script only checks the c, h and cpp files for trailing whitespaces within the code files and fixes them. There is not functionality right now that checks trailing whitespaces in python and evergreen files. This ticket plans to add this functionality so that the code becomes more consistent and clean.

Definition of done: A script is written to check and can potentially fix the whitespaces automatically in python and evergreen files. The PR should also fix all the whitespaces in the current codebase.

---

## WT-10788: Evaluate whether to consider internal pages for dirtied by a transaction

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2023-03-22
- **Updated:** 2023-06-16

**Description:**
WT-10027 modified the code which now includes a comment saying to exclude the changes to the internal pages, but the code includes them. This ticket will explore whether to fix the comment or the code.

The relevant comment says: "For application threads, track the transaction bytes added to cache usage. We want to capture only the application's own changes to page data structures. Exclude changes to internal pages or changes that are the result of the application thread being co-opted into eviction work."

---

## WT-10794: Save WT files in cloud storage as part of test artifacts

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2023-03-22
- **Updated:** 2023-03-23

**Description:**
When there is an Evergreen failure on a test that uses tiered storage, we should capture any WT files in object storage (e.g., S3) as part of the test artifacts so that we can refer to them during debugging.

Several approaches are possible:
1. After test failure we find and copy cloud objects to a directory that is included in the artifacts tarball
2. We don't delete the cloud objects, and instead save something with the artifacts that would tell an engineer where to find them.
3. We implement a shim layer that saves a copy of any object when it is written to the cloud.

---

## WT-10795: Add fflush calls in random_abort and other parent/child programs

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Susan LoVerso
- **Created:** 2023-03-22
- **Updated:** 2023-03-22

**Description:**
In WT-10789, `random_abort` running recovery in the parent process hit a segfault. The output from the program in the log seems to be missing output from the child and the parent. The reason is buffered output on stdout.

There are only a few calls to `printf` in the program for user-level output. This ticket should:
- Add calls to `fflush(stdout)` to the locations in the child and parent to flush the output.
- The other parent/child `*_abort` programs should also be looked at for a similar issue and fixes made there too.

---

## WT-10801: Update skip list comment to include speculation in description

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2023-03-22
- **Updated:** 2023-03-26

**Description:**
The resolution to WT-10461, which was a bug related to CPUs speculatively executing code, was simple, but included a lot of comments describing why the changes were necessary.

It would be useful if the comments mentioned that speculative execution is involved, since `ordering` that we use in the comments at the moment is ambiguous. Both the compiler and CPUs reorder operations, but only CPUs are speculative.

Specifically referring to comments in row_srch.c.

---

## WT-10824: Create a tool to automatically parse and categorize checksum mismatch failures

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod, supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Keith Smith
- **Created:** 2023-03-27
- **Updated:** 2025-11-04

**Description:**
We occasionally see checksum mismatch failures – both in our internal testing and in the field. Since data integrity is extremely important, it would be useful to collect data about as many of these failures as possible to see if we can identify common patterns and/or causes.

The proposal is to add further processing after detecting a checksum mismatch to provide more information about what went wrong:
- Did we read data that looks like valid WT data?
- Did we read data that is at a different location in the file?
- Does retrying the read produce the correct data?
- Does the data look like garbage?
- Does the data look "almost" right – i.e., could it be correct except for one or two flipped bits?

---

## WT-10828: Add workgen and ext/storage_source to s_string

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2023-03-28
- **Updated:** 2023-07-20

**Description:**
Our .cpp workgen and ext/storage_sources files are currently not covered by s_string, as the workgen and s3_store files are not using the proper snake_case as per our style guide and our spellchecker aspell doesn't support camelCase on all platforms. Once these files have been updated to our WiredTiger cpp style (using snake_case) s_string can be run on these files.

We'll need to handle any issues raised by s_string, and for any foreign API calls that are camelCase or ProperCase add them to the whitelist.

---

## WT-10829: Redact AccountKey when printing out configuration passed into WiredTiger

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, security, tiered-storage
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Andrew Morton
- **Created:** 2023-03-28
- **Updated:** 2025-12-03

**Description:**
When running WiredTiger with tiered storage we pass the field `tiered_storage.auth_token` which will contain details for accessing the backing cloud storage.

However, if a parsing error occurs on the provided configuration string WiredTiger will print the string including the token. For evergreen failures this results in the token being saved to our evergreen logs unintentionally.

As an interim measure we should redact this key in WiredTiger as part of `__config_err`. A review of api_data.py doesn't show any other fields that will require this redaction, so we can limit this change to just auth_token.

---

## WT-10832: Investigate reconciliation split logic not creating reasonably sized pages

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2023-03-28
- **Updated:** 2024-01-16

**Description:**
We have seen some recent evidence that WiredTiger is inefficiently laying data out on leaf pages. That is managed by the reconciliation split logic - we should review that logic looking for places where it might make poor decisions.

The case in hand has pages with an average of 600 bytes of data, when leaf_page_max is configured to be 16KB. Durable history is enabled. It is on an index in MongoDB, which probably has small key/value pairs being randomly updated across the data, along with 5 minutes of pinned history.

---

## WT-10833: Implement a mechanism to combine small on-disk pages together

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2023-03-28
- **Updated:** 2024-01-16

**Description:**
It would be useful to have a mechanism in WiredTiger that identified when leaf pages don't have much content, and combined them together. That would mean that we can create more efficient tree structures.

The functionality could be linked in with the `WT_SESSION::compact` API, or just part of normal operation. The goal of this ticket is to describe how pages can be combined back together.

This is particularly relevant if WT-10832 turns out to be an issue with reconciliation creating leaf pages with an inefficiently small amount of content.

---

## WT-10839: Add cursor reset to commit in cppsuite

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Mick Graham
- **Created:** 2023-03-28
- **Updated:** 2023-03-28

**Description:**
In WT-10706, an issue was found where after a txn commit the cursors were not reset (until after a sleep/sync) and it led to cache issues. There was a suggestion to call reset cursors (`__wt_session_reset_cursors`). That was attempted however it was discussed that `__wt_session_reset_cursors` looked at ncursors on the session which is only incremented for file cursors.

In order for this ticket to be successful (and allow the removal of cursor resets from the cppsuite) we would need to first deal with ncursors and then test for the impacts.

---

## WT-10842: Improve the HS validation by checking hs_counter

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, hs
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-03-29
- **Updated:** 2023-03-30

**Description:**
Currently, the HS validation only checks that all the keys present in the HS are present in the DS. We can do more than this and check for the `hs_counter` field present in the key.

This counter is incremented every time two consecutive keys are made of the same fields (`btree_id`, `key`, `hs_start_ts`). Two identical keys have their `hs_counter` differ by 1. We should check for this.

---

## WT-10843: Improved support for transient tables

- **Status:** Open
- **Type:** Improvement
- **Priority:** Minor - P4
- **Labels:** refinement
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Sulabh Mahajan
- **Created:** 2023-03-29
- **Updated:** 2023-04-05

**Description:**
The query uses temporary tables to act as spillable data structures. The durability guarantees WiredTiger provides make using temporary tables harder or less efficient for the use case. We can explore means to support a use case of temporary tables.

Requirements:
- Table-like interface
- No durability guarantees
- No timestamps support required

We could also consider the impact on the cache expected from having such a table, whether the checkpoints would skip this table, etc.

---

## WT-10844: Try to combine __wt_hs_verify_one and __hs_verify_id

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality, hs
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Etienne Petrel
- **Created:** 2023-03-29
- **Updated:** 2023-03-30

**Description:**
The goal is to re-use the code and make `__wt_hs_verify` use `__wt_hs_verify_one`. `__wt_hs_verify_one` could take two new arguments: a hs cursor and a ds cursor. If those are NULL, open them. If not, reuse them.

This way, we could call `__wt_hs_verify_one` from `__wt_hs_verify`. Since `__wt_hs_verify` opens those two cursors already, we could pass them to the new `__wt_hs_verify_one`.

If this makes the design cleaner, easier to understand and to test, we should proceed.

---

## WT-10845: Add statistics that give insight to cached disk image size

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** supportability
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Alexander Gorrod
- **Created:** 2023-03-29
- **Updated:** 2024-01-16

**Description:**
It would be useful to be able to look at WiredTiger statistics and get a feel for common disk-image sizes associated with pages in the cache.

This might look like a histogram with counts of page sizes in buckets. Those statistics would also be interesting per table (collection or index).

Having the information would help us reason about some application behaviors regarding cache and memory utilization, and would also give us a tool for checking to ensure a healthy on-disk tree is being generated.

---

## WT-10850: s3 subsystem not printing error messages by default

- **Status:** Open
- **Type:** Task
- **Priority:** Major - P3
- **Labels:** code-quality, supportability, tiered-storage
- **Components:** Tiered Storage
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2023-03-29
- **Updated:** 2025-12-03

**Description:**
Looking at WT-10752, we see a failure from what we presume is an S3 network error, but there is no output from the s3 module. The s3 logging code in s3_log_system.cpp uses `_awsLogLevel` to control whether logging takes place, but it doesn't appear to be initialized except when explicitly configured via `verbose=(tiered)`. In particular, it looks like calls to `s3->log->LogErrorMessage` are lost unless `verbose` is set in the wiredtiger_open connection string.

It seems like we should always receive those messages. We should see if gcp and azure have similar issues.

---

## WT-10853: Avoid complete stdout/stderr dump of massive files for Python tests

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** dev-prod
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2023-03-30
- **Updated:** 2023-03-31

**Description:**
There are some test runs that may require a lot of work to diagnose because of a huge (possibly irrelevant) dump of a stdout.txt file. If the stdout is verbose output, it may be tens of thousands of lines long. The code in evergreen.yml unconditionally dumps those lines to the console, overwhelming the evergreen tooling.

We should consider changing the `find` command to use "head -1000" rather than cat, or better yet, deduce that the file is huge and indicate that the output is truncated.

---

## WT-10855: Lock free lists using CAS and generations

- **Status:** Open
- **Type:** Improvement
- **Priority:** Major - P3
- **Labels:** code-quality
- **Components:** none
- **Assignee:** [DO NOT USE] Backlog - Storage Engines Team
- **Reporter:** Donald Anderson
- **Created:** 2023-03-30
- **Updated:** 2023-04-05

**Description:**
This ticket describes a lock free mechanism to handle linked lists and potentially other data structures, using WT generations to manage the disposal of removed objects.

There are already some great ideas to do lock free data structures in WT-10609. This idea differs in that it uses simple data structures, pretty easily adapted from the sort of lists we use now. Its disadvantage relative to both WT-10609, and a pure locking approach, is that it cannot provide an isolated view of the items.

The algorithms are explained in the comments.

---
