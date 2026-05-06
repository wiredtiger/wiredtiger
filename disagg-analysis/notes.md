# DisAgg Testing Gaps Analysis — Notes

**Analyst:** Ivan Kochin  
**Driving ticket:** WT-17225 — [Disagg Testing Gaps] Analyse open Jira tickets and FIXMEs to identify testing gaps  
**Date started:** 2026-05-06

---

## Goal

Identify missing and gap testing scenarios for WiredTiger's Disaggregated Storage (DisAgg) feature by:
1. Analysing all open/backlog WT Jira tickets from 2025–2026 for DisAgg testing signals
2. Analysing FIXMEs in the codebase (separate step)
3. Compiling a final report (WT-17226)

This document covers step 1: the Jira ticket analysis.

---

## Approach

### Data collection strategy

The Jira MCP tool (`jira_search_issues`) returns at most 50 results per query with no offset/pagination parameter. To collect all tickets, the 26 months from January 2025 through May 6, 2026 were queried using date-range splitting:

- **Low-volume months** (< 50 tickets): single query for the full month
- **High-volume months** (≥ 50 tickets): bi-weekly splits (1st–15th, 16th–end)
- **Very high-volume months**: further splits to 5-day intervals

Months requiring sub-splits:
- **July 2025**: 3 ranges (1–15, 16–23, 24–31) — July 24–31 alone had 46 tickets
- **April 2026**: 5 ranges (1–10, 11–15, 16–20, 21–25, 26–30) — April had ~103 tickets total

**Known gap:** December 2025 — the full-month query returned 50 of 65 total tickets. The ~15 missing Dec 2025 tickets were not recovered (subsequent re-query was not performed). All other months are fully covered.

### JQL queries used

Base template:
```
project = WT AND status in (Open, Backlog) AND created >= "YYYY-MM-DD" AND created <= "YYYY-MM-DD" ORDER BY created ASC
```

Ranges queried (all in 2025–2026):

| Range | Notes |
|-------|-------|
| 2025-01-01 – 2025-01-31 | Full month |
| 2025-02-01 – 2025-02-28 | Full month |
| 2025-03-01 – 2025-03-31 | Full month |
| 2025-04-01 – 2025-04-15 | Split (high volume) |
| 2025-04-16 – 2025-04-30 | Split (high volume) |
| 2025-05-01 – 2025-05-31 | Full month |
| 2025-06-01 – 2025-06-15 | Split (high volume) |
| 2025-06-16 – 2025-06-30 | Split (high volume) |
| 2025-07-01 – 2025-07-15 | Split |
| 2025-07-16 – 2025-07-23 | Split |
| 2025-07-24 – 2025-07-31 | Split (46 tickets) |
| 2025-08-01 – 2025-08-31 | Full month |
| 2025-09-01 – 2025-09-15 | Split (high volume) |
| 2025-09-16 – 2025-09-30 | Split (high volume) |
| 2025-10-01 – 2025-10-31 | Full month |
| 2025-11-01 – 2025-11-15 | Split |
| 2025-11-16 – 2025-11-30 | Split |
| 2025-12-01 – 2025-12-31 | Full month — **INCOMPLETE** (50/65 returned) |
| 2026-01-01 – 2026-01-15 | Split |
| 2026-01-16 – 2026-01-31 | Split |
| 2026-02-01 – 2026-02-14 | Split |
| 2026-02-15 – 2026-02-28 | Split |
| 2026-03-01 – 2026-03-15 | Split (48 tickets) |
| 2026-03-16 – 2026-03-31 | Split |
| 2026-04-01 – 2026-04-10 | Split (very high volume month) |
| 2026-04-11 – 2026-04-15 | 5-day split |
| 2026-04-16 – 2026-04-20 | 5-day split (35 tickets) |
| 2026-04-21 – 2026-04-25 | 5-day split (35 tickets) |
| 2026-04-26 – 2026-04-30 | 5-day split |
| 2026-05-01 – 2026-05-06 | Partial month (up to analysis date) |

**Total tickets collected:** 987 unique tickets  
**DisAgg-related:** 350 tickets (~35%)

---

## DisAgg Label Taxonomy

Labels observed on DisAgg-related tickets, with approximate counts:

### Primary classification labels
| Label | Count | Meaning |
|-------|-------|---------|
| `lc_bulk_04_29_26` | 228 | Bulk-applied label on 2026-04-29 to tag tickets relevant to the DisAgg initiative (many pre-existing tickets retroactively labeled) |
| `Disag_Storage` | 136 | Core DisAgg storage feature label |
| `Disag_Must_Have` | 19 | Required for milestone/release |
| `Disag_Private_Preview` | 15 | Target milestone: Private Preview |
| `Disag_Internal` | 10 | Internal/team-only scope |
| `disaggregated-storage` | 7 | Alternative spelling of the feature label |
| `Disag_Customer` | 7 | Customer-facing requirement |
| `Disag_Public_Preview` | 2 | Target milestone: Public Preview |
| `Disag_Post_GA` | 1 | Post-general-availability work |
| `Disag_Launch` | 1 | Launch milestone |
| `Disag_M12` | 1 | Milestone 12 |

### Engineering area labels
| Label | Count | Meaning |
|-------|-------|---------|
| `WT_disagg_eng` | 9 | Engineering implementation work |
| `WT_disagg_TBD` | 8 | Ownership/scope TBD |
| `WT_disagg_testing` | 3 | Testing-specific work |
| `WT_disagg_planning` | 2 | Planning/design |
| `WT_disagg_design` | 1 | Design work |
| `WT_disagg_project` | 4 | Project-level tracking |

### Functional grouping labels
| Label | Meaning |
|-------|---------|
| `Disag_grouping_Performance` | Performance-related |
| `Disag_grouping_Functional_Parity` | Feature parity with local storage |
| `Disag_grouping_Operational_Readiness` | Ops/reliability |
| `Disag_grouping_Load_Resilience` | Resilience under load |
| `Disag_grouping_Durability` | Data durability |
| `Disag_grouping_Security` | Security |
| `Disag_grouping_Dev_Experience` | Developer experience |
| `Disag_grouping_Backup_Restore` | Backup & restore |
| `Disag_grouping_Hygiene` | Code hygiene |
| `Disag_grouping_Multi-tenancy_Protection` | Multi-tenancy |

### Performance labels
| Label | Count | Meaning |
|-------|-------|---------|
| `disagg-performance` | 3 | General performance |
| `disagg-performance-investigation` | 2 | Performance investigation |
| `disag_perf_in_cache_100_update` | 2 | Specific perf test scenario |
| `disag_perf_128_thread_load` | 1 | Specific perf test scenario |

### Other relevant labels appearing on DisAgg tickets
- `expedite` — fast-tracked priority
- `layered-cursor` — layered cursor subsystem
- `dc` — unclear meaning (appears alongside disagg labels)
- `na-mdb` — not applicable to MongoDB
- `perf-improvement` — performance improvement

---

## Key Observations

### 1. Bulk retroactive labeling (lc_bulk_04_29_26)

On 2026-04-29, a bulk labeling operation applied `lc_bulk_04_29_26` to 228 tickets. This label
appears on tickets created as far back as February 2025. It seems to have been used to gather
all tickets relevant to a DisAgg sprint or work bundle. This means many pre-2026 tickets that
might look unrelated (no other disagg labels) are actually tracked as DisAgg work.

**Implication for gap analysis:** Tickets with `lc_bulk_04_29_26` but no specific testing labels
are candidates for missing test coverage.

### 2. Testing-labeled tickets are rare

Only 3 tickets carry `WT_disagg_testing` label — a very low count relative to 350 DisAgg tickets.
This strongly suggests that testing coverage gaps exist across the DisAgg feature.

The 3 testing-labeled tickets are:
- WT-14416 [ds-06.09] — Existing functional tests (Atlas and mongod) pass vs local storage
- WT-14429 [ds-12.01] — Automated development environments for disagg
- WT-14440 [ds-19.01] — Automatic recovery testing from process/HW/networking failures

### 3. DisAgg story tickets ([ds-XX.XX])

20 story tickets following the `[ds-XX.XX][Storage Engines (Core)]` naming convention were found.
These represent high-level requirements from the DisAgg design document. Most are in Open status.

Story tickets by functional area:
| Ticket | Code | Area |
|--------|------|------|
| WT-14408 | ds-04.02 | Pre-mortems on durability (data corruption) |
| WT-14454 | ds-04.03 | Pre-mortems on availability |
| WT-14413 | ds-05.08 | Restore with RTO < 15 mins |
| WT-14415 | ds-06.08 | Change stream support |
| WT-14416 | ds-06.09 | Existing functional tests pass vs local storage |
| WT-14906 | ds-06.05 | Multi-document transactions |
| WT-14420 | ds-07.04 | MongoD stores intermediate key for decrypting |
| WT-14423 | ds-08.06 | Local development environment for mongod/wt |
| WT-14427 | ds-09.04 | 100% hygiene plan execution |
| WT-14664 | ds-09.05 | Design Review + Document for Layered Tables |
| WT-14429 | ds-12.01 | Automated development environments |
| WT-14432 | ds-14.03 | Performance benchmarking of hardware/pod specs |
| WT-14433 | ds-14.04 | Read performance matching NVME via local cache |
| WT-14434 | ds-14.05 | Achieve performance parity with latest MongoD |
| WT-14435 | ds-14.06 | Automated Performance Regression Tests |
| WT-14436 | ds-14.07 | High Value Workload performance testing |
| WT-14440 | ds-19.01 | Automatic recovery testing |
| WT-14441 | ds-21.01 | Complete Durability threat model of SLS with mongod |
| WT-14442 | ds-28.02 | Mongod admission control using SLS metrics |
| WT-14463 | ds-19.06 | Complete Availability threat model of SLS with mongod |

### 4. Critical/High-priority DisAgg bugs (open)

| Ticket | Summary | Priority |
|--------|---------|----------|
| WT-17247 | Layered cursor writes on follower do not check stable cell's full time window | Critical - P2 |
| WT-17278 | Follower remove returns WT_NOTFOUND where leader returns WT_ROLLBACK | Major - P3 |
| WT-17160 | Increasing situations in test_layered91.py results in abort due to cache stuck | Major - P3 |
| WT-17311 | Modify that sees outdated tombstone returns WT_NOTFOUND instead of WT_ROLLBACK | Major - P3 |

### 5. Validation and verification gaps

Several tickets indicate active work on validation infrastructure that implies current test gaps:
- WT-17250 — Add validation test for shared disk cache
- WT-17189 — During GC, verify most recent update against stable table (debug build)
- WT-17190 — During GC, verify older updates against history store
- WT-17192 — During GC, verify most recent update against stable table (release build)
- WT-17146 — Add shared metadata consistency check to verify
- WT-17188 — Extend btree ID uniqueness verification to shared (disagg) metadata
- WT-16720 (Epic) — Validation improvements in disagg (Jasmine Bi)
- WT-15476 (Epic) — Validate layered table content during garbage collection

### 6. Follower-mode test coverage appears thin

Multiple open bugs specifically mention follower-mode correctness issues that were discovered
without dedicated tests:
- WT-17247 — Follower write path missing stable cell time window check
- WT-17278 — Follower remove vs leader behavior divergence
- WT-17131 — Follower layered cursors should not reopen unchanged stable table at checkpoint

### 7. Checkpoint and step-up/step-down coverage

Several tickets reveal testing gaps around the checkpoint pickup and node role transitions:
- WT-17352 (Epic) — Checkpoint Pickup Performance
- WT-17090 — Reconcile checkpoint pick-up with metadata operations on follower
- WT-17091 — Investigate and implement step-down for publish
- WT-17309 — Support step-up without resetting all cursors
- WT-17319 — Provide more information when failing to pickup a checkpoint

### 8. April 2026 spike — lc_bulk bulk-label sprint

April 2026 was unusually high-volume (~103 tickets in one month), correlating with the
`lc_bulk_04_29_26` bulk labeling event. Many of these tickets appear to represent a
structured planning effort to identify and track all DisAgg work items.

---

## Potential Testing Gaps Identified

Based on ticket analysis, areas that appear to lack explicit test coverage:

1. **Follower-mode correctness** — time window checks, NOTFOUND vs ROLLBACK behavior, cursor sweep
2. **Garbage collection verification** — updates vs stable table, updates vs history store
3. **Checkpoint pickup** — failure scenarios, metadata consistency, follower mode pickup
4. **Step-up / step-down transitions** — especially with prepare, fast truncate, and cursor state
5. **Shared disk cache validation** — no explicit validation tests (WT-17250 is new work)
6. **Shared metadata consistency** — btree ID uniqueness, metadata verification
7. **Fast truncate in disagg** — prepared fast truncate, follower mode truncate
8. **Layered cursor operations** — readonly config, sweep behavior, file descriptor exhaustion
9. **Multi-node validation** — data mismatch detection across leader/follower
10. **Performance regression tests** — automated performance testing for DisAgg (ds-14.06 still open)

---

## Classification Review (v2 — after disagg_components.md)

After adding `disagg_components.md` (page deltas, shared storage, layered tables, disagg block manager,
PALI, cache management / materialization frontier, layered cursors, shared metadata, shared history store,
prepared transactions, version cursors, encryption/KEK, diagnosability, testing, observability), the
ticket classification was revised. The original classification relied only on known DisAgg labels and
`disagg`/`layered`/`[ds-XX.XX]` in the summary.

### False positives removed (4 tickets)

These were in DisAgg v1 solely because of the `dc` label. The `dc` label (always co-occurring with
`na-mdb` on these tickets) is not a reliable disagg indicator — it appears on generic build failures.

| Ticket | Summary | Why removed |
|--------|---------|-------------|
| WT-14214 | ASan: Out of bounds access in __wt_cell_type_raw | Only `dc` label; generic memory safety bug |
| WT-14331 | Fast truncate information written to disk | Only `dc` label; generic build failure |
| WT-16058 | failed: format-stress-test-2 on ubuntu2004-stress-tests-arm64 | Only `dc` label; generic stress test |
| WT-16270 | mirror error on rhel8-zseries | Only `dc` label; generic platform failure |

### False negatives added (43 tickets)

Tickets previously in "Other" that cover core DisAgg components:

**PALI / PALite** (DisAgg block manager / page storage interface):
- WT-14950 — Update PALI doc post-discard verify routine implementation
- WT-15266 — Dump all pages from the pali response in the results array on checksum failure
- WT-15419 — Log error messages when PALI API call fail
- WT-16134 — Enable test/format to run using PALI instead of PALite
- WT-16159 — Enable multi-process DB access in PALite
- WT-16668 — Determine cause of PALite indirect leak LSan failure
- WT-16806 — Enable Windows build for PALite

**Page deltas** (core DisAgg storage mechanism):
- WT-15026 — Optimize re-use of old disk images to avoid full page for page deltas
- WT-15027 — Add heuristic to consider building a delta if a percentage of rows are modified
- WT-15194 — Use the same macro to unpack full page images and page deltas
- WT-15709 — Support generating page deltas for page splits
- WT-16224 — Unpack the internal page deltas and base page progressively during merging
- WT-16239 — Write a full page instead of delta if we have a lot of deletes on the page
- WT-16442 — Write Performance Reconciliation Efficiency - Delta Generation for re-split pages

**Precise checkpoints** (DisAgg checkpoint sharing component):
- WT-15009 — task-timed-out: precise-checkpoint-stress-test (build failure)
- WT-15397 — Temporarily disable truncate if precise checkpoint and preserve prepared are enabled
- WT-17317 — failed: precise-checkpoint-stress-test-tiered (build failure)

**Step-up / step-down** (distributed systems node role transitions):
- WT-15808 — Support readers when performing step-up
- WT-15860 — Investigate how to manage internal threads during step up/down
- WT-17309 — Support step-up without resetting all the cursors

**Leader / follower mode** (distributed systems primary/standby):
- WT-16813 — (Follower mode) Implement GC checkpoint pick-up with fast truncate design
- WT-16837 — Investigate whether the stat log server should process ingest tables on leader
- WT-16851 — Eliminate the need to create missing ingest btrees when loading a new checkpoint
- WT-16877 — Make __wt_layered_table_manager.leader to be wt_shared
- WT-17049 — Avoid reopening the stable table for each operation on leader
- WT-17089 — Implement the publish functionality for the followers
- WT-17090 — Reconcile checkpoint pick-up with metadata operations on the follower
- WT-17135 — (Follower mode) Enable fast truncate on develop
- WT-17192 — During GC, verify the most recent update against the stable table in release build
- WT-17349 — Support reading individual pages in follower mode without checkpoint pickup

**Shared metadata / shared disk** (disagg metadata component):
- WT-16477 — Read shared metadata directly when opening dhandle on shared table on standby
- WT-17066 — Investigate and define shared disk hash table bucket size
- WT-17250 — Add validation test for shared disk cache
- WT-17344 — Add wt util subcommand to dump the turtle page
- WT-17348 — Generalise verify read_corrupt config to all modes in wt util

**PAGE_LOG** (disagg block manager / page storage):
- WT-16525 — Remove WT_PAGE_LOG_LSN_MAX
- WT-16535 — Ensure WT_PAGE_LOG_ENCRYPTED is default set for regular tables
- WT-17341 — Add wt util subcommand to read a single page through WT_PAGE_LOG

**Checkpoint pickup / checkpoint coordination** (disagg-specific concepts):
- WT-16188 — Ensure that checkpoint pick up scales to millions of tables
- WT-16544 — Investigate slow checkpoint pick-up behaviour
- WT-17093 — Redefine the rules of checkpoint order for fake checkpoint
- WT-17296 — Merge cross checkpoint caching feature branch

**Version cursors** (DisAgg component):
- WT-16136 — Version cursor: determine if stop durable timestamp is from tombstone or previous value
- WT-16148 — Investigate why version cursor cannot access the HS entry

**Stable schema epoch / turtle file** (DisAgg metadata component):
- WT-15057 — Ensure that the turtle file is updated atomically with the metadata file during checkpoint
- WT-17327 — Document the stable schema epoch

**Standby lag** (distributed systems — follower node):
- WT-17307 — Creating large numbers of tables causes standby lag

### Net result

| Version | DisAgg | Other |
|---------|--------|-------|
| v1 (original) | 350 | 637 |
| v2 (revised) | 389 | 598 |
| Change | +39 | -39 |

### Classification decisions deferred

- **WT-16697** "Investigation of spurious errors related to the block manager" — the disagg_components.md
  calls out a "Disagg block manager", but this ticket may refer to the generic WT block manager.
  Left in Other pending more context.

---

## Output Files

- `disagg-analysis/wt-tickets-open-backlog-2025-2026.md` — Full deduplicated ticket list (987 tickets)
- `disagg-analysis/notes.md` — This file
