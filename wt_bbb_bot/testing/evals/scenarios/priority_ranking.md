Rank these three WiredTiger BF tickets by priority. No Jira or Evergreen access needed — all evidence is below.

---

## BF-SCENARIO-A
- **Summary:** test_checkpoint_snapshot SIGABRT — assertion in txn_timestamp.c
- **Status:** Open | **Assignee:** Unassigned
- **Blast radius:** 8 variants, all on trunk
- **Failure rate:** 25% over 7 days (12/47 runs)
- **Age:** 4 days open
- **Release impact:** Blocking mongodb-mongo-master
- **Failure type:** Crash / assertion

## BF-SCENARIO-B
- **Summary:** test_rollback_to_stable intermittent failure on SUSE 12
- **Status:** Open | **Assignee:** Assigned (engineer X)
- **Blast radius:** 1 variant (suse12 only)
- **Failure rate:** 3% over 7 days (2/67 runs)
- **Age:** 12 days open
- **Release impact:** Not blocking any release branch
- **Failure type:** Flaky / intermittent

## BF-SCENARIO-C
- **Summary:** test_disagg_checkpoint hang on all disagg variants — no output after page flush
- **Status:** Open | **Assignee:** Unassigned
- **Blast radius:** 3 variants (all disagg), disagg path only
- **Failure rate:** 70% over 7 days (7/10 runs)
- **Age:** 2 days open
- **Release impact:** Blocking disagg storage integration milestone
- **Failure type:** Hang / timeout
