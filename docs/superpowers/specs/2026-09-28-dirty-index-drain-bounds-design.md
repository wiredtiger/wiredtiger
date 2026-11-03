# Dirty-index drain bounds design

## Goal

Bound per-btree dirty-index drain work and ensure the adaptive scheduler recognizes both ordinary
and urgent eviction submissions as productive. Strengthen split-retirement coverage to verify that
the drain actually ran during the split workload.

## Design

The drain will cap slot examinations per visit at four times the number of remaining per-tree queue
slots. The occupancy high-water statistic will continue to use the uncapped ring occupancy. The
existing ordinary queue count will continue to control how many queue slots the drain consumes and
whether the tree walk can be skipped.

The drain will separately report productive submissions. A successfully queued ordinary candidate
or urgent candidate counts as productive for resetting the consecutive-empty counter and
re-enabling a parked drain; filtered, stale, or hazard-blocked slots do not. This keeps queue-budget
accounting independent from scheduling feedback.

The split-retirement test will assert that drain scanning was observed during the workload that
creates in-memory splits and will wait for this asynchronous statistic with a deadline rather than
assuming the drain ran synchronously.

## Alternatives considered

- Scan at most the remaining queue slots: lowest work bound, but risks stopping before finding enough
  candidates among stale or filtered entries.
- Scan without a separate bound: preserves candidate discovery but allows a full ring scan in one
  visit.
- Scan up to four times the remaining queue slots: selected compromise between bounded worker time
  and finding candidates beyond stale or filtered entries.

## Validation

Run the focused dirty-index tests and WiredTiger style validation. Confirm the worktree diff remains
limited to the drain behavior and its directly related test.
