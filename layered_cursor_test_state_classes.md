
# Layered Cursor Test State Classes

This document describes all independent state dimensions that must be exercised to achieve
comprehensive coverage of any layered cursor API function. Not every dimension applies to
every function; the applicable set is noted per dimension.

---

## Overview

A layered cursor is a merge view over two BTrees: **ingest** (mutable, follower-writable) and
**stable** (read-only checkpoint on follower, read-write on leader). The cursor struct
(`__wt_cursor_layered`) carries per-call state on top of those two constituent cursors.
Grey-box tests must verify both the user-visible contract (return code, key/value contents) and
the internal structural invariants (which constituent `current_cursor` points to, which flags are
set, what the constituent cursors' own key/value flags are).

---

## Dimension 1: Node Role

The `leader` field (`WT_LAYERED_TABLE_MANAGER.leader`, cached per cursor) changes the control
flow in essentially every operation.

| State ID | Role | Notes |
|----------|------|-------|
| R-1 | Follower | Primary focus. Reads ingest first, then stable. Writes go to ingest. |
| R-2 | Leader | Ingest is skipped for reads (`FIXME-WT-16810`). Writes go to stable. |

**Interaction with stable cursor**: On leader, stable cursor is always open and writable
(asserted at `cur_layered.c:155`). On follower, stable may be NULL (no checkpoint yet).

---

## Dimension 2: Constituent Cursor Availability

Which constituent cursors are open and usable at the time the operation runs.

| State ID | Ingest | Stable | When it occurs |
|----------|--------|--------|----------------|
| CA-1 | Open | Open | Normal follower with a checkpoint |
| CA-2 | Open | NULL | Follower, no checkpoint yet; write operations only need ingest |
| CA-3 | NULL | Open | Should not happen in correct code; included for negative testing |
| CA-4 | Open | Open (read-only BTree flag) | Follower with checkpointed stable |

The `WT_CLAYERED_READ_STABLE` flag controls whether stable is opened when missing. Search
operations set this flag; write-only operations may not.

---

## Dimension 3: Table Content — Key Distribution

The most important dimension for grey-box testing. The ingest table and stable table can hold
overlapping, disjoint, or complementary sets of keys. Enumerate all logically distinct
combinations for a *single target key* (for search/lookup functions) or *full key range* (for
iteration functions).

### 3a. Single-key content (for search, update, remove, reserve)

| State ID | Key in Ingest | Key in Stable | Expected outcome |
|----------|--------------|--------------|-----------------|
| K-1 | Absent | Absent | WT_NOTFOUND; both constituents unpositioned |
| K-2 | Live value | Absent | Found; `current_cursor == ingest` |
| K-3 | Tombstone (deleted) | Absent | WT_NOTFOUND; tombstone hides the key |
| K-4 | Absent | Live value | Found; `current_cursor == stable` |
| K-5 | Live value | Live value (same) | Found; `current_cursor == ingest` (ingest wins) |
| K-6 | Live value | Live value (different) | Found; ingest value returned, `current_cursor == ingest` |
| K-7 | Tombstone | Live value | WT_NOTFOUND; ingest tombstone shadows stable live value |
| K-8 | Absent | Key in truncate list | WT_NOTFOUND; truncate entry hides stable key |
| K-9 | Tombstone | Key in truncate list | WT_NOTFOUND; double deletion (both ingest and truncate list) |

State K-7 is the most surprising and important: a deleted key in ingest must shadow a live entry
in stable. This is the primary correctness invariant of the layered architecture.

### 3b. Full-range content (for next, prev, search_near, largest_key)

| State ID | Ingest keys | Stable keys | Notes |
|----------|-------------|------------|-------|
| R-3 | Empty | Empty | WT_NOTFOUND on first next/prev |
| R-4 | {A, B, C} | Empty | Only ingest traversed |
| R-5 | Empty | {X, Y, Z} | Only stable traversed |
| R-6 | {A, B, C} | {X, Y, Z} (disjoint, ingest < stable) | Ingest exhausts first, then stable |
| R-7 | {X, Y, Z} | {A, B, C} (disjoint, stable < ingest) | Stable exhausts first, then ingest |
| R-8 | {A, C, E} | {B, D, F} (interleaved, no overlap) | Merge alternates between constituents |
| R-9 | {A, B, C} | {A, B, C} (full overlap, same values) | Ingest wins at each position; stable advanced past equal keys |
| R-10 | {A, ∅B, C} | {B, D} (partial overlap, B tombstoned in ingest) | B skipped during forward scan |
| R-11 | {∅A, ∅B} | {A, B, C} (ingest has only tombstones) | A and B skipped; only C visible |

Where `∅X` denotes key X stored with a tombstone value.

---

## Dimension 4: Table Content — Size Classes

These control which code paths in the merge logic are exercised and expose boundary conditions.

| State ID | Ingest size | Stable size | Purpose |
|----------|-------------|------------|---------|
| S-1 | 0 | 0 | Fully empty |
| S-2 | 1 | 0 | Single element in ingest only |
| S-3 | 0 | 1 | Single element in stable only |
| S-4 | 1 | 1 | Exactly one element each |
| S-5 | 2 | 2 | Smallest non-trivial merge |
| S-6 | 5 | 5 | Small tables with a few elements |
| S-7 | 100+ | 100+ | Large tables; confirms no off-by-one in merge loop |
| S-8 | 1 | 100+ | Asymmetric sizes (small ingest, large stable) |
| S-9 | 100+ | 1 | Asymmetric sizes (large ingest, small stable) |

---

## Dimension 5: Cursor Position State (Before the API Call)

The state of `current_cursor`, `WT_CURSTD_KEY_INT`, `WT_CURSTD_KEY_EXT`, and the constituent
cursor positions prior to the call being tested.

| State ID | Position state | Flags on `iface` | `current_cursor` |
|----------|---------------|-----------------|-----------------|
| P-1 | Not positioned (fresh cursor) | None | NULL |
| P-2 | Positioned via `search` | KEY_INT + VALUE_INT | ingest or stable |
| P-3 | Positioned via `next` in ingest | KEY_INT + VALUE_INT + ITERATE_NEXT | ingest |
| P-4 | Positioned via `next` in stable | KEY_INT + VALUE_INT + ITERATE_NEXT | stable |
| P-5 | Positioned via `prev` in ingest | KEY_INT + VALUE_INT + ITERATE_PREV | ingest |
| P-6 | Positioned via `prev` in stable | KEY_INT + VALUE_INT + ITERATE_PREV | stable |
| P-7 | KEY_EXT (after `cursor_copy_release` path or copy-out) | KEY_EXT | (any) |
| P-8 | After `reset` | None | NULL |
| P-9 | One constituent exhausted (ingest done, stable still has keys) | KEY_INT + VALUE_INT | stable |
| P-10 | One constituent exhausted (stable done, ingest still has keys) | KEY_INT + VALUE_INT | ingest |
| P-11 | At the first element (beginning) | KEY_INT + ITERATE_NEXT | ingest or stable |
| P-12 | At the last element (end of forward scan) | KEY_INT + ITERATE_NEXT | ingest or stable |

---

## Dimension 6: Iteration Direction and Direction Change

The `WT_CLAYERED_ITERATE_NEXT` and `WT_CLAYERED_ITERATE_PREV` flags control whether the
alternate cursor is re-positioned on the next call.

| State ID | Previous direction | Requested direction | Notes |
|----------|--------------------|-------------------|-------|
| D-1 | None (fresh) | NEXT | First next() call |
| D-2 | None (fresh) | PREV | First prev() call |
| D-3 | NEXT | NEXT | Continuing forward |
| D-4 | PREV | PREV | Continuing backward |
| D-5 | NEXT | PREV | Direction reversal; triggers `__clayered_position_alternate` |
| D-6 | PREV | NEXT | Direction reversal; triggers `__clayered_position_alternate` |
| D-7 | NEXT | (search) | Breaking iteration with a point lookup; ITERATE flags cleared |

---

## Dimension 7: Transaction Context

The transaction isolation and read timestamp affect which versions of values are visible.
For layered cursors, the snapshot generation and read timestamp are also cached and used to
detect stale state.

| State ID | Transaction type | Notes |
|----------|-----------------|-------|
| T-1 | No explicit transaction (implicit, auto-commit) | Default for most operations |
| T-2 | Explicit `begin_transaction` + `commit_transaction` | All ops in one txn |
| T-3 | Explicit transaction with `read_timestamp=T` | Follower stable cursor view pinned at T |
| T-4 | Read timestamp changes between operations on same cursor | Triggers cursor state re-evaluation |
| T-5 | Snapshot generation changes between operations | Triggers stable cursor reopen on follower |
| T-6 | `isolation=read-committed` | Triggers `__wt_txn_read_committed_should_release_snapshot` path and constituent reset |
| T-7 | Multiple operations in one transaction (batch) | Verifies that intermediate state is consistent |

---

## Dimension 8: Tombstone / Deletion Encoding

The layered design encodes deletions as a special tombstone value in the ingest table. The
`__clayered_deleted_encode` / `__clayered_deleted_decode` functions handle values that
collide with the tombstone prefix.

| State ID | Value type | Notes |
|----------|-----------|-------|
| E-1 | Normal value (no tombstone prefix) | Happy path; no encoding needed |
| E-2 | Value that starts with tombstone bytes | Must be encoded with extra byte suffix |
| E-3 | Tombstone-encoded value (deletion marker) | Signals deleted entry; hidden from readers |
| E-4 | Empty value (`size == 0`) | Not a tombstone; must not be confused with deletion |

State E-2 is the edge case: a user value that happens to start with `\0\0` (the tombstone
prefix) must be stored with an extra byte appended so it is not misread as a tombstone.

---

## Dimension 9: Truncate List Visibility

The layered table maintains a truncate queue (`WT_LAYERED_TABLE.truncateqh`) for range deletions.
Cursor lookup checks this list lock-free via `WT_TRUNCATE.committed` before returning stable
results.

| State ID | Truncate list state | Notes |
|----------|-------------------|-------|
| TL-1 | Empty list | No range deletions; baseline |
| TL-2 | Committed entry covering searched key | Key in stable deleted by range truncate |
| TL-3 | Uncommitted entry covering searched key | Not yet visible; key should still be found |
| TL-4 | Committed entry not covering searched key | No effect on lookup |
| TL-5 | Multiple overlapping committed entries | Loop in `__clayered_reposition_truncate_iterate` |
| TL-6 | Truncate entry covering the stable cursor's current position during iteration | Triggers reposition |

---

## Dimension 10: Stable Cursor Checkpoint Advancement (Follower)

On follower, the stable cursor opens a specific named checkpoint. The `checkpoint_meta_lsn`
field tracks whether the stable cursor needs to advance to a newer checkpoint.

| State ID | Checkpoint state | Notes |
|----------|-----------------|-------|
| CK-1 | No checkpoint exists (stable cursor NULL) | Write-only operations still work via ingest |
| CK-2 | Checkpoint exists and cursor is current | No reopen needed |
| CK-3 | New checkpoint available (`checkpoint_meta_lsn` changed) | `__clayered_reopen_stable` called |
| CK-4 | Stable cursor positioned; checkpoint advances | Position preserved via `__wt_cursor_dup_position` |
| CK-5 | Stable cursor positioned at key that disappears in new checkpoint | Falls back to ingest; ITERATE flags cleared |

---

## Dimension 11: Cursor Bounds

The cursor bounds API restricts iteration to a subrange. Bounds are copied to constituent
cursors via `__clayered_copy_bounds`.

| State ID | Bounds state | Notes |
|----------|-------------|-------|
| B-1 | No bounds | Full table scan |
| B-2 | Lower bound only | Limits start of forward scan |
| B-3 | Upper bound only | Limits end of forward scan |
| B-4 | Both bounds | Subrange scan |
| B-5 | Bounds set, then cursor reopened (role change or checkpoint advance) | `__clayered_copy_bounds` must reapply bounds to new constituent |

---

## Dimension 12: Random Cursor Mode

When `WT_CLAYERED_RANDOM` is set, `next()` is replaced by `next_random`, and standard
iteration semantics do not apply.

| State ID | Random mode | Notes |
|----------|-------------|-------|
| RA-1 | Normal (non-random) cursor | Standard next/prev iteration |
| RA-2 | Random cursor (configured at open) | Uses `__clayered_next_random`; cannot be reopened during iteration |

---

## Dimension 13: `current_cursor` Assignment After Operation

For operations that leave the cursor positioned, verify which constituent `current_cursor`
points to and what key/value flags are set on both the layered cursor and the constituent.

| State ID | After operation | `current_cursor` | Flags on `iface` |
|----------|----------------|-----------------|-----------------|
| CC-1 | After `search` (found in ingest) | `ingest_cursor` | KEY_INT + VALUE_INT |
| CC-2 | After `search` (found in stable) | `stable_cursor` | KEY_INT + VALUE_INT |
| CC-3 | After `search` (not found) | NULL | None |
| CC-4 | After `insert` (success) | NULL | None (insert clears position) |
| CC-5 | After `next` (ingest wins merge) | `ingest_cursor` | KEY_INT + VALUE_INT |
| CC-6 | After `next` (stable wins merge) | `stable_cursor` | KEY_INT + VALUE_INT |
| CC-7 | After `reset` | NULL | None |

---

## Dimension 14: Write Operation Specifics (Insert/Update/Remove)

For write operations, additional state dimensions apply.

| State ID | Write condition | Notes |
|----------|----------------|-------|
| W-1 | `WT_CURSTD_OVERWRITE` set (default) | Skip duplicate check |
| W-2 | `WT_CURSTD_OVERWRITE` unset | Check for existing key before write |
| W-3 | Insert into ingest (follower) when key exists only in stable | Overwrite without OVERWRITE flag → WT_DUPLICATE_KEY |
| W-4 | Update a key that has a tombstone in ingest | Resurrects the key |
| W-5 | Remove a key present only in stable | Writes tombstone to ingest |
| W-6 | Remove a key already tombstoned in ingest | No-op or double tombstone |

---

## Priority Matrix

Priority ordering for implementing tests (highest first):

| Priority | Dimensions | Rationale |
|----------|-----------|-----------|
| P0 | Role (R-1 follower), Single-key content (K-1..K-7), CA-1..CA-2 | Core contract: ingest-wins and tombstone-shadowing |
| P1 | Size classes (S-1..S-9), Full-range content (R-3..R-11) | Iteration completeness and boundary conditions |
| P2 | Position state (P-1..P-12), Direction (D-1..D-7) | Merge position tracking correctness |
| P3 | Transaction context (T-1..T-5), `current_cursor` assignment (CC-1..CC-7) | Per-call state invariants |
| P4 | Tombstone encoding (E-1..E-4), Truncate list (TL-1..TL-6) | Edge cases in deletion handling |
| P5 | Checkpoint advancement (CK-1..CK-5), Cursor bounds (B-1..B-5) | Follower-specific reopen and range scan |
| P6 | Leader role (R-2), Write specifics (W-1..W-6), Random mode (RA-1..RA-2) | Completion; some covered by Python tests |

---

## Comparison with Previous Analysis (`layered_cursor_states.md`)

The `layered_cursor_states.md` document inventories *which fields exist* and *where they are
set/read*. This document maps those fields to *test-observable state classes*. The gaps filled here:

1. **Tombstone-shadows-stable (K-7)**: Not explicitly called out as a test state in the
   inventory; critical correctness property.
2. **Truncate list visibility (TL-*)**: Mentioned in the inventory under `truncateqh`; mapped
   here to concrete test cases.
3. **Checkpoint advancement with a positioned cursor (CK-4, CK-5)**: The inventory describes
   `__clayered_reopen_stable` logic; this document converts it to testable scenarios.
4. **Direction reversal (D-5, D-6)**: Implicitly covered by the `__clayered_position_alternate`
   description; now explicit test states.
5. **Tombstone value encoding (E-2)**: Mentioned as `__clayered_deleted_encode` but not mapped
   to test states.
6. **`current_cursor` invariants (CC-*)**: The inventory tracks the field; this document defines
   what must be true about it after each operation.
7. **Size-based boundary conditions (S-*)**: Not in the inventory (structural); added here as
   important test state class.
