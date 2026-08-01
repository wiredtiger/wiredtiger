# Dirty-Index Ref Lifetime Repair

## Problem

The dirty-index ring relies on a page back-pointer to identify the ring slot that owns a `WT_REF`.
During old-ref retirement, `__wt_dirty_index_block_page` can find that the named slot already holds
a replacement ref. It correctly leaves that slot populated, but incorrectly resets the shared page
back-pointer to `WTI_DIRTY_BP_NONE`. The populated slot is then orphaned. Later retirement of the
replacement ref cannot find and clear it, allowing the ring drain to read the ref after split-
generation reclamation frees it.

This matches the ASAN failure: the stale ref was allocated by `__split_insert`, reclaimed from a
`WT_GEN_SPLIT` stash, and later dereferenced by `__wt_hazard_set` in the drain.

## Invariant

Every non-NULL ring slot must be named by its page's `dirty_index_slot` back-pointer until the drain
or teardown clears that slot. Split-generation protection then keeps a ref loaded by an active drain
alive long enough for hazard-pointer acquisition.

## Design

When retirement blocks a page and discovers that the slot contains a replacement ref rather than
the retiring ref, restore the page back-pointer from `WTI_DIRTY_BP_BLOCKED` to the original encoded
slot value. Do not reset it to `WTI_DIRTY_BP_NONE`. This preserves ownership of the replacement slot
and lets its eventual teardown clear the slot before reclaiming the replacement ref.

Do not add a slot-claim sentinel, reader counter, refcount, or new `WT_REF`/`WT_PAGE` member. Those
approaches add a second lifetime protocol and are unnecessary once the existing back-pointer
invariant is maintained.

## Tests

Replace the unit test that currently accepts a populated replacement slot with a `NONE` back-pointer.
The corrected test will prove:

1. Old-ref retirement leaves the replacement slot populated and restores its encoded back-pointer.
2. Subsequent retirement of the replacement ref clears that slot and blocks further insertion.
3. Existing publication, split cleanup, duplicate suppression, and drain tests continue to pass.

Verification will include the dirty-index Catch2 tests, the focused Python eviction tests with and
without the disagg hook, `test_rollback_to_stable38`, an ASAN Evergreen rerun, and `dist/s_fast`.

## Scope

The change is limited to replacement-ref handling in `__wt_dirty_index_block_page` and its regression
coverage. The contradictory drain-order comment may be corrected if needed, but drain behavior will
not be redesigned as part of this fix.
