# wt3874_pad_byte_collator — Pad-byte-aware collator insert/remove correctness

**Path:** `test/csuite/wt3874_pad_byte_collator/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-3874
**Components under test:** Custom collator, `cursor->insert`, `cursor->remove`, key comparison with padding bytes

## What This Test Does
This test reproduces WT-3874, where an assertion fired when removing a record if the stored key was compared against the caller-supplied key without accounting for the custom collator. The test registers a collator (`my_coll`) that compares only the first byte of each key and treats all remaining bytes as padding. It inserts a 20-byte key with first byte `'a'` and padding `'X'`, takes a checkpoint, then removes using a key with the same first byte but different padding `'Y'`. The bug would trigger an assertion when WiredTiger compared the stored key against the given key using a full byte-for-byte comparison rather than the collator.

## Test Scenarios / Cases

### Scenario: Remove with different padding under custom first-byte collator
- **What it tests:** That `cursor->remove` correctly locates and removes a record when the supplied key has different padding bytes than the stored key, as long as the collator considers them equal (same first byte).
- **Components:** `conn->add_collator`, custom `WT_COLLATOR` (`my_compare` using only `strncmp(..., 1)`), `cursor->insert`, `cursor->remove`, `session->checkpoint`.
- **Notes:** KEY_SIZE=20. Insert uses padding `'X'`; remove uses padding `'Y'`. Both have first byte `'a'`. The fix ensures collator-aware key comparison is used on the remove path.

## LazyFS Variant
None.
