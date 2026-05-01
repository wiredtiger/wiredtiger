# wt3184_dup_index_collator — Index search with custom collator on variable-length keys

**Path:** `test/csuite/wt3184_dup_index_collator/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-3184
**Components under test:** Index tables, custom collator, `cursor->search`, `cursor->search_near`, variable-length index keys, key unpacking

## What This Test Does
This test reproduces WT-3184, where a custom collator supplied to an index received a truncated key (due to a bug in the index key construction) causing key-unpack errors. The test creates ordered data sets of 5 elements (0–4), inserts elements 1 and 3, then performs `search` and `search_near` for all 5 positions (including missing ones) using both a custom collator and the default collator. It verifies that searches return correct results or `WT_NOTFOUND` as appropriate, even when index keys have variable length.

## Test Scenarios / Cases

### Scenario: Custom collator — search for present and absent keys
- **What it tests:** That `cursor->search` and `cursor->search_near` return correct results when using a custom integer collator on an index with variable-length keys, and that the collator is not given truncated keys that would cause unpack errors.
- **Components:** Custom `WT_COLLATOR`, index table, `cursor->search`, `cursor->search_near`.
- **Notes:** Elements 1 and 3 are inserted; searches for 0, 2, and 4 should yield `WT_NOTFOUND` or a near-miss result.

### Scenario: Default collator — same operations
- **What it tests:** Same search behavior using the built-in WiredTiger collator as a correctness baseline.
- **Components:** Index table, default collator, `cursor->search`, `cursor->search_near`.

## LazyFS Variant
None.
