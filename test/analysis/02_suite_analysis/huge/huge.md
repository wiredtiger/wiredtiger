# huge — Very-large key and value insert/retrieve test (up to ~4 GB)

**Path:** `test/huge/`
**Language:** C
**Storage mode:** General
**Components under test:** cursor insert/update/search/remove, large key and value handling, row-store and column-store B-tree, `file:` and `table:` URIs, memory management for oversized items

## Overview

This test inserts a single very large key or value into WiredTiger (up to roughly 4 GB minus 1 MB in the full run, or up to 1 MB in the small mode), retrieves it, verifies the data matches byte-for-byte, and then removes it. The test matrix covers `file:` and `table:` URIs in both row-store and column-store configurations, with both a big key (row-store only) and a big value variant. Each combination opens a fresh database with a 10 GB cache.

## Test Scenarios / Cases

### Scenario: Large value in a row-store file (file:xxx, key_format=S)
- **What it tests:** Inserts a string value of size N bytes (all `'a'`s, null-terminated at position N-1) with a fixed string key `"key001"`, reads it back with `cursor->search` + `cursor->get_value`, asserts `memcmp` equality, then removes it.
- **Components:** Row-store B-tree, cursor update (used instead of insert to preserve the key), oversized value page handling
- **Notes:** `cursor->update` is used instead of `cursor->insert` because insert would discard the key for row-store. An explicit transaction is used so the cursor can be reset (unpinning the large page) before commit.

### Scenario: Large value in a column-store file (file:xxx, key_format=r)
- **What it tests:** Same large-value write/read/remove but with record-number key 1 in a variable-length column store.
- **Components:** Variable-length column-store B-tree, oversized value handling
- **Notes:** Same explicit transaction + cursor reset pattern to avoid self-eviction deadlock.

### Scenario: Large value in a row-store table (table:xxx)
- **What it tests:** Same as the file:xxx row-store scenario but via the table URI, which exercises the schema layer on top of the B-tree.
- **Components:** Schema/table layer, row-store B-tree, large value
- **Notes:** Identical logic to the file: variant.

### Scenario: Large value in a column-store table (table:xxx, key_format=r)
- **What it tests:** Column-store variant via table URI.
- **Components:** Schema/table layer, column-store B-tree, large value
- **Notes:** Identical logic.

### Scenario: Large key in a row-store file / table
- **What it tests:** In addition to large values, a large key (all `'a'`s with a null-terminator) is used as the lookup key. The key is set via `cursor->set_key(cursor, big)` and verified with `cursor->get_key`.
- **Components:** Large key storage, B-tree key comparison with oversized keys
- **Notes:** Column-store does not have variable-length keys (uses record numbers), so the large-key variant runs only for row-store URIs.

### Scenario: Size progression
- **What it tests:** The test runs the full matrix (4 URI/key-type combinations × big-key/big-value) for each entry in the `lengths[]` array: 20 B, 1 MB, 250 MB, 1 GB, 2 GB, 3 GB, and ~4 GB − 1 MB. The small (`-s`) mode stops at 1 MB.
- **Components:** Memory allocator, large-object eviction, 32-bit size field boundary (near 4 GB)
- **Notes:** Each size run creates a fresh database directory. The near-4 GB entry (`4 GB − 1 MB`) probes the practical maximum that WiredTiger can handle given 32-bit length fields.

## Coverage Notes

The huge test is the primary vehicle for exercising WiredTiger's handling of items that exceed normal page sizes. It uniquely covers the near-4 GB boundary for both key and value storage. The forced explicit transaction + cursor reset pattern is specifically required to avoid the eviction deadlock that would occur when a single auto-transaction pins a page that also needs to be evicted. Gaps: only a single key/value pair per run (no concurrent access, no multiple large items, no range scans); no checkpoint or recovery of large items; no modify or partial-value update of large items.
