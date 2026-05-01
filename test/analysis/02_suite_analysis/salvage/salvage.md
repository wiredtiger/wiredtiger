# salvage — B-tree page salvage correctness test for all overlap/LSN cases

**Path:** `test/salvage/`
**Language:** C
**Storage mode:** General
**Components under test:** `session->salvage`, `session->verify`, B-tree page recovery, LSN-based page selection, row-store and column-store variable-length page formats, checksum recomputation

## Overview

This test systematically constructs corrupt or overlapping B-tree page layouts directly at the binary file level, invokes `session->salvage` to recover the file, and then verifies that the salvaged output matches a pre-computed expected result. It covers 24 distinct corruption scenarios for both row-store (`WT_PAGE_ROW_LEAF`) and variable-length column-store (`WT_PAGE_COL_VAR`) page types, and is run with both unique and non-unique value sets.

## Test Scenarios / Cases

### Scenario: Empty file salvage (run 1)
- **What it tests:** Salvaging a completely empty file (no data pages appended). The expected result is an empty output.
- **Components:** Salvage on empty file, schema creation
- **Notes:** Baseline smoke test.

### Scenario: Sequential non-overlapping pages (runs 2–3)
- **What it tests:** Three pages with non-overlapping key ranges at sequential LSNs (run 2: ascending LSN order, run 3: descending LSN order). All pages should be retained intact.
- **Components:** Multi-page salvage, LSN ordering, page selection
- **Notes:** Establishes the baseline that good data is preserved.

### Scenario: Case #1 — fully overlapping pages, same start key, different LSNs (runs 4–6)
- **What it tests:** Three pages each starting at the same key, with sequential LSNs in all six permutations of ordering. Salvage must keep only the page with the highest LSN.
- **Components:** LSN-based conflict resolution, duplicate-page elimination
- **Notes:** Three permutations of LSN order cover all cases where any of the three pages is the newest.

### Scenario: Case #2 — second page overlaps beginning of first page (runs 7–8)
- **What it tests:** When the second page's key range partially overlaps the start of the first page, salvage must keep the non-overlapping prefix of the lower-LSN page and the full higher-LSN page.
- **Components:** Partial-page trimming, prefix/suffix key range splitting
- **Notes:** Two sub-cases: first page has higher LSN, second page has higher LSN.

### Scenario: Case #3 — second page overlaps end of first page (runs 9–10)
- **What it tests:** Second page overlaps the tail of the first page; salvage trims the higher-LSN page wins its range, lower-LSN page keeps its non-overlapping prefix.
- **Components:** Suffix key range splitting, LSN resolution
- **Notes:** Two LSN-order sub-cases.

### Scenario: Cases #4, #5, #6 — containment: second page is a prefix, middle, or suffix of first (runs 11–16)
- **What it tests:** When one page's range is entirely contained within another's range, the outer or inner page wins depending on LSN, with the outer page's non-contained ranges preserved.
- **Components:** Containment detection, three-way key range splitting
- **Notes:** Each case has two LSN-order sub-cases (6 runs total).

### Scenario: Cases #9, #10, #11 — reverse containment: first page is a prefix, suffix, or middle of second (runs 17–22)
- **What it tests:** Mirror of cases 4–6 with the containment direction reversed.
- **Components:** Symmetric containment handling
- **Notes:** 6 runs.

### Scenario: Column-store only — missing initial key range (run 23)
- **What it tests:** A column-store page starting at recno 100, placed at offset 100 in the file. Salvage must fill in the missing initial record range (records 1–99) before the page.
- **Components:** Column-store recno gap handling during salvage
- **Notes:** Row-store skips this test (key ranges do not have numeric gaps in the same sense).

### Scenario: Column-store only — missing middle key range (run 24)
- **What it tests:** Two non-contiguous column-store pages (recnos 100–109 and 138–147) with a 28-record gap in the middle. Salvage must correctly position both pages and leave the gap empty.
- **Components:** Column-store non-contiguous recno handling
- **Notes:** Row-store skips this test.

### Scenario: Unique vs. non-unique values
- **What it tests:** All 24 runs are executed twice: once with unique values per record (each record has a distinct `ivalue`) and once with a constant value (`37`) across all records. The non-unique variant tests that salvage's LSN-based selection logic is not confused by identical values.
- **Components:** Value uniqueness in page selection
- **Notes:** Non-unique values are the default of most production workloads; unique values make it easier to verify that the correct page was retained.

## Coverage Notes

The salvage test is the definitive correctness test for WiredTiger's database recovery-from-corruption path. It uniquely covers all geometric overlap relationships between two B-tree pages (prefix, suffix, full overlap, containment, and middle-within-outer) for both page types, in both LSN-order directions, with both unique and non-unique data. No other test constructs corrupt files at the binary level and compares salvage output against expected results. Gaps: only tests two-page interactions at a time (no three-way overlap); does not test salvage of files with corrupted checksums or truncated pages; no concurrent-access scenarios; does not test internal (non-leaf) pages; no tiered or disaggregated storage salvage.
