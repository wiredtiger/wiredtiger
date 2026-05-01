# test_truncate23 — Truncate boundary conditions with and without prepared transactions

**File:** `test/suite/test_truncate23.py`
**Storage mode:** General
**Components under test:** truncate boundary conditions, prepared transactions, `WT_PREPARE_CONFLICT` for adjacent keys

## Test Cases

### `test_truncate23.test_truncate23`
- **What it tests:** (Currently skipped via `self.skipTest("FIXME-WT-13232")`) Tests a `scenario()` helper that populates keys in the truncation range at ts=10, populates keys outside the range in a second session either with a prepared (ts=20) or committed transaction, then truncates the range at ts=30 and commits (and optionally commits the prepared transaction at ts=40). Verifies that only keys outside the truncation range remain. Covers: start/stop keys existing or not, all-None truncation, empty transaction range, with both prepared=False and prepared=True.
- **Components:** `btree.c`, `txn.c`, `cursor.c`
- **Notes:** Disabled pending FIXME-WT-13232 (how to handle prepare conflicts for keys adjacent to the truncation range). When enabled would test 12 boundary/range combinations × 2 (prepared/not) = 24 scenarios.
