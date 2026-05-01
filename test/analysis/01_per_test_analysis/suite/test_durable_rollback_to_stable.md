# test_durable_rollback_to_stable — Durable timestamp interaction with rollback_to_stable

**File:** `test/suite/test_durable_rollback_to_stable.py`
**Storage mode:** General
**Components under test:** durable timestamp, prepared transactions, rollback_to_stable, checkpoint, history store

## Test Cases

### `test_durable_rollback_to_stable.test_durable_rollback_to_stable`
- **What it tests:** Verifies that when a prepared transaction's durable timestamp exceeds the stable timestamp, `rollback_to_stable` correctly removes those updates, leaving only the previously durable (first update) values. Full scenario:
  1. Populate 50 rows; checkpoint with `stable_timestamp=100`.
  2. First update (value=111): prepare at ts=150, commit at ts=200, durable at ts=220. Durable timestamp (220) > stable (100) but stable has not moved yet — first update is durable after stable advances.
  3. Read at ts=150: sees original values (pre-update).
  4. Read at ts=220: sees first update (111).
  5. Second update (value=222): prepare at ts=230, then set `stable_timestamp=250`, commit at ts=240, durable at ts=300. Durable timestamp (300) > stable (250), so second update is NOT durable.
  6. Checkpoint: first update (111) is durable; second update (222) is only visible but not durable.
  7. Verify second update is visible in current session.
  8. Call `rollback_to_stable()`: second update should be rolled back.
  9. Reopen, set stable/oldest=250; verify all rows show first update value (111).
  10. Run `wt verify -s` to confirm no residual second-update data.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/txn/txn.c`, `src/checkpoint/`, `src/history/hs.c`
- **Notes:** Scenarios: `file`/`table-simple` x `row-string`/`row-int` (column/recno excluded by `keep` filter). Key timestamp relationships: prepare(150) < commit(200) < durable(220) for first update; prepare(230) < commit(240) < stable(250) < durable(300) for second update. The second update's durable timestamp exceeds stable, making it non-durable. Uses `suite_subprocess` for `wt verify -s`.
