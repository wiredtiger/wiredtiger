# test_checkpoint09 — Reconciliation clears obsolete time window info from on-disk cells

**File:** `test/suite/test_checkpoint09.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, reconciliation, time window cleanup, statistics

## Test Cases

### `test_checkpoint09.test_checkpoint09`
- **What it tests:** Verifies that when checkpoint reconciliation processes pages, it removes obsolete time window (TW) start-timestamp information from on-disk cells, incrementing the `rec_time_window_start_ts` statistic.
- **Components:** `src/reconcile/rec_write.c`, `src/reconcile/rec_visibility.c`, `src/checkpoint/`
- **Notes:** Populates a table with timestamped data, advances `oldest_timestamp` to make some TW info obsolete, then runs a checkpoint. Checks `stat.conn.rec_time_window_start_ts > 0` confirming that reconciliation cleaned obsolete start-timestamp cells. This is a unit-level check of the reconciliation time-window cleanup path.
