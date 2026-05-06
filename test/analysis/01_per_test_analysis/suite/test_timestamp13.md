# test_timestamp13 — Session query_timestamp API

**File:** `test/suite/test_timestamp13.py`
**Storage mode:** General
**Components under test:** `session.query_timestamp`, per-session timestamp queries (commit, first_commit, prepare, read)

## Test Cases

### `test_timestamp13.test_degenerate_timestamps`
- **What it tests:** Queries all timestamp types (commit, first_commit, prepare, read) outside a transaction — returns 0. Inside a transaction with nothing set — all return 0. Invalid query key raises error. After rollback — all return 0.
- **Components:** `txn_timestamp.c`, `session.c`
- **Notes:** Parameterized over column and row formats.

### `test_timestamp13.test_query_read_commit_timestamps`
- **What it tests:** Sets `read_timestamp=10`, `commit_timestamp=20`, then `commit_timestamp=30` within a transaction; verifies `get=read` returns 10, `get=commit` returns 30 (latest), `get=first_commit` returns 20 (first set).
- **Components:** `txn_timestamp.c`
- **Notes:** Tests that `first_commit` tracks the initially-set commit timestamp and `commit` always reflects the latest.

### `test_timestamp13.test_query_round_read_timestamp`
- **What it tests:** Sets oldest=10, begins transaction with `roundup_timestamps=(read=true)`, sets read_timestamp=5 (below oldest); verifies `get=read` returns 10 (rounded up). Moving oldest to 20 does not change the already-rounded read timestamp.
- **Components:** `txn_timestamp.c`
- **Notes:** Confirms roundup is computed at set time, not dynamically.

### `test_timestamp13.test_query_prepare_timestamp`
- **What it tests:** Prepares a transaction at timestamp=10; verifies `get=prepare` returns 10; sets commit=20, durable=20; verifies prepare still returns 10 and commit returns 20.
- **Components:** `txn_timestamp.c`, `txn.c`
