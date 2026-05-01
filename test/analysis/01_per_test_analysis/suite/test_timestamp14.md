# test_timestamp14 — Global timestamps: oldest_reader, all_durable, pinned

**File:** `test/suite/test_timestamp14.py`
**Storage mode:** General
**Components under test:** `query_timestamp` (all_durable, oldest_reader, pinned, oldest_timestamp)

## Test Cases

### `test_timestamp14.test_all_durable_old`
- **What it tests:** Verifies `all_durable` is 0 without a commit timestamp; reflects commit at ts=1; held at oldest when a lower-ts transaction is active; resumes after that transaction commits; moves back when a transaction with ts < all_durable is in flight; unaffected by transactions with `no_timestamp=true`.
- **Components:** `txn_timestamp.c`
- **Notes:** Parameterized over integer-row and column formats.

### `test_timestamp14.test_oldest_reader`
- **What it tests:** `oldest_reader` returns 0 with no active reader; reflects read_timestamp of first active reader; unaffected by non-timestamped transactions; moves to next lowest when current oldest reader commits; returns 0 when all readers commit.
- **Components:** `txn_timestamp.c`

### `test_timestamp14.test_pinned_oldest`
- **What it tests:** `pinned` returns 0 with no oldest_timestamp; matches oldest after oldest is set; reflects oldest_reader when a reader is older than oldest; returns oldest after reader commits.
- **Components:** `txn_timestamp.c`

### `test_timestamp14.test_all_durable`
- **What it tests:** Combined test of `all_durable` with prepared transactions: verifies commit+durable interaction for prepared txns, and multiple commit timestamps in one non-prepared txn.
- **Components:** `txn_timestamp.c`, `txn.c`

### `test_timestamp14.test_all`
- **What it tests:** Combined scenario exercising oldest_reader, all_durable, and pinned in a multi-session workload with varying read and commit timestamps; verifies pinned == oldest_reader when reader is active, and pinned == oldest when no readers.
- **Components:** `txn_timestamp.c`
