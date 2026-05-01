# test_timestamp28 — Commit timestamp validated against stable and oldest at both set and commit time

**File:** `test/suite/test_timestamp28.py`
**Storage mode:** General
**Components under test:** `timestamp_transaction`, `commit_transaction`, stable/oldest commit timestamp enforcement

## Test Cases

### `test_timestamp28.test_timestamp28`
- **What it tests:** Sets stable (or oldest) to 30; tries to commit at ts=20 — expects error at commit time. Sets to 40; sets commit_timestamp=50 via `timestamp_transaction`; advances stable/oldest to 60; tries to commit — expects error because 50 is now behind 60. Multi-step scenario: sets commit timestamps 70 and 71 in one transaction, advances stable/oldest to 75, tries to commit at 80 — expects error because first commit (70) is before stable/oldest (75).
- **Components:** `txn_timestamp.c`, `txn.c`
- **Notes:** Parameterized over `stable_timestamp` vs `oldest_timestamp` as the global timestamp being tested. Tests that the earliest commit timestamp in a multi-step transaction is the one validated.
