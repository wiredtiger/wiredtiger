# test_layered_incomplete_table — Layered table incomplete metadata reopen behavior

**File:** `test/catch2/misc_tests/test_layered_incomplete_table.cpp`
**Storage mode:** Disagg
**Components under test:** `__metadata_clean_incomplete_table`, layered table metadata validation on `wiredtiger_open`, `file:T.wt_ingest`, `file:T.wt_stable`
**Test type:** Unit

## TEST_CASE: "Layered table incomplete metadata: leader role" [layered_incomplete]
Tests all four combinations of {ingest present, stable present} for the leader role.

### SECTION: "leader + ingest + stable: should succeed"
- **What it tests:** A complete layered table (both ingest and stable metadata present) reopens successfully as leader.
- **Components:** `wiredtiger_open`, disagg leader role
- **Notes:** Happy path; both required files present.

### SECTION: "leader + ingest, no stable: should abort"
- **What it tests:** Reopening as leader when `file:T.wt_stable` is absent triggers `WT_ASSERT_ALWAYS`.
- **Components:** `__metadata_clean_incomplete_table`, SIGABRT, leader stable requirement
- **Notes:** Run in a forked child process to isolate the abort signal. Returns true if child exits abnormally.

### SECTION: "leader + stable, no ingest: should abort"
- **What it tests:** Reopening as leader when `file:T.wt_ingest` is absent triggers `WT_ASSERT_ALWAYS`.
- **Components:** `__metadata_clean_incomplete_table`, ingest requirement
- **Notes:** Both files are required on leader; missing ingest is fatal.

### SECTION: "leader + neither ingest nor stable: should abort"
- **What it tests:** Reopening as leader with both files absent triggers `WT_ASSERT_ALWAYS`.
- **Components:** `__metadata_clean_incomplete_table`
- **Notes:** Worst case; entire layered table metadata stripped.

## TEST_CASE: "Layered table incomplete metadata: follower role" [layered_incomplete]
Tests all four combinations of {ingest present, stable present} for the follower role.

### SECTION: "follower + ingest + stable: should succeed"
- **What it tests:** A complete layered table reopens successfully as follower.
- **Components:** `wiredtiger_open`, disagg follower role
- **Notes:** Happy path for follower.

### SECTION: "follower + ingest, no stable: should succeed (stable optional on follower)"
- **What it tests:** Reopening as follower with only `file:T.wt_ingest` present succeeds; stable is not required on followers.
- **Components:** `__metadata_clean_incomplete_table`, follower stable optional
- **Notes:** Key semantic difference between leader and follower roles.

### SECTION: "follower + stable, no ingest: should abort"
- **What it tests:** Reopening as follower with `file:T.wt_ingest` absent triggers `WT_ASSERT_ALWAYS`.
- **Components:** `__metadata_clean_incomplete_table`, ingest requirement
- **Notes:** Ingest is required on both leader and follower.

### SECTION: "follower + neither ingest nor stable: should abort"
- **What it tests:** Reopening as follower with both files absent triggers `WT_ASSERT_ALWAYS`.
- **Components:** `__metadata_clean_incomplete_table`
- **Notes:** Run in a forked child process.

**Implementation notes:**
- The `prepare_db` helper creates a complete DB as leader, then optionally reopens as follower to surgically remove metadata entries using `__wt_metadata_remove`.
- The `reopen_aborts` helper forks a child, resets `SIGABRT` to `SIG_DFL` (to bypass Catch2's signal handler), and returns true if the child is killed by a signal or exits non-zero.
- Non-POSIX systems (`_WIN32`) are excluded via `#ifndef _WIN32`.
