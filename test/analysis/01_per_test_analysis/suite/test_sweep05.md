# test_sweep05 — Detection of sessions without recent sweep

**File:** `test/suite/test_sweep05.py`
**Storage mode:** General
**Components under test:** session sweep staleness detection (`no_session_sweep_5min`, `no_session_sweep_60min`)

## Test Cases

### `test_sweep05.test_short`
- **What it tests:** Opens two sessions; runs session1 with periodic resets (every ~60 s) for 4 minutes while session2 remains idle; after 2 more minutes verifies that the default session and session2 each counted as having missed a sweep (5-min violation count == 2, 60-min == 0); then uses both sessions and resets them; waits another 60 s and verifies violation count is still 2 (counter does not decrease); repeats to accumulate 4 total 5-min violations.
- **Components:** `stat.c`, `session.c`, `file_manager.c`
- **Notes:** Marked `@extralongtest`. Total wall-clock time ~10-15 minutes. Verbose sweep at level 3 generates log lines that are explicitly ignored.

### `test_sweep05.test_long`
- **What it tests:** Runs session1 and session2 for 55 minutes with resets, while the default session is idle; after 55 minutes verifies 1 five-minute violation (default session only); continues session1 for 5 more minutes while session2 is idle; after another 60 s verifies 2 five-minute violations and 1 sixty-minute violation (default session crossed the 60-min threshold).
- **Components:** `stat.c`, `session.c`, `file_manager.c`
- **Notes:** Marked `@extralongtest`. Total wall-clock time ~1.5 hours.
