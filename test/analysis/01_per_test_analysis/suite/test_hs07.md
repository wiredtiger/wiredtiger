# test_hs07 — History store: sweep server cleans obsolete HS entries

**File:** `test/suite/test_hs07.py`
**Storage mode:** General
**Components under test:** history store (sweep), timestamps, eviction, modify

## Test Cases

### `test_hs07.test_hs`
- **What it tests:** Three rounds of updates and modifies, each followed by advancing oldest/stable timestamps and sleeping 10 seconds to allow the HS sweep server to clean obsolete entries. After each sweep, verifies that the data is still readable correctly:

  **Round 1:** Inserts 10,000 rows at ts=1, pins oldest/stable=1. Forces eviction of first table via second table updates. Advances oldest/stable=100, sleeps, verifies rows still visible at ts=100. Applies three modifies (at ts=110, 120, 130).

  **Round 2:** Updates 10,000 rows with bigvalue2 at ts=200, forces eviction. Advances oldest/stable=200, sleeps, verifies. Applies three more modifies (ts=210, 220, 230).

  **Round 3:** Updates with bigvalue at ts=300, forces eviction. Advances oldest/stable=300, sleeps, verifies.
- **Components:** `src/history/`, `src/conn/`, `src/modify/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`}; value_format=S; `eviction_updates_trigger=95,eviction_updates_target=80`. Uses 10-second sleeps to let the sweep server run. Ignores stdout `"Eviction took more than 1 minute"`.
