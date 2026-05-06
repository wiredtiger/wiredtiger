# test_encrypt07 — Salvage of encrypted databases

**File:** `test/suite/test_encrypt07.py`
**Storage mode:** General (skipped for tiered storage)
**Components under test:** encryptors (rotn/rot13), salvage, btree

## Test Cases

### `test_encrypt07.test_salvage_api`
- **What it tests:** Inherited from `test_salvage01.test_salvage_api`. Runs the salvage API test against a `table:` encrypted with rotn keyid=13 (rot13). Verifies that `session.salvage()` correctly recovers an intact encrypted table.
- **Components:** `src/session/`, `src/btree/`, `ext/encryptors/rotn`
- **Notes:** Inherits `test_salvage01` and overrides `moreinit` to search for rot13-encoded unique bytes in the file when locating a spot to damage. The `uniquebytes` used for damage location are rot13-encoded before the file scan.

### `test_encrypt07.test_salvage_api_damaged`
- **What it tests:** Inherited from `test_salvage01`. Damages the encrypted table file and verifies that `session.salvage()` can recover some or all data.
- **Components:** `src/session/`, `src/block/`, `ext/encryptors/rotn`
- **Notes:** Skipped under tiered storage hook.

### `test_encrypt07.test_salvage_process_damaged`
- **What it tests:** Inherited from `test_salvage01`. Uses the `wt salvage` command-line process on a damaged encrypted table and verifies recovery behavior.
- **Components:** `src/utilities/`, `ext/encryptors/rotn`
- **Notes:** Skipped under tiered storage hook. Uses 5,000 records, bigvalue = `"abcdefghij" * 1007`.
