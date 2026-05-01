# test_encrypt03 — Encryption configuration error: per-table encryptor without system encryptor

**File:** `test/suite/test_encrypt03.py`
**Storage mode:** General
**Components under test:** encryptors (rotn), schema, configuration parsing

## Test Cases

### `test_encrypt03.test_encrypt`
- **What it tests:** Attempts to create a `table:` with per-table encryption (`rotn,keyid=13`) when the system encryption is `none`. Verifies that `session.create()` raises a `WiredTigerError` matching `/to be set: Invalid argument/`, enforcing the rule that a table cannot use a different encryptor than the system unless the system encryptor is already set.
- **Components:** `src/conn/`, `src/schema/`, `ext/encryptors/rotn`
- **Notes:** Scenario: `table:` only. The commented-out `noname` case (system=rotn, table=none) is explicitly noted as now-permitted (table inherits system encryption). Extensions skipped if missing.
