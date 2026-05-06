# test_encrypt05 — Encryption config rejected when keyid contains escaped characters

**File:** `test/suite/test_encrypt05.py`
**Storage mode:** General
**Components under test:** encryptors (rotn), configuration parsing

## Test Cases

### `test_encrypt05.test_encrypt`
- **What it tests:** Reopens the connection with a malformed encryption config in which the `keyid` value contains a quoted escaped character (`\n`, `\r`, `\t`, or `\b`). Verifies that WiredTiger either rejects it with `"Invalid argument"` or, if it succeeds, performs a clean reopen with an empty config afterward to avoid teardown errors.
- **Components:** `src/config/`, `ext/encryptors/rotn`
- **Notes:** Scenarios: `\n`, `\r`, `\t`, `\b`. The test intentionally ignores the stderr pattern `"Unexpected escaped character: Invalid argument"`. Extensions skipped if missing.
