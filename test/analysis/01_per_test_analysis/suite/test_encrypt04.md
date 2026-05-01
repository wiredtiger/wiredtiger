# test_encrypt04 — Encryption mismatch between create and reopen

**File:** `test/suite/test_encrypt04.py`
**Storage mode:** General
**Components under test:** encryptors (rotn), block manager, connection open/close lifecycle

## Test Cases

### `test_encrypt04.test_encrypt`
- **What it tests:** Writes 5,000 records to a `table:` using one encryption configuration, then reopens the database with a potentially different configuration (different encryptor name, keyid, or secretkey). Verifies that reopening with matching config succeeds and data is readable; reopening with mismatched config fails as expected. Also tests the `rotn_force_error` extension flag that causes the encryptor to return error code -1000, confirming it propagates through `wiredtiger_open`. When configurations differ, writes more data under the new configuration and verifies both sets.
- **Components:** `src/block/`, `src/conn/`, `ext/encryptors/rotn`
- **Notes:** Cross-product of 5 × 5 = 25 encryption scenario pairs: none vs rotn17abc vs rotn11abc vs rotn11xyz vs rotn11xyz_and_clear. Force-error is triggered when both scenarios specify rotn17abc (scenario 1) and rotn11xyz (scenario 2). `fileinclear` flag disables per-table encryption to allow clear-file reads. Uses custom `setUpConnectionOpen` to intercept the -1000 error code.
