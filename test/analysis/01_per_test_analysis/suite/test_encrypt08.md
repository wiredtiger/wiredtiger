# test_encrypt08 — Sodium encryptor system-level configuration error paths

**File:** `test/suite/test_encrypt08.py`
**Storage mode:** General
**Components under test:** encryptors (sodium), connection open

## Test Cases

### `test_encrypt08.test_encrypt`
- **What it tests:** Reopens the database with sodium as the system-level encryptor using various invalid configurations, and asserts each raises a `WiredTigerError` with the expected message. Covers the encryptor's `customize` method error paths for system encryption.
- **Components:** `src/conn/`, `ext/encryptors/sodium`
- **Notes:** Scenarios and expected errors:
  - `nokey` (empty config) → `/no key given/`
  - `keyid` (keyid=123) → `/keyids not supported/`
  - `twokeys` (keyid + secretkey) → `/keys specified with both/`
  - `nothex` (secretkey=plop) → `/secret key not hex/`
  - `badsize` (secretkey=short hex) → `/wrong secret key length/`
  
  The test deliberately avoids using `conn_config` for encryption setup so it can catch exceptions from `reopen_conn()`. Extension skipped if missing.
