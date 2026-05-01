# test_encrypt09 — Sodium encryptor per-table configuration error paths

**File:** `test/suite/test_encrypt09.py`
**Storage mode:** General
**Components under test:** encryptors (sodium), schema

## Test Cases

### `test_encrypt09.test_encrypt`
- **What it tests:** With sodium as the system encryptor (using a valid secretkey), attempts to create a `file:` table with various per-table sodium encryption configurations and verifies the expected error (or success) from `session.create()`. Exercises the encryptor's `customize` method for per-table encryption.
- **Components:** `src/schema/`, `ext/encryptors/sodium`
- **Notes:** Scenarios:
  - `nokey` (empty per-table config) → success (no separate encryptor created)
  - `keyid=123` → `/keyids not supported/`
  - `keyid=123,secretkey=...` → `/unknown configuration key: .secretkey.:/` (secretkey not allowed per-table)
  - `secretkey=plop` → same `unknown config key` error
  - `secretkey=<short>` → same
  
  The `twokeys`, `nothex`, and `badsize` scenarios do not reach the sodium extension because `secretkey=` is not a recognized per-table config key; the error comes from WiredTiger's config parser instead. Extension skipped if missing.
