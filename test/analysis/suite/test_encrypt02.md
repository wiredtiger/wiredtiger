# test_encrypt02 — Encryption with password / secretkey and wt dump

**File:** `test/suite/test_encrypt02.py`
**Storage mode:** General
**Components under test:** encryptors (rotn, sodium), block manager, wt utility (dump)

## Test Cases

### `test_encrypt02.test_pass`
- **What it tests:** Creates a `file:` table with 5,000 random records under various encryption key configurations (no args, keyid only, secretkey only, keyid+secretkey, sodium+secretkey), writes to disk, reopens, verifies all records, and then runs `wt dump` on the encrypted file (passing `-E secretkey` when required) to confirm the dump utility can decrypt and produce non-empty output.
- **Components:** `src/block/`, `src/btree/`, `ext/encryptors/rotn`, `ext/encryptors/sodium`, `src/utilities/`
- **Notes:** Scenarios: noarg, keyid=11, pass=ABC, keyid+pass, sodium+secretkey. Sodium without a keyid (the only valid sodium configuration) is covered here; all other sodium error combinations are tested in test_encrypt08. Uses `suite_subprocess` to invoke the `wt` binary. Extensions skipped if missing.
