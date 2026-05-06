# test_key_provider — Disagg key provider extension lifecycle tests

**File:** `test/catch2/ext/test_key_provider.cpp`
**Storage mode:** Disagg
**Components under test:** Key provider extension (`WT_KEY_PROVIDER`), key state machine, key lifecycle callbacks
**Test type:** Unit

## TEST_CASE_METHOD (kp_fixture): "Key provider config" [key_provider]
- **What it tests:** `__wti_kp_config` correctly parses and applies the key provider configuration.
- **Components:** `__wti_kp_config`, `WT_KEY_PROVIDER`
- **Notes:** Verifies config fields are populated from the config string.

## TEST_CASE_METHOD (kp_fixture): "Key provider load key" [key_provider]
- **What it tests:** A key can be loaded into the key provider; the key moves to `KEY_STATE_CURRENT`.
- **Components:** `__wti_kp_load_key`, key state machine
- **Notes:** Validates state transition from unloaded to `KEY_STATE_CURRENT`.

## TEST_CASE_METHOD (kp_fixture): "Key provider get key" [key_provider]
- **What it tests:** `__wti_kp_get_key` returns the current key material for encryption.
- **Components:** `__wti_kp_get_key`
- **Notes:** Key must be in `KEY_STATE_CURRENT` for get to succeed.

## TEST_CASE_METHOD (kp_fixture): "Key provider expire key" [key_provider]
- **What it tests:** Expiring the current key moves it to `KEY_STATE_PENDING`.
- **Components:** `__wti_kp_expire_key`, key state machine
- **Notes:** State transition: `KEY_STATE_CURRENT` → `KEY_STATE_PENDING`.

## TEST_CASE_METHOD (kp_fixture): "Key provider rotate key" [key_provider]
- **What it tests:** Rotating a key promotes a pending key to current and archives the old current key.
- **Components:** `__wti_kp_rotate_key`, key state machine
- **Notes:** State transition: `KEY_STATE_PENDING` → `KEY_STATE_CURRENT`; old current → read-only.

## TEST_CASE_METHOD (kp_fixture): "Key provider on_key_update" [key_provider]
- **What it tests:** The `on_key_update` callback is invoked when the key changes, allowing external systems to react.
- **Components:** `__wti_kp_on_key_update`, callback interface
- **Notes:** The callback receives the new key material.

## TEST_CASE_METHOD (kp_fixture): "Key provider key state READ" [key_provider]
- **What it tests:** A key can transition to `KEY_STATE_READ` (read-only, used only for decryption).
- **Components:** Key state machine, `KEY_STATE_READ`
- **Notes:** Read-only keys are retained for decryption but not used for new encryption.

## TEST_CASE_METHOD (kp_fixture): "Key provider full lifecycle" [key_provider]
- **What it tests:** The complete key lifecycle: load → get → expire → rotate → on_key_update → read → destroy.
- **Components:** All `__wti_kp_*` functions
- **Notes:** Integration test for the entire key provider state machine.
