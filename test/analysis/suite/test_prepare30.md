# test_prepare30 — preserve_prepared requires prepared_id in prepare_transaction

**File:** `test/suite/test_prepare30.py`
**Storage mode:** General
**Components under test:** prepared transactions, preserve_prepared, prepared_id, API validation

## Test Cases

### `test_prepare30.test_prepare30`
- **What it tests:** Verifies that when a connection is opened with `preserve_prepared=true`, calling `prepare_transaction()` without the `prepared_id` parameter returns an error stating "prepared_id need to be set if the preserve_prepared config is enabled"
- **Components:** `txn/txn_prepare.c`, `conn/conn_open.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; no scenarios; a basic API contract test ensuring that the required `prepared_id` parameter is enforced when the preserve_prepared feature is active; `prepared_id` is needed so that the `prepared_discover:` cursor can later identify and claim unresolved prepared transactions after recovery
