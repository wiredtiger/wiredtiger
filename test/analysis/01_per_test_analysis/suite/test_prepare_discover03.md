# test_prepare_discover03 — prepared_discover cursor: two prepared transactions, partial claim, unclaimed error

**File:** `test/suite/test_prepare_discover03.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, prepared_discover cursor, claim_prepared_id, error handling

## Test Cases

### `test_prepare_discover03.test_prepare_discover03`
- **What it tests:** Prepares two transactions with `prepared_id=123` and `prepared_id=150`; claims only one (id=123); verifies that attempting to claim an already-claimed id returns an error; verifies that closing the `prepared_discover:` cursor while there is still one unclaimed prepared transaction raises an error stating "Found 1 unclaimed prepared transactions"
- **Components:** `txn/txn_prepare.c`, `cursor/cur_prepare_discover.c`, `checkpoint/checkpoint.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; no scenarios; tests the error contract: (1) double-claiming a prepared_id is rejected; (2) the cursor enforces that all discovered prepared transactions must be claimed before the cursor is closed, otherwise an error is returned; this prevents accidental data loss from forgetting to claim some prepared transactions during recovery
