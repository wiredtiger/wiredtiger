# test_prepare38 — Database written with preserve_prepared can be opened without it

**File:** `test/suite/test_prepare38.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true` → reopen without)
**Components under test:** prepared transactions, preserve_prepared, backward compatibility, connection open

## Test Cases

### `test_prepare38.test_open`
- **What it tests:** Creates a database with `preserve_prepared=true`, writes and commits a prepared transaction, checkpoints; then copies the home directory and reopens the copy with `preserve_prepared=false`; verifies the database opens successfully and the committed data is readable
- **Components:** `conn/conn_open.c`, `txn/txn_prepare.c`, `checkpoint/checkpoint.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; the reopen uses `preserve_prepared=false` (or omits the option); this tests backward compatibility — a database written with the preserve_prepared feature should still be readable by a connection that does not enable it; no scenarios; verifies that the on-disk format produced by preserve_prepared is consumable by a standard connection
