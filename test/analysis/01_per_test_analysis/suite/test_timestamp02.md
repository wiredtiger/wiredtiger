# test_timestamp02 — Basic timestamp semantics: insert/update/delete visibility

**File:** `test/suite/test_timestamp02.py`
**Storage mode:** General
**Components under test:** transaction timestamps, `oldest_timestamp`, `stable_timestamp`, `durable_timestamp`, timestamp stats

## Test Cases

### `test_timestamp02.test_basic`
- **What it tests:** Inserts 100 keys at timestamp=key; verifies reads at each timestamp see exactly the right subset of keys; advances oldest_timestamp to 100; updates all keys at timestamp=key+100; verifies `all_durable` tracking; sets stable_timestamp=200; reads at each update timestamp; advances oldest; removes all keys at timestamp=key+200; verifies reads through all phases. Also validates error conditions: oldest must not go backward, stable must not go backward, oldest must not exceed stable. Tests combined oldest+stable set_timestamp with conflicting values. Tests `force` mode for setting oldest ahead of stable and confirms `txn_set_ts_force` stat.
- **Components:** `txn_timestamp.c`, `txn.c`
- **Notes:** Parameterized over column and row key formats. Tests `txn_set_ts_oldest`, `txn_set_ts_oldest_upd`, `txn_set_ts_stable`, `txn_set_ts_stable_upd`, `txn_set_ts_durable`, `txn_set_ts_durable_upd`, `txn_set_ts_force` statistics.

### `test_timestamp02.test_read_your_writes`
- **What it tests:** Confirms that within a transaction with a read_timestamp, a write to the same key is immediately visible to the writing transaction (read-your-own-writes semantics).
- **Components:** `txn.c`, `txn_timestamp.c`
- **Notes:** Parameterized over column and row formats.
