# test_tiered23 — Tiered flush_tier under artificial storage delay

**File:** `test/suite/test_tiered23.py`
**Storage mode:** Tiered
**Components under test:** flush_tier with dir_store delay extension config (`delay_ms`, `force_delay`), data correctness under delayed I/O, iterative populate-flush loop

## Test Cases

### `test_tiered23.test_tiered`
- **What it tests:** Runs 9 iterations of: populate a `SimpleDataSet` with 10×i rows (10, 20, …, 90), check the data, call `checkpoint('flush_tier=(enabled)')`, then re-check the data. For dir_store, a 130 ms artificial delay is injected every 3 flush operations (`delay_ms=130,force_delay=3`) via the dir_store extension configuration, simulating slow object storage. The test confirms that data integrity is maintained (all populated values are readable) across each flush even when the storage layer is artificially slow.
- **Components:** `ext/storage_sources/dir_store` (delay configuration: `delay_ms`, `force_delay`), `src/tiered/conn_tiered.c` flush_tier path, SimpleDataSet populate/check
- **Notes:**
  - Parametrized across all tiered storage backends (tiered_only=True). Delay config is only applied for local storage (`is_local_storage`); cloud backends return an empty extension config string.
  - The `tiered_extension_config` override in this class sets `delay_ms=130,force_delay=3`, meaning after every 3rd forced flush operation the dir_store extension will sleep 130 ms before completing the write.
  - `key_format=S` via `SimpleDataSet`.
  - Dataset grows linearly (10, 20, …, 90 rows) across iterations, so later flushes carry more data.
