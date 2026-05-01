# test_tiered04 — Tiered storage API: flush statistics, local retention, object naming, and reconfiguration

**File:** `test/suite/test_tiered04.py`
**Storage mode:** Tiered
**Components under test:** flush_tier API, local_retention, `flush_tier_skipped`/`flush_tier_switched`/`flush_tier` statistics, `local_objects_removed` stat, tiered metadata (`last`, `oldest`, `tiered_object`), `conn.reconfigure`, flush_tier options (timeout, sync, force)

## Test Cases

### `test_tiered04.test_tiered`
- **What it tests:** Comprehensive test of the `checkpoint('flush_tier=(enabled)')` API covering: (1) three-table setup — system-tiered, per-table-tiered with its own bucket and 600 s retention, and non-tiered (`tiered_storage=(name=none)`); (2) statistics tracking of flush skip/switch counts; (3) local retention — object files exist locally after flush and are automatically removed by the background thread after `local_retention` seconds; (4) metadata correctness (`last=N`, `oldest=1`, `tiered_object=true/false`, `flush_time`, `flush_timestamp`); (5) connection-level `reconfigure` of `tiered_storage=(local_retention=...)`; (6) flush_tier configuration variants: `timeout=100`, `sync=false`, `force=true`; (7) flush state persistence across a connection close/reopen — the first post-restart flush correctly skips the unmodified table and switches the modified one.
- **Components:** `src/tiered/conn_tiered.c`, flush_tier path, background tiered-manager thread, `wt_tiered_work_unit` processing, storage_source extension (dir_store / s3_store / etc.), metadata cursor
- **Notes:**
  - Parametrized across all tiered storage backends (dir_store, s3_store, gcp_store, azure_store). No non-tiered scenario.
  - `local_retention=3` for the connection-level table; `local_retention=600` for the per-table override.
  - Verifies that the object file `base + '1.wtobj'` exists after flush and is removed after sleeping past retention.
  - Verifies `tiered_object=true` on tiered file/object/tier URIs and `tiered_object=false` on the plain `file:` URI.
  - Uses `time.sleep` to drive the background thread between retention checks.
