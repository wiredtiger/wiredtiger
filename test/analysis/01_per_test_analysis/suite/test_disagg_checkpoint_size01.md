# test_disagg_checkpoint_size01 — Checkpoint size field correctness in stable-file metadata

**File:** `test/suite/test_disagg_checkpoint_size01.py`
**Storage mode:** Disagg (via `@disagg_test_class` decorator; always disagg)
**Components under test:** src/checkpoint, src/block_disagg, src/conn/conn_layered*.c (stable file metadata), ext/page_log/palite

## Infrastructure notes

`test_disagg_checkpoint_size` is decorated with `@disagg_test_class`, which:
- Mixes in `DisaggConfigMixin` automatically.
- Creates `follower/` and `kv_home/` directories in `early_setup` (symlinks follower's
  `kv_home` → leader's), required for disagg local storage.
- Loads the page log extension via `conn_extensions`.
- Appends `disaggregated=(page_log=<backend>)` to `conn_config` if not already present.
- Suppresses expected `WT_VERB_RTS` verbose output at shutdown.

`conn_config` is set at the class level:
`'disaggregated=(role="leader"),disaggregated=(lose_all_my_data=true)'`

`conn_extensions` also loads the `zstd` compressor (marked `skip_if_missing`) alongside
the disagg page log extension, to support the compression tests.

The helper `find_checkpoint_size(metadata_value)` extracts all `,size=N,` values from the
metadata string via regex and returns the last one (i.e. the most recent checkpoint's size).

There is no `make_scenarios` call — the class runs as a single scenario using whatever
page log backend is active in the test environment.

## Test Cases

### `test_disagg_checkpoint_size.test_checkpoint_size_populated_non_compressed`
- **What it tests:** Inserts 1000 rows of 100-byte values into an uncompressed layered
  table, takes a checkpoint, then reads the `file:<name>.wt_stable` metadata and asserts
  that the `size` field in the checkpoint record is greater than 100,000 bytes
  (i.e. at least as large as the raw data inserted). Confirms the size field is actually
  populated and is not artificially deflated when no compression is in use.
- **Components:** `src/checkpoint` (size field computation during checkpoint completion),
  `src/block_disagg` (bytes written to page log), `src/conn/conn_layered*.c` (stable file
  metadata write path), `src/btree/rec_write.c` (reconciliation size accounting)
- **Notes:**
  - The threshold `> 100000` assumes no compression and includes B-tree overhead, so the
    actual value should be somewhat larger than the raw data.
  - Reads the stable file URI `file:<uri_base>.wt_stable` from the `metadata:` cursor
    directly — tests internal metadata format.
  - Failure indicates the `size` field is missing, zero, or inexplicably small, meaning
    checkpoint size accounting is broken for uncompressed tables.

### `test_disagg_checkpoint_size.test_checkpoint_size_populated_compressed`
- **What it tests:** Same workload as the non-compressed test (1000 rows × 100-byte
  values) but with `block_compressor=zstd` on the layered table. After checkpoint, asserts
  that the `size` field is strictly less than 100,000 bytes, confirming that the recorded
  checkpoint size reflects the compressed (on-disk) size, not the raw data size.
- **Components:** `src/checkpoint`, `src/block_disagg`, `src/conn/conn_layered*.c`,
  `ext/compressors/zstd` (zstd compressor), `src/btree/rec_write.c`
- **Notes:**
  - The zstd extension is loaded with `skip_if_missing=True`, so this test is silently
    skipped on builds without zstd.
  - Together with `test_checkpoint_size_populated_non_compressed`, these two tests form a
    pair that verifies the size field tracks compressed on-disk bytes, not logical data bytes.
  - Failure means either compression has no effect on the recorded size (the field stores
    uncompressed size) or the field is not updated at all.

### `test_disagg_checkpoint_size.test_checkpoint_size_increases`
- **What it tests:** Inserts 500 rows and takes checkpoint 1; records the size. Then inserts
  1000 more rows (keys 500–1499) and takes checkpoint 2; records the new size. Asserts that
  the second checkpoint size is strictly greater than the first, confirming monotonic growth
  of the size field as data is added.
- **Components:** `src/checkpoint`, `src/block_disagg`, `src/conn/conn_layered*.c`
- **Notes:**
  - Uses two sequential checkpoints with growing data to verify the size reflects the
    cumulative state, not just the delta of the most recent checkpoint.
  - Failure means the `size` field is static (never updated after the first checkpoint)
    or decrements incorrectly.

### `test_disagg_checkpoint_size.test_checkpoint_size_persists_across_restart`
- **What it tests:** Inserts 1000 rows, takes a checkpoint, reads the `size` field from
  stable-file metadata, then calls `reopen_conn()` (which triggers the disagg reopen path,
  shown by the expected stdout pattern `"Removing local file"`). After restart, reads the
  `size` field again and asserts it equals the pre-restart value.
- **Components:** `src/checkpoint`, `src/conn/conn_layered*.c` (metadata reload on open),
  `src/block_disagg` (page log metadata persistence), `ext/page_log/palite`
- **Notes:**
  - The `expectedStdoutPattern("Removing local file")` guard ensures the test is running
    in the correct disagg restart mode where local files are removed and state is recovered
    from the page log.
  - Failure means the checkpoint size stored in the page log's metadata record is not
    re-applied to the local stable-file metadata on restart, breaking any consumer that
    uses this size (e.g. for capacity planning or ingest gating).
