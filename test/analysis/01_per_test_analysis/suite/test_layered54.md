# test_layered54 — Prefix and suffix compression in page deltas

**File:** `test/suite/test_layered54.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, reconciliation (page delta prefix/suffix compression), page log, checkpoint, follower reads

## Test Cases

### `test_layered54.test_prefix_suffix_compression1`
- **What it tests:** Calls `verify_compression(False)` — tests page deltas without prefix compression. Verifies that suffix compression is applied on initial full-page writes (`rec_suffix_compression > 0`), no prefix compression occurs for delta pages (`rec_prefix_compression_delta == 0`), and no prefix compression occurs for full pages either (since it is disabled). After updates, verifies leaf and/or internal deltas are written per the delta configuration, and that data is correct on both leader reopen and follower reopen.
- **Components:** reconciliation (suffix compression, prefix compression for full pages and delta pages), block_disagg, page log, checkpoint

### `test_layered54.test_prefix_suffix_compression2`
- **What it tests:** Calls `verify_compression(True)` — tests page deltas with prefix compression enabled. Verifies suffix compression occurs on initial writes, prefix compression occurs for full pages (`rec_prefix_compression_full > 0`) but not for delta pages initially. After a reopen and small updates, verifies that deltas use prefix compression (`rec_prefix_compression_delta > 0`) and that full-page prefix compression drops to 0 (only deltas are written). Data is verified on leader and follower reopens.
- **Components:** reconciliation (prefix compression in delta path), block_disagg, page log, checkpoint, follower reads

- **Notes (both tests):** Parametrized over three delta configurations: `leaf_only`, `internal_only`, `both`. Combined with disagg storage backend. Uses `file:` URI with `block_manager=disagg`, small page sizes (512 B), `delta_pct=100`. 1000 rows with shared prefix key `"abcabcabcabc{i}"` to generate compressible keys. Updates rows 1–19 to trigger delta writing. Uses `reopen_disagg_conn` between phases; follower reopen sleeps 1 second. Disagg-only.
