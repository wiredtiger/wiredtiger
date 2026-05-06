# test_layered74 — Internal page delta correctness with encryption and compression

**File:** `test/suite/test_layered74.py`
**Storage mode:** Disagg/Layered
**Components under test:** Internal and leaf page delta generation, `rec_page_delta_leaf`, `rec_page_delta_internal`, `cache_read_internal_delta`, encryption, compression

## Test Cases

### `test_layered74.test_internal_page_delta_random`
- **What it tests:** Populates 10,000 keys with small 512-byte pages (to force splits and internal page creation), checkpoints, reopens the connection, then runs 1–10 random delta rounds (each updating 10–2000 random keys and checkpointing). Verifies: delta stats (`rec_page_delta_leaf` and/or `rec_page_delta_internal`) are non-zero for `write_leaf_only`/`write_both`, or zero for `write_none`. After all deltas, reopens and verifies all values match expected (delta-merged results). Checks `cache_read_internal_delta` is non-zero for internal-delta-enabled scenarios. Finally reopens as follower and repeats the value verification plus `cache_read_internal_delta` check.
- **Components:** `src/btree/bt_rec.c`, `src/btree/bt_page.c`, page log extension, encryption extension (`rotn`), compression extension (`snappy`)
- **Notes:** Highly parametrized: encrypt (none/rotn) × compress (none/snappy) × uri (layered: vs file: with block_manager=disagg) × ts (timestamp/non-timestamp) × delta config (leaf_only/none/both). 512-byte allocation/page sizes to force page splits. The number of delta rounds and modified keys are randomly chosen, so the test catches ordering/merge bugs probabilistically. Uses `reopen_disagg_conn()` to simulate cold restart.
