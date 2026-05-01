# test_layered43 — Disaggregated storage with block cache (currently skipped)

**File:** `test/suite/test_layered43.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, block cache (DRAM), page log, checkpoint, delta chains, eviction

## Test Cases

### `test_layered43.test_layered43`
- **What it tests:** Intended to verify that the block cache correctly caches pages read from the disaggregated page log, and that re-reading a page after eviction is served from the block cache (no additional `cache_read` increment, but `cache_pages_requested` increases). Also checks that the block cache removes stale cached blocks when a page is superseded (`block_cache_blocks_removed` increases). Creates a table, inserts 500 records, then performs 10 updates to a single key (building a delta chain), evicts the page twice, and reads it back expecting block-cache satisfaction.
- **Components:** block_disagg, block cache (`block_cache=(enabled=true,type="dram",size=256MB)`), page log, checkpoint, eviction (`debug=(release_evict)`), statistics (`block_cache_blocks_removed`, `cache_read`, `cache_pages_requested`)
- **Notes:** **Currently skipped** — `early_setup` calls `self.skipTest("FIXME-WT-15663: currently block cache is disabled.")`. Parametrized over two URI prefixes (`layered:` and `table:` with `block_manager=disagg,log=(enabled=false)`). Uses `precise_checkpoint=true`. Disagg-only.
