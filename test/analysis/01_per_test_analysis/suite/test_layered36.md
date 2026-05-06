# test_layered36 — Missing stable tables are re-created on restart from page log

**File:** `test/suite/test_layered36.py`
**Storage mode:** Disagg/Layered
**Components under test:** conn_layered_ingest.c, stable btree, checkpoint, restart without local files, schema (table creation)

## Test Cases

### `test_layered36.test_layered36`
- **What it tests:** Verifies that after restarting a node without its local files (simulating cold-start from shared storage), both an empty layered table and a data-containing layered table are re-created correctly from the page log. Creates two tables — one empty, one with a single key — writes a checkpoint, then calls `restart_without_local_files()`, and checks: (a) the empty table is present and contains zero records, (b) the filled table contains the expected key-value pair.
- **Components:** stable btree (re-creation from page log on restart), conn_layered_ingest.c (table open after restart), checkpoint, page log, `restart_without_local_files` helper
- **Notes:** Parametrized over two URI prefix styles: `layered:` (direct layered prefix) and `table:` with `block_manager=disagg,type=layered`. Starts as follower, steps up to leader before the checkpoint. Uses `precise_checkpoint=true`. 500-item capacity not used here (test uses a single key 'a'='b'). Disagg-only.
