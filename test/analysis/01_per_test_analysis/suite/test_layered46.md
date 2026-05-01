# test_layered46 — Local files are deleted on restart in disaggregated mode

**File:** `test/suite/test_layered46.py`
**Storage mode:** Disagg/Layered
**Components under test:** conn_layered_ingest.c, startup / local file cleanup, checkpoint, WAL logging, schema (layered vs. non-layered tables)

## Test Cases

### `test_layered46.test_layered46`
- **What it tests:** Verifies that when a disaggregated node restarts, local `.wt` files for layered tables are deleted and non-layered ("local") tables are also removed. Creates one layered table and one regular (non-disagg) table, writes three checkpoints with WAL logging enabled, then closes and reopens the connection. Expects "Removing local file" to appear in stdout during reopen. After reopening, picks up the last checkpoint and steps back up to leader. Verifies that (a) the layered table still has the expected data, and (b) the local-only table no longer exists (`open_cursor` raises `WiredTigerError`).
- **Components:** conn_layered_ingest.c (startup local file cleanup), WAL logging (`log=(enabled=true,path=log)`), checkpoint, page log, schema (table vs. layered table open behaviour on restart)
- **Notes:** Overrides `helper_disagg.disagg_ignore_expected_output` to also suppress `WT_VERB_METADATA` warnings from `wiredtiger_open`. Creates the log subdirectory manually in `wiredtiger_open`. Disagg-only, no non-disagg variant. The `lose_all_my_data=true` config is used.
