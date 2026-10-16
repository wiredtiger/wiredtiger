# PoC: in-memory files without `WT_BLOCK`

Branch: `poc/in-memory-no-block-handle`

## Approach

When `file_config` has `in_memory=true`, `__wt_blkcache_open` wires a `WT_BM` via
`__wti_bm_open_in_memory_file()` and does **not** call `__wt_block_open`. `bm->block`
stays `NULL`; there is no connection `blockhash` entry and no backing `WT_FH` for that
object.

`__btree_conf` still sets `WT_BTREE_IN_MEMORY` from the same config (unchanged). Checkpoint
and eviction behaviour for in-memory btrees is unchanged: reconciliation must not persist
pages, and checkpoint skips these trees.

This differs from the WT-18888 reference on `otherwt`, which still allocates a `WT_BLOCK`
but sets `block->in_memory` and skips `__block_open_file`, with EINVAL on block read/write.

## Files touched

| File | Change |
|------|--------|
| `src/block_cache/block_cache.c` | `__blkcache_file_in_memory()`; skip `__wt_block_open` when set |
| `src/block_cache/block_mgr.c` | In-memory `WT_BM` vtable, `__wti_bm_open_in_memory_file()`, null-safe `__bm_close` / `__bm_readonly` |
| `src/include/extern.h` | Declare `__wti_bm_open_in_memory_file` |

Layered ingest already passes `in_memory=true` in `schema_create.c` (`ingest_cfg`); no schema
change required for this PoC.

## What still assumes `btree->bm` / `WT_BLOCK`

Many call sites use `btree->bm` only through the `WT_BM` method table (OK if methods are
stubbed). Direct `bm->block` dereferences remain elsewhere and are **not** safe with this
PoC unless gated:

- `meta_ckpt.c`, `conn_compact.c`, `block_map.c`, `live_restore_server.c`
- Debug / verify / compact paths in `btree/` (`bt_debug.c`, `bt_compact.c`, `bt_vrfy.c`)
- Default `block_mgr.c` methods if any were left unwired (the in-memory vtable overrides the
  full set used at open)

Re-open after restart with non-empty checkpoint metadata pointing at on-disk addresses is
rejected in `__bm_inmem_checkpoint_load` (EINVAL).

Connection-level `in_memory=true` is **unchanged**: those files still use a normal
`WT_BLOCK` unless the per-file `in_memory` config is set.

## Gaps vs production

- No `read`/`write`/`free`: correct for `WT_BTREE_IN_MEMORY` today, but any future path that
  tried to persist in-memory tables would need a different story.
- `test_layered_dhandle01.test_ingest_fd_reclaimed` fails on the assertion that expects
  `.wt_ingest` file descriptors on the follower (counts 0 with this design). That test encodes
  the stub-file-handle model; it would need revisiting if we adopt no-block handles.
- `dist/s_void` flags several in-memory stubs as "could return void" (false positives for
  `WT_BM` signatures).
- `s_export` failure in this environment appears unrelated (compatibility build symbol drift).

## vs stub-handle approach (WT-18888 / `otherwt`)

| | Stub `WT_BLOCK` (`block->in_memory`) | No `WT_BLOCK` (this PoC) |
|--|--------------------------------------|---------------------------|
| FD / `blockhash` | Handle exists; file open skipped | No handle, no FD |
| Shared block refcount | Yes (same as other files) | N/A |
| `bm->block` | Non-NULL | NULL |
| Risk | Leaks assumptions that `block` exists | Leaks assumptions that I/O stubs are never called |
| Layered ingest FD tests | Can count `.wt_ingest` FDs | No FDs to count |

## Tests run

```bash
cmake --build build
cd build && python3 ../test/suite/run.py test_prepare44   # PASS
cd build && python3 ../test/suite/run.py test_inmem01    # PASS (conn in_memory; unchanged path)
cd build && python3 ../test/suite/run.py test_layered_dhandle01  # FAIL (ingest FD count; see above)
```
