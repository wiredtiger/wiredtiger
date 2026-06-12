# Layered cursor stress test — TODO list

The single consolidated list of open items. Each item has a `topic` that matches a `# TODO(<topic>)`
marker in `test/suite/test_layered_cursor_stress.py` (grep `TODO(` to find the code site). Keep this
file and the code markers in sync — when you add a `# TODO(x)` in the test, add a row here; when you
resolve one, remove both.

| topic | where (code) | what | status |
|---|---|---|---|
| `merge-coverage` | `assert_merge_exercised` | Stable-read fraction is only ~2–9% in the random run, so the floor was lowered 10%→1% as an interim. Real fix: a forced-eviction scenario op + a long run (~300k ops, not 300) to heavily exercise the follower stable path, then restore a meaningful floor. | open (interim 1% floor) |
| `scenarios` | `DEFAULT_WEIGHTS['scenarios']` | Add injected-scenario ops under the `scenarios` group: `forced_evict`, `bulk_remove`, `massive_prepare`, `truncate`. Each = one `op_<name>` method + one OpSpec row + one weight. | open |
| `workload-tuning` | `DEFAULT_WEIGHTS` | The weights are a rough first pass. Revisit once the long run lands; in particular decide (D1) whether to reintroduce a derived "break fraction" knob (the old `P_BREAK`), now that break frequency is implicit in the weights. | open |
| `pin-reset` | `_end_txn` | The as-of-T (READ_TIMESTAMP) cursor reset guards against the Q2 pin. RC/RU read-only txns also hold cursors across commit but resume in a compatible (latest) view, so no pin divergence has been seen — extend the reset to them only if one ever appears. | open (watch) |
| `read-ts` | `op_begin` | READ_TIMESTAMP currently draws a single uniform past timestamp. Drive boundary read timestamps (exactly oldest, exactly stable, mid-window) to stress the edges. | open |
| `prepare-chain` | — | Fold prepared-transaction conflicts into the random chain (today they live only in the bug-review package `findings/` + `test/suite/test_layered_prepare_iterate_diff.py`). Blocked on the storage team's verdict on the prepare-iterate bug candidate. | blocked |
| `cpp-poc` | — | A C++ rewrite prototype exists as a POC in an agent worktree (single-process gdb into `cur_layered.c`, cheap 300k-op runs). Promote into `test/csuite` (built as C++) once Python parity (C1–C5) is reached and the team agrees. | POC only |

## Findings status (not TODOs, but tracked here for one-stop visibility)
- **Q1** cross-txn positioned remove — REAL BUG, fixed (WT-17796).
- **Q2** pinned-snapshot scan-vs-search — NOT a bug (read-committed snapshot pinning).
- **prepare-iterate** divergence — likely a real, fixable bug; review package prepared, awaiting the
  storage team (`findings/prepare_iterate_bug_candidate.md`).
