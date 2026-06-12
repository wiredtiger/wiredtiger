# Plan — unify transaction enum + structured weights

Two asks from Ivan (2026-06-12), implemented in reviewed steps. The C++ prototype stays a POC
(no work this round).

## Ask 1 — one transaction enum + `write_allowed`
`Mode` {AUTOCOMMIT, RO_TXN, RW_TXN} and `Iso` {SNAPSHOT, READ_COMMITTED, READ_UNCOMMITTED}
intersect. Collapse them into ONE enum:

```python
class Txn(Enum):
    NO = ...                 # autocommit / no transaction open
    SNAPSHOT = ...           # read-write snapshot transaction
    READ_COMMITTED = ...     # read-only (isolation rejects writes)
    READ_UNCOMMITTED = ...   # read-only
    READ_TIMESTAMP = ...     # snapshot + as-of-past read_timestamp (read-only)
```

- A function `write_allowed(txn)` (a.k.a. the inverse `is_read_only`) returns whether writes are
  legal: `txn in (Txn.NO, Txn.SNAPSHOT)`. This replaces the `txn_readonly` flag.
- `State` holds `self.txn` (default `Txn.NO`) instead of `in_txn` + `txn_readonly`. `in_txn` is
  `txn is not Txn.NO`. `txn_read_ts` stays (the numeric as-of timestamp; set only for READ_TIMESTAMP).
- `_mode()` is deleted; `_legal` reads `self.state.txn` directly.
- `READ_TIMESTAMP` becomes a first-class mode (today it is "snapshot + read_ts"). `TODO(read-ts)`:
  in future drive boundary read timestamps (exactly oldest, exactly stable, mid-window) rather than
  a single uniform draw.

## Ask 2 — one structured, inheritable weights config
All workload weights live in ONE nested structure (`DEFAULT_WEIGHTS`), with **hierarchical /
inherited** weights. Shape (Ivan's spec):

```python
DEFAULT_WEIGHTS = {
    'next': 40, 'prev': 40, 'search': 12, 'search_near': 10,   # top-level leaf ops
    'pos_update': 14, 'pos_remove': 8,
    'put': 30, 'remove': 8, 'reset': 12, 'verify': 15,
    'txn': {                       # group: .weight = chance of a txn action at the top level
        'weight': 12,
        'snapshot': 72, 'read_committed': 16,                  # begin-mode mix (when no txn open)
        'read_uncommitted': 12, 'read_timestamp': 30,
        'commit': 70, 'rollback': 30,                          # close mix (when a txn IS open)
    },
    'scenarios': {                 # group: rare checkpoint / eviction / (future) injected scenarios
        'weight': 36,
        'advance': 50, 'evict': 50,
        # TODO(scenarios): forced_evict, bulk_remove, massive_prepare, truncate -- add as op rows
    },
}
```

**Semantics (Ivan's rule):** a group's `.weight` is its share at the *parent* level; a child's
weight is conditional *within* the group. Effective leaf weight = product of weights along the path,
so after legality-filtering we just sum the survivors and sample — renormalisation is automatic.

**Inheritance:** `DEFAULT_WEIGHTS` is the base. `run_sequence(seed, tag, n_ops, weights=None)`
defaults to it; a test passes an override that is **deep-merged** over the base (`merge_weights`),
so a scenario-heavy test can bump just `scenarios` without restating everything.

### How it maps onto the code
- `pick_op` is rewritten around a `_candidates(positioned)` helper that walks the tree and yields
  `(effective_weight, spec)` for every *legal* op, then weighted-samples one. The `txn` group is the
  one state-dependent node: when no txn is open it offers `begin` (weight = group weight; op_begin
  picks its own mode from the begin-mode sub-weights); when a txn is open it offers `commit` /
  `rollback` by their sub-weights. `scenarios` and top-level leaves are generic.
- `op_begin` reads the `txn` begin-mode sub-weights to choose its `Txn` mode; `READ_TIMESTAMP` falls
  back to `SNAPSHOT` when no past window exists (`oldest_ts < 1` or `ts <= oldest_ts`).
- **Removed:** `P_BREAK`, `P_TXN`, the `Category` enum, `OpSpec.weight`, `OpSpec.category`.
- **OpSpec** keeps only: `fn`, `name` (the config key — a string used purely to *look up a weight*,
  never to dispatch/branch), `needs_position`, `needs_live`, `is_write`, `autocommit_only`,
  `in_txn_only`.

### Decision / revisit flags
- **D1 — drop keep/break + `P_BREAK`.** Ivan's example uses flat top-level op weights, so the
  single "fraction of ops that break the cursor position" knob goes away; the long-chain bias is now
  implicit in next/prev (80) being heavy vs the break ops. `TODO(workload-tuning)`: the effective
  break fraction is documented in a comment; revisit whether a derived knob should be reintroduced.
- **D2 — string keys in the weights config are OK.** They are config *data* (a human-authored
  literal), mapped to ops via a one-time `name -> OpSpec` lookup. This does not reintroduce
  string-based *dispatch* (control flow still goes through the direct `OpSpec.fn` ref). Documented.
- **D3 — `write_allowed` is a module function** (matches Ivan's wording), not an enum method.

## Steps (checkboxes)
- [ ] **P1. Txn enum + `write_allowed`.** Replace Mode/Iso; rewire State, `_legal`, `op_begin`,
      `_end_txn`, the coverage counters. Behaviour-preserving (same begin-mode probabilities, just
      expressed via `Txn`). Suite green. **Review (agent).** Commit.
- [ ] **P2. Structured weights.** Add `DEFAULT_WEIGHTS` + `merge_weights`; rewrite `pick_op` via
      `_candidates`; `op_begin` reads mode sub-weights; remove P_BREAK/P_TXN/Category/OpSpec.weight;
      `run_sequence`/tests take `weights`. Suite green + coverage guards. **Review (agent).** Commit.
- [ ] **P3. Docs + diagrams.** Sync architecture doc, function map, both `.dot`s; consolidate the
      TODO list (code `# TODO(...)` markers mirrored in `layered_cursor_stress_test_todo.md`). Commit.

## TODO list (mirrored from code `# TODO(...)` markers — start of the consolidated list)
- `merge-coverage` — forced-eviction scenario op + ~300k-op long run, then restore the
  `assert_merge_exercised` floor from the 1% interim.
- `workload-tuning` — revisit the weights once the long run lands; reconsider a derived break knob (D1).
- `pin-reset` — extend the as-of-T cursor reset to RC/RU read-only txns only if a Q2-style divergence appears.
- `scenarios` — add forced_evict / bulk_remove / massive_prepare / truncate as op rows under the
  `scenarios` group.
- `read-ts` — drive boundary read timestamps (oldest / stable / mid-window), not just a uniform draw.
- `prepare-chain` — fold prepared-txn conflicts into the random chain (currently only in the
  bug-review package); awaiting the storage team's verdict on the prepare-iterate bug candidate.
- `cpp-poc` — the C++ rewrite prototype is a POC in an agent worktree; promote into `test/csuite`
  (built as C++) for single-process gdb + 300k-op runs once parity (C1–C5) is reached.
