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
- [x] **P1. Txn enum + `write_allowed`.** Replaced Mode/Iso; rewired State, `_legal`, `op_begin`,
      `_end_txn`, the coverage counters. Behaviour-preserving. Suite green. **Reviewed APPROVE.**
      Commit `4d3fa1f5b7`.
- [x] **P2. Structured weights.** Added `DEFAULT_WEIGHTS` + `merge_weights`; rewrote `pick_op` via
      `_candidates` (groups contribute exactly their share over *normalised* children — the bug
      that earlier let `gw*70=560` swamp leaf ops); `op_begin` reads the begin-mode sub-weights;
      removed P_BREAK/P_TXN/Category/OpSpec.weight; `run_sequence(weights=...)`. Re-tuned the
      position-breaking weights down so chains stay long (positioned ~36%, n_positional ~139). Suite
      green + all coverage guards comfortable. Commit `f542f3a921`. **Review running.**
- [x] **P3. Docs + diagrams.** Synced §6 (generation), the function map, and the oploop `.dot`/svg/png
      to the OpSpec-name + DEFAULT_WEIGHTS + `_candidates` design; created the consolidated
      `layered_cursor_stress_test_todo.md` (code `# TODO(...)` markers mirrored there).
- [x] **P4. Weights as named-field dataclasses** (Ivan's follow-up: "so we don't mistype weight").
      Replaced the string-keyed dict + `merge_weights` with frozen `Weights`/`TxnWeights`/
      `ScenarioWeights`; each leaf field name == its `OpSpec.name`; `_candidates` walks
      `dataclasses.fields()`; `_validate_weights()` asserts at setup that every weight field is backed
      by a real op (typo → loud setup failure, not a mid-run KeyError); inheritance via dataclass
      defaults + `dataclasses.replace`. Suite green, coverage unchanged. Commit `4685a6b0c0`. Docs
      synced. **Review running.**

## TODO list
Moved to the dedicated **`layered_cursor_stress_test_todo.md`** (one row per `# TODO(<topic>)` code
marker). Keep that file in sync with the code markers.
