# Generator/driver refactor — plan

Refactor the workload generator (`pick_op`) and the per-sequence driver (`run_sequence`) of
`test/suite/test_layered_cursor_stress.py` into a cleaner, single-responsibility design, driven by
one declarative **workload-shape config**. Four asks (all assessed as sound):

1. **One `config` structure** holding the entire workload shape (all weights), multi-level.
   Representation is open — three independent proposals are being generated (position-break-first,
   read/write split with conditional rates, flat tagged op-table); Ivan picks.
2. **`pick_op` returns a bound callable**, one function per op, so `run_sequence` just calls it.
3. **A `State` class** encapsulating the model state (`live`, `cur_pos`, txn flags, `ts`, counters).
4. **`verify()` becomes a weighted op** (`full_scan`); the always-on oracle is per-op `compare_read`.

**Invariant for every step:** the suite stays green and `assert_self_coverage` passes. Note the
refactor *changes the generated sequences* (RNG draw order differs), so traces won't match the old
ones — that's fine; each run is still deterministic per seed. "Behaviour-preserving" here means
"same op vocabulary + roughly the same workload distribution + still green," not identical traces.

**Process:** one step at a time; after each, run the suite and have a **different agent** review
(read-only, no destructive git), then commit. Per CLAUDE.md.

---

## Target architecture (after the refactor)

- `State` — the model: `live` (key→value), `cur_pos`, `in_txn`/`txn_wrote`/`txn_readonly`/
  `txn_read_ts`, `live_snapshot`; plus the run counters (`ts`, `wseq`, `dirty`, `oldest_ts`,
  `last_advance_ts`, `n_positional`, `n_read_ts`, `n_iso_rc`, `n_iso_ru`). One object threaded
  through the ops. (Decide: does `State` also own the timestamp bookkeeping, or just the
  generation-facing model? Lean: model + counters together, since the ops mutate both.)
- `Op` functions — one per operation (`op_put`, `op_remove`, `op_pos_update`, `op_pos_remove`,
  `op_search`, `op_search_near`, `op_next`, `op_prev`, `op_reset`, `op_begin`, `op_commit`,
  `op_rollback`, `op_advance`, `op_evict`, `op_full_scan`). Each takes the context it needs
  (nodes, state, trace, rng) and performs the op + updates state. Each is single-responsibility.
- `WORKLOAD` config — the chosen structure (ask 1). Maps the current situation
  (mode + positioned) → op weights. Drives `pick_op`.
- `pick_op(state, rng, allow_writes)` — consults `WORKLOAD`, samples a legal op for the current
  `(mode, positioned, read_only)`, binds its argument, and returns a zero-arg callable.
- `run_sequence` — set up nodes + state, loop `n_ops`: `op = pick_op(...); op()`; close any open
  txn; (decide: final `verify`?). Just orchestration — no per-op dispatch logic.
- `compare_read` stays the always-on oracle (per read op). `verify`/`full_scan` becomes a weighted
  op (ask 4).

---

## Steps (checkboxes)

### S0. Plan + config-option exploration  ✅ in progress
- [x] S0.1 Assess the four asks (all sound; verify nuance noted).
- [x] S0.2 Spawn 3 config-design agents (position-break-first / read-write-split / flat-table).
- [ ] S0.3 Present the three config options + a recommendation; **Ivan chooses** the representation.
- [ ] S0.4 Decide the verify trade-off: full_scan weighted-only, OR also a final end-of-sequence
      verify as a safety net (recommended: keep one cheap end verify — position is moot at end —
      AND add the weighted in-chain full_scan). **Ivan decides.**

### S1. `State` class (ask 3) — behaviour-preserving
- [ ] S1.1 Introduce a `State` class holding `live`, `cur_pos`, the txn flags, `live_snapshot`, and
      the run counters; instantiate per sequence (and reset per sequence as today).
- [ ] S1.2 Replace the scattered `self.<x>` accesses in `mirror_write`/`apply_positional`/`_end_txn`/
      `advance`/`pick_op`/`run_sequence` with the `State` object. No behaviour change.
- [ ] S1.3 Suite green; coverage guards pass. **Review (agent A).** Commit.
- [ ] S1.R Revisit: is `State` the right boundary (model-only vs model+counters+timestamps)? Does it
      read more clearly than `self.*`? Note the decision.

### S2. One function per op + `pick_op` returns a callable (ask 2) — behaviour-preserving
- [ ] S2.1 Extract each op's effect (currently in `run_sequence`'s if/elif and the
      `mirror_write`/`apply_positional`/`_end_txn`/`advance`/`drain_ingest` helpers) into a
      single-responsibility `op_*` function taking `(nodes, state, trace, rng[, arg])`.
- [ ] S2.2 `pick_op` returns a bound zero-arg callable (op + its argument) instead of a
      `(name, arg)` tuple. `run_sequence`'s loop becomes `pick_op(...)()` + trace logging.
- [ ] S2.3 Keep the CURRENT weight logic for now (just restructured into callables) so behaviour is
      preserved; the config swap is S3.
- [ ] S2.4 Decide how the trace still records a readable op name (e.g., the callable carries a
      label, or `pick_op` returns `(label, callable)` and `run_sequence` logs the label).
- [ ] S2.5 Suite green; guards pass. **Review (agent B).** Commit.
- [ ] S2.R Revisit: are the op signatures uniform enough? Any op still doing two jobs?

### S3. Workload-shape config (ask 1) — behaviour-preserving (roughly)
- [ ] S3.1 Implement the **chosen** config structure (from S0.3) as a module-level/class constant.
- [ ] S3.2 Rewrite `pick_op` to consume the config (filter legal ops by mode/positioned, sample by
      weight, bind arg). Remove the scattered if/elif weight branches.
- [ ] S3.3 Tune the example weights so the workload distribution stays roughly as today (verify via
      a quick op-histogram over the 10 seeds; coverage guards still pass).
- [ ] S3.4 Suite green; guards pass. **Review (agent C).** Commit.
- [ ] S3.R Revisit: is the config readable/tunable? Does adding a new op require touching only the
      config + one `op_*` function? (That's the success test.)

### S4. `verify` → weighted `full_scan` op (ask 4) — behaviour change
- [ ] S4.1 Add `op_full_scan` (reset + full layered-vs-reference scan + leader-vs-follower scan,
      i.e. today's `verify` body) as a weighted, position-breaking op in the config.
- [ ] S4.2 Make per-op `compare_read` the always-on oracle (already is). Per S0.4: keep/remove the
      mandatory end-of-sequence `verify`.
- [ ] S4.3 Add a coverage guard that `full_scan` actually fired (`n_full_scan > 0`) in
      `assert_self_coverage`.
- [ ] S4.4 Suite green; guards pass. **Review (agent D).** Commit.
- [ ] S4.R Revisit: with full_scan weighted, is untouched-key divergence still caught often enough?
      (Measure how many sequences fire it; tune the weight or keep the end verify.)

### S5. Wrap-up
- [ ] S5.1 Update `layered_cursor_stress_test_architecture.md` (function map, the new config section,
      the op-function list) + regenerate the op-loop `.dot` (now config-driven, callable dispatch).
- [ ] S5.2 Full suite green; final independent review of the whole refactor.
- [ ] S5.3 Update the plan/CLAUDE status.

---

## Decisions (resolved 2026-06-11)
- **D1 (config):** **Hybrid — top-level `P_BREAK` dial selecting keep-vs-break, over a flat
  op-table whose rows carry legality tags.** Combines proposal #1's visible central dial with
  proposal #3's declarative/extensible tagged rows. Adding an op = one row + one `op_*` function.
- **D2 (verify):** **Weighted `full_scan` op + keep one final end-of-sequence `verify`** as a
  safety net (the always-on oracle remains per-op `compare_read`).
- **D3 (State scope):** **One `State` class** holding the model (`live`, `cur_pos`, txn flags) AND
  the run counters/timestamps. Connection-global fields persist across sequences; a
  `new_sequence()` resets the per-sequence fields.
