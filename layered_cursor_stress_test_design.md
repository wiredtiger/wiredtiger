# Layered Cursor Stress Test — Design (v3)

## Goal

Find subtle bugs in the follower layered cursor merge logic (`cur_layered.c`) by running
long, unusual-but-valid cursor operation sequences and checking results after every step.
The test must be compact and human-readable, yet exercise the merge state machine in ways
hand-written tests never would. Failures must be reproducible from a single integer seed.

---

## v3 — Current Direction (supersedes parts of v2 below)

This section is the live design. The v2 material below is retained for history; where it
conflicts, v3 wins.

### Big shift from v2: long-lived positioned cursors

v2 reset the cursor on every mutation (kept comparison simple). v3's core idea is the
opposite: **keep cursors positioned across long chains** that include writes, so we test
the cursor surviving update/remove/next/prev/transaction-switch without losing position.
Position-resetting ops (`insert` via set_key, `reset`) are deliberately **rare** (~1 in
100–500). This is where the real bugs live — the M3 finding came from exactly this shape.

Per-op position-retention semantics are documented in
`layered_cursor_position_semantics.md`. For the chain generator: ops that **keep** the
position (chain can continue with `next`/`prev`) are `search`, `search_near`, `next`, `prev`,
`update`, `modify`, `reserve`, and `remove` when already positioned (stays on the removed key,
KEY_INT, no value). Ops that **clear** the position (make these rare in a position-holding
chain) are `insert`, `remove` when unpositioned, `reset`, `largest_key`. Layered keeps the
`ITERATE_NEXT/PREV` flags across a positioned `update`/`modify`/`remove` so a write inside a
scan does not restart iteration.

### The oracle — plain reference table (ASC) vs layered table (DSC)

Per connection there are **two tables in one session**:
- **ASC** = a plain (non-layered) WiredTiger reference table.
- **DSC** = the layered table under test.

Each has one cursor. Every chosen op is applied to the layered (DSC) cursor, then the
reference (ASC) cursor, and the results (error code + key + value) are compared. Writes go to
both tables; transaction/timestamp ops apply at the session level (so both cursors share the
snapshot/read_timestamp). The same op sequence replicates to the follower connection, which
has its own ASC + DSC.

Why this works for the hard cases: a reference that stored only "current value per key" could
not answer reads under an explicit `read_timestamp` (that needs version history) — an
information-theoretic gap, not an implementation detail. But because ASC is a **real
WiredTiger table**, it handles `read_timestamp` / isolation / prepared transactions
**correctly by construction** — WT itself is the model. So we get the every-op independent
oracle with **zero hand-rolled MVCC**.

We also keep **leader-vs-follower** as a cheap second cross-check. Both layered cursors must
match their reference, so they transitively match each other; the value of having both is that
the leader's layered table holds data in *stable* while the follower's holds data in *ingest
+ stable* — different physical layouts checked against the same logical truth.

Scenarios (eviction, checkpoint advance) mutate only the layered table's *physical* state, not
the logical state, so the layered cursor must still match the unchanged reference afterward —
that is the test.

**Read-committed snapshot pinning (important for Phase A).** A follower's stable constituent
only advances to a new checkpoint when the session's snapshot is *released*; while it is pinned
(another cursor held open → `ncursors > 0`, or an explicit txn / `read_timestamp`), iteration
keeps the pre-advance view. This is correct read-committed behavior and was the root cause of
the (now-resolved) checkpoint finding. Consequences: compare the layered and reference cursors
in the **same session/snapshot**; a held cursor or open txn across a checkpoint advance keeps
the old view (not a mismatch). Pinned-snapshot-across-advance is one place where the layered
cursor and a plain table may legitimately diverge — surface such cases for domain judgment
rather than auto-failing.

### search_near tie-break alignment

`search_near` may legally return either immediate neighbor. When the reference and the
layered cursor return the two equidistant neighbors of a gap, step the lagging cursor by one
(`next`/`prev`) to realign — exactly one call. This replaces the v2 bracket-probe hack with
a cleaner rule.

### Non-fatal errors are normal — keep reusing the cursor

Error codes like `WT_NOTFOUND` and `WT_DUPLICATE_KEY` are **expected outcomes, not things to
avoid**. The generator produces them frequently and the **same cursor is kept open and reused**
afterward — a non-fatal error must leave the cursor in a clean, well-defined state.

- `search`/`next`/`prev` off the ends or on an absent key → `WT_NOTFOUND` (cursor ends
  unpositioned — a natural soft position-reset within a chain).
- `insert` with `overwrite=false` on an existing key → `WT_DUPLICATE_KEY`; the existing value
  is retrievable via `get_value` (`wiredtiger.h.in:475-476, 493-496`).
- `update` with `overwrite=false` on a missing key → `WT_NOTFOUND`.

The test (a) compares the **error code** layered-vs-reference (must agree), (b) for
`WT_DUPLICATE_KEY` also compares the **existing value** from `get_value` on both, and (c)
continues the chain on the same cursor and asserts subsequent ops behave identically. This is
distinct from `WT_PREPARE_CONFLICT` / `WT_ROLLBACK`, which require rolling back the
transaction before the cursor is reusable (handled in the transaction phase).

### Single-threaded, multi-session (NOT multi-threaded)

Reproducibility from a seed is the #1 requirement; multi-threading breaks it (nondeterministic
scheduling). test/model is single-threaded by design for this reason; test/format is
multi-threaded precisely because it does *not* do per-op comparison. Use **multiple sessions
in one thread** to get prepared-transaction conflicts deterministically (session A leaves a
prepared txn on key K; session B's cursor touches K → `WT_PREPARE_CONFLICT`).

### Scenario injections (deterministic, at seeded points)

Always start from **empty tables**; scenarios mutate size/shape dramatically:
- Evict 20/40/60/80/100% of the ingest content mid-cursor-life (`release_evict`).
- Remove-all via `remove()` (→ table full of tombstones on follower) vs via `truncate()`
  (fast-truncate path) — both, they hit different code.
- Bulk insert to suddenly grow the table.
- Prepared-transaction flood (several sessions leave prepared txns) to raise conflict odds.
- Checkpoint advance **mid-iteration** (the M3 finding territory).
- Adjacent-key insert next to the cursor position, then `next` (snapshot-visibility).
- Tombstone-byte-prefixed value (the `__clayered_deleted_encode` E-2 edge).

### Config matrix (seed-selected, combined and separate)

`overwrite` on/off (per cursor open), `bounds` on/off (via `bound()`, cleared on reset),
isolation level (snapshot / read-committed / read-uncommitted). Formats stay fixed
(`key_format=i`, `value_format=S`).

### OPEN PROBLEMS

Resolved: ASC = plain reference table, DSC = layered table (one cursor each, compare every
op); oracle = plain-reference primary + leader-vs-follower secondary; framework = Python now,
test/model later, never hand-roll MVCC; single-threaded + multi-session.

Still open (tracked with full detail in the plan file, §4):
1. **Reference-table placement.** Can a `role=follower` connection host a writable plain
   `table:` for its ASC reference? If not, fall back (reference on the leader connection, or a
   separate plain connection). Verify empirically in Phase A.
2. **Per-op position-retention semantics** must be confirmed before the long-lived-cursor
   chain generator is trustworthy.
3. **M3 finding triage** (see below) — investigate now / file ticket / fold into Phase D4.

### Status / where we are

Implemented (Python, `test/suite/test_layered_cursor_stress.py`), M0–M3 done and green:
leader-vs-follower per-op comparison, `search_near` bracket check, mirrored writes with
controlled ingest/stable split, deterministic eviction, 5 named wild scenarios, reproducible.
**One candidate bug found** (stale checkpoint on a positioned cursor — see
`finding_stale_checkpoint_cursor` and the skipped scenario). v3 reshapes this toward
long-lived positioned cursors + a plain-table reference oracle once the open problems are
resolved.

---

## Framework Decision: Python Test Suite

`test/format` (process-level follower, awkward per-op comparison) and `test/model`
(requires implementing cursor semantics in the C++ model first — a separate project) are
both deferred. The Python suite wins because the scaffolding already exists:

- `@disagg_test_class` + `DisaggConfigMixin` (`helpers/helper_disagg.py`) provide
  leader/follower connection setup and `disagg_advance_checkpoint()`.
- `test_layered_cursor01.py` already opens leader + follower sessions and compares them.
- `debug=(release_evict)` cursors work directly on a `layered:` URI
  (`helpers/helper_layered_fast_truncate.py:193`) — deterministic, synchronous eviction.
- `random.Random(seed)` gives exact reproducibility.

`test/model` remains the strong long-term target (true model oracle + built-in workload
shrinking); this Python test is the fast first iteration that reuses existing helpers.

---

## The Oracle: Leader vs Follower (validated against the code)

The code confirms the two roles run **different** paths:

| Operation | Leader | Follower |
|-----------|--------|----------|
| read of `K` | stable cursor only; ingest skipped (`cur_layered.c:671-675`, `1129-1133`) | merge(ingest, stable) |
| `insert/update/remove` | applied to stable (`cur_layered.c:313`, `805`) | applied to ingest |
| iteration | stable only | merge with direction tracking |

This makes the leader a **good** oracle, not a weak one: the leader never executes the merge
logic, so the subtle merge bugs we hunt cannot be silently shared by both sides. The only
shared surface is the plain stable-btree read path, which is already well tested. The leader
is the simple-path reference; the follower is the complex path under test.

(Note: an earlier inventory doc claimed "leader writes to ingest" — that is stale. The
`FIXME-WT-16810` comments and `__clayered_truncate_leader` confirm the leader operates on
stable and keeps ingest empty.)

### Why equality is valid: one logical dataset, two physical layouts

Every logical write is **mirrored to both connections**. It lands in different physical
places but produces the same logical state:

- Leader `insert(K,V)` → stable table (ingest stays empty).
- Follower `insert(K,V)` → ingest table.

Both connections therefore always hold the same logical data. A read must return the same
result on both — and since only the follower merges, any difference is a follower bug.

### Controlling the follower's ingest/stable split

The follower's stable table only gains data when the test explicitly calls
`disagg_advance_checkpoint(conn_follow)`. This is the lever that creates interesting merge
states:

- Write keys, **do not advance** → keys live only in follower ingest (pure-ingest case).
- Advance → the leader's checkpoint flows into follower stable; older ingest entries become
  logically redundant.
- Write fresh keys **after** advancing → follower now holds a genuine ingest + stable mix.

**Invariant maintained by the generator:** at least one key has been written since the last
advance, so the merge is always exercised and a checkpoint never logically empties the
follower's ingest contribution (satisfies the "checkpoint must not remove 100% of ingest"
requirement).

---

## Key/Value Format

`key_format='i'` (signed int), `value_format='S'`.

Integer keys make gaps, neighbors, and boundary cases trivial to construct. Keys are spread
with gaps (e.g. multiples of 10) so that `search_near` targets can fall *between* keys
(e.g. search 105 with keys 100, 110 present) to hit the interesting merge branches. Four
key zones are tracked for operation generation:

- `ingest_only` — written since last advance, present only in follower ingest.
- `stable_only` — checkpointed and advanced, present in stable on both sides.
- `both` — present in ingest and stable (overlap; ingest must win).
- `absent` — never written, or in a gap, or beyond both ends (before-first / after-last).

---

## Comparison Model

`apply(cursor, op)` returns a normalized tuple `(ret, key, value, cmp)`. `compare` has three
branches:

### Exact-result operations (`search`, `next`, `prev`, `reset`)

Plain tuple equality between leader and follower results. On `WT_NOTFOUND`, both must agree.

### `search_near` — adjacent-neighbor check, not "distance"

WT's `search_near` contract returns an **immediate neighbor** (largest-smaller *or*
smallest-larger), not necessarily the numerically closest. So requiring equal distance
would false-fail (search 50 with keys {40, 55}: leader may return 40, follower 55 — both
valid). The correct, model-free check:

- Exact match exists → both sides must return the identical key with `cmp == 0`.
- Key outside all data → both must clamp to the same end (first or last key).
- Otherwise the two returned keys must **bracket the gap**: they are adjacent in sort order
  with nothing strictly between them.

```python
def compare_search_near(self, kl, cmpl, kf, cmpf, log):
    if kl == kf:
        self.assertEqual(sign(cmpl), sign(cmpf))           # same key ⇒ same side
        return
    lo, hi = sorted((kl, kf))
    self.assertLess(lo, self.search_key)
    self.assertGreater(hi, self.search_key)                # bracket the search key
    self.probe.set_key(lo); self.probe.search()            # nothing strictly between
    self.assertEqual(self.probe.next(), 0)
    self.assertEqual(self.probe.get_key(), hi)
```

### Position re-sync after `search_near` (important)

`search_near` is the only operation whose *landing position* is legally nondeterministic
between the two implementations — either immediate neighbor is valid. If left unsynced, the
two cursors sit at different (both correct) keys and every following `next`/`prev` diverges
legitimately, producing false failures. After a found `search_near`, the harness repositions
the **leader** onto the **follower's** landed key via an exact search (the follower keeps its
authentic post-`search_near` state). This restores the invariant "both cursors agree on
position after every op" while still exercising the follower's search_near→iterate path.
(Discovered immediately in M1 — the comparison engine caught it on the first run.)

### On mismatch

Dump the full operation log (every op + both results) so the failure is a self-contained,
replayable script, then run the shrinker (below) before reporting.

---

## Deterministic Eviction and Reproducibility

A `debug=(release_evict)` cursor evicts the page under the cursor **synchronously, in the
test thread, targeted** — unlike non-deterministic cache-pressure eviction. It works on the
`layered:` URI directly. Therefore forced eviction is just another seeded operation in the
sequence and stays reproducible.

- Background eviction is suppressed by sizing the cache large (no pressure); the only
  evictions are the ones the test injects.
- **Caveat, stated honestly:** the background layered GC / drain threads
  (`__wti_layered_drain_ingest_tables`, gc pruning) run on their own timing, but they only
  remove ingest entries already made redundant by stable — they never change a query
  *result*. So **black-box result-equality is 100% reproducible.** Only the optional
  grey-box tier (asserting *which* constituent answered) would need GC quiesced.

---

## Operation Vocabulary

The seeded generator emits `(kind, arg)` ops, weighted to over-represent the hard paths.

| Op | Effect |
|----|--------|
| `search(K)` | exact lookup; K drawn from any zone |
| `search_near(K)` | nearest lookup; weighted toward `absent`/gap keys |
| `next` / `prev` | step; drives merge + direction tracking |
| `reset` | clear position and iteration flags |
| `insert(K,V)` / `update(K,V)` | mirrored write to both connections |
| `remove(K)` | mirrored delete (follower writes a tombstone to ingest) |
| `advance` | `disagg_advance_checkpoint` — moves follower ingest → stable |
| `evict(K)` | `release_evict` the page backing K on the follower |
| `begin/commit/rollback` | transaction boundaries (read_timestamp variants) |

---

## Wild Sequences (chosen by reading the implementation)

White-box knowledge selects *what to generate*; assertions stay black-box. The generator
over-weights these because the code analysis flagged them as silent-failure-prone:

1. **Direction reversal at exhaustion** — `next` past the last stable key, then `prev` back
   across the ingest/stable boundary (`__clayered_position_alternate`, `cur_layered.c:1020-1056`).
2. **Search breaks iteration** — `next`×k → `search(mid)` → `next`; ITERATE flags must be
   cleared (`cur_layered.c:1196-1202`).
3. **Opposite-side `search_near`** — search a gap between the last ingest key and the first
   stable key, so the constituents land on opposite sides (`cur_layered.c:1928-1941`).
4. **Checkpoint removes the positioned key** — position on K, then on the leader remove K +
   checkpoint + advance follower, then `next`/`prev` (`WT_NOTFOUND` fallback, `cur_layered.c:448`).
5. **Tombstone shadows stable** — K in stable, tombstone in follower ingest; `search(K)`
   must return `WT_NOTFOUND` (core invariant K-7).
6. **Evict-then-touch** — `evict(K)` then immediately `search(K)` / `next` (re-read of an
   evicted page mid-iteration).

---

## Metamorphic Self-Checks (no oracle)

These assert a property of a *single* cursor, catching bugs even when leader and follower
would agree. They are just sequences, so they cost almost nothing and are sprinkled in:

- **Round-trip:** `next` to end collecting keys, then `prev` to start → reverse list.
- **Involution:** `next` then `prev` returns to the same key; repeated stays stable.
- **Path independence:** `search(K)` + `next` lands on the same key as
  `search_near(K-ε)` + step.

---

## Shrinking (for debuggability)

When a long sequence fails, a greedy delta-debug pass drops ops one at a time, keeping any
subsequence that still reproduces the failure, then reports the minimal repro:

```python
def shrink(self, seed_state, log):
    changed = True
    while changed:
        changed = False
        for i in range(len(log)):
            cand = log[:i] + log[i+1:]
            if self.reproduces(seed_state, cand):
                log = cand; changed = True; break
    return log
```

Output: "seed 3829104 fails; minimal repro is these 4 ops: …".

---

## Test Structure (one knob: the seed)

The seed deterministically chooses the table split, the operation sequence, and the
eviction/advance points. "More coverage" = "more seeds" — no `make_scenarios` Cartesian
blow-up. A small set of named deterministic scenarios covers the known-hard cases for
readability and targeted debugging; the seed-driven fuzzer covers the long tail.

```python
def run_seed(self, seed):
    rnd = random.Random(seed)
    self.pr(f'SEED={seed}')                           # printed for reproduction
    self.setup_leader_follower()                      # 2 conns, 1 layered table
    keys = self.seed_initial_split(rnd)               # mirror writes; control advance
    lc = self.session.open_cursor(self.uri)           # leader: stable-only path
    fc = self.session_follow.open_cursor(self.uri)    # follower: merge path
    log = []
    for _ in range(rnd.randint(50, 300)):
        op = self.pick_op(rnd, keys)                  # weighted; keys from interesting zones
        log.append(op)
        rl = self.apply(lc, self.session, op)
        rf = self.apply(fc, self.session_follow, op)
        self.compare(op, rl, rf, log)                 # raises with full log on mismatch
```

---

## Tiers

1. **Black-box default tier** (this design) — asserts only observable results
   (return code, key, value, neighbor contract). Survives the cur_layered.c rework because
   it pins the *contract*, not internals.
2. **Grey-box tier** (separate, later, likely Catch2) — asserts which constituent
   `current_cursor` points to and which flags are set. Expected to be rewritten alongside
   the implementation, so it is kept out of the default suite.

---

## Out of Scope (deferred)

- Step-up / step-down during an active cursor (positioned-cursor role-change survival is a
  separate work item).
- Fast truncate (covered by project-level testing).
- Random cursor mode (`WT_CLAYERED_RANDOM`).
- `test/model` differential testing (model-as-oracle) — the strong long-term follow-on.
