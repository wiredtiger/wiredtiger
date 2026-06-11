# Bug: layered follower cursor blocks ALL iteration on any pending prepare in the ingest

**Status:** likely a real bug, fixable (clean fix is moderate-to-hard). Found by the
layered-cursor stress-testing effort; initially mis-triaged as "by-design," re-opened, and a deep
source analysis confirms the over-eager behavior is an implementation artifact, not a necessity.

**Component:** `src/cursor/cur_layered.c` (disaggregated follower layered cursor, ingest+stable merge).

---

## TL;DR

On a disaggregated **follower**, the layered cursor merges an in-memory **ingest** btree with a
**stable** checkpoint. If a **prepared (uncommitted) transaction** has *any* entry in the ingest, a
**forward (or backward) scan returns `WT_PREPARE_CONFLICT` on the very first `next()`/`prev()`** —
returning *none* of the committed keys, **even committed keys that sort before the prepared key and
are completely unaffected by it**. A plain (single-btree) table returns the committed keys first and
only conflicts when iteration actually *reaches* the prepared key.

So one pending prepared transaction in the ingest **blocks the entire scan of a layered table** on
the follower until that prepare commits or aborts.

**This is over-conservative, not necessary.** It never returns wrong data — it conflicts *too
early*. The conflict can be deferred to the prepared key's true sort position (matching plain-table
semantics) using a key-only positioning primitive that already exists in the engine. It's a
liveness/availability bug, not a correctness one.

---

## Reproducers

| file | what it shows | status |
|---|---|---|
| `test/suite/test_layered_prepare_iterate_diff.py` | canonical 2 cases: layered (1st `next()` conflicts) vs plain (returns `1`, then conflicts at `2`) | committed, green |
| `findings/repro_prepare_iterate_worst_case.py` | worst case: prepared key sorts **after** all committed keys, which are still all blocked | committed, green |
| `findings/repro_prepare_iterate_layered_vs_plain.py` | prepared **remove** of a middle key + post-rollback resume corollary | committed, green |

### Minimal & worst case

| | committed (stable) | prepared (ingest, separate txn) | layered forward scan | plain forward scan |
|---|---|---|---|---|
| minimal | `1` | `2` | 1st `next()` → **CONFLICT** | `1`, then CONFLICT at `2` |
| worst | `1, 2, 3` | `9` | 1st `next()` → **CONFLICT** (none of 1,2,3) | `1, 2, 3`, then CONFLICT at `9` |

### How to run

```
cd build
python3 ../test/suite/run.py test_layered_prepare_iterate_diff
# worst case (run.py discovers test/suite only):
cp ../findings/repro_prepare_iterate_worst_case.py ../test/suite/ && \
  python3 ../test/suite/run.py repro_prepare_iterate_worst_case ; \
  rm ../test/suite/repro_prepare_iterate_worst_case.py
```

---

## Mechanism (file:line)

A fresh forward walk enters `__clayered_iterate_constituents` (`cur_layered.c:1114`). With both
constituents unpositioned it takes the `fresh_start` branch (`:1156`) and positions the **ingest**
constituent **first** (`:1159`), then stable (`:1160`):

```c
if (fresh_start) {
    WT_ERR_NOTFOUND_OK(__clayered_constituent_iter_helper(clayered, c_ingest, forward), false); // :1159
    WT_ERR_NOTFOUND_OK(__clayered_constituent_iter_helper(clayered, c_stable, forward), false); // :1160
    goto done;
}
```

`__clayered_constituent_iter_helper` (`:1073`) calls the constituent's `next()` → `__wt_btcur_next`
→ `__cursor_row_next`. With ingest `{9: prepared}`, the first entry is the prepared key; positioning
on it calls `__wt_txn_read_upd_list_internal` (`txn_inline.h:1502`), which finds the prepared update
and returns `WT_PREPARE_CONFLICT` (`txn_inline.h:1599`). `WT_ERR_NOTFOUND_OK` (`error.h:109-110`)
only swallows `WT_NOTFOUND`, so the conflict propagates immediately — **the stable cursor at `:1160`
is never positioned.** The fresh-start error path (`:1225-1230`) resets the cursors so a retry
restarts cleanly, and returns the conflict. The merge never learns that stable holds keys sorting
before `9`. The `:1164-1167` comment ("the cursor walk must be blocked by a prepared conflict on the
ingest cursor") documents this as expected — see *Intent* for why that's incidental.

---

## What is and isn't affected

- **Iteration (next/prev)** on the follower: blocked wholesale (the bug). Symmetric for `prev`.
- **Point search** of a *non-prepared* key: NOT affected — `__clayered_lookup` (`:1685`) searches
  the *exact* key in ingest, which returns `WT_NOTFOUND` for `9` when searching `1`, then falls
  through to stable. `search("1")` → `0`. (`search("9")` of the prepared key itself does conflict,
  matching plain-table.)
- **Leader** (reads stable only, ingest cursor nulled at `:1133-1134`): behaves like a plain table.

---

## Why it's deferrable (the by-design premise is refuted)

The earlier "by-design / architecturally unavoidable" rationale was: *"you cannot position a cursor
onto a prepared update without conflicting, so the merge can't order it against stable."* **The code
refutes this:**

- WiredTiger has **`WT_CURSTD_KEY_ONLY`** (`wiredtiger.h.in:796`): both iteration paths return the
  entry's **key before** the value/visibility (prepare) check — `bt_curnext.c:324-325` (insert
  list) and `:377-380` (on-page slot); mirrored in `bt_curprev.c`. Under `KEY_ONLY`, `next()` sets
  `WT_CURSTD_KEY_INT` but **not** `WT_CURSTD_VALUE_INT` (`bt_curnext.c:826-829`) and never resolves
  the prepared value, so it **does not conflict**.
- This primitive is already used to position on a prepared entry's key without conflicting:
  `__wt_btcur_search_prepared` (`bt_cursor.c:582-610`) — *"Set the key only flag ... we don't want
  to check visibility ... This short circuits validity checking"* — and `__curfile_largest_key`
  (`cur_file.c:1006-1048`).
- **Empirical proof the merge can traverse the ingest correctly:** an `ignore_prepare=true` reader
  (`txn.c:719-723`; skips the prepared update at `txn_inline.h:1592-1597` instead of conflicting)
  iterates the layered follower and returns the full correct result — `{1,2,3}` with prepared `9`
  → returns `1,2,3` then NOTFOUND; with prepared remove of `2` → returns `1,2,3`. So the ordering
  information the merge needs is reachable; only the default value-resolving positioning forces the
  early conflict.

So the merge *could* peek the ingest key (key-only), return stable keys that sort before it, and
resolve the prepared value (taking the conflict) only when it's about to emit the prepared key —
i.e. conflict at the key's true sort position, exactly like a plain table.

---

## Correctness vs liveness

**No correctness exposure — strictly an over-eager (false-early) conflict.** I found no case where
the layered cursor returns wrong data or violates ordering/visibility.

- **Prepared INSERT of a new key** (headline case): deferral is *provably* safe. The prepared key
  exists in no committed view, so it shadows/reorders nothing. Returning the committed prefix first
  is a valid prefix of every outcome (commit → `1,2,3,9`; abort → `1,2,3`).
- **Prepared in-place update / remove of an existing stable key** (e.g. prepared tombstone on `2`):
  also sound to defer — return `1` (committed, unaffected), and conflict *at* `2` (the merge legit
  cannot decide whether `2` is present until the prepare resolves). That's exactly the plain-table
  behavior. The current bug just moves the conflict *earlier* than `2`'s sort position.

The eager conflict is **never necessary for correctness**; it only costs progress.

---

## Intent — incidental, not a deliberate decision

- `arch-disagg-layered-cursor.dox` / `arch-disagg-layered.dox`: **zero** mentions of prepare or this
  semantic.
- **WT-17257** (`eda3e6b42d`) added **tests only** (`test_layered_prepare01.py`), freezing the
  first-`next()` conflict into a regression whose actual subject is *post-rollback recovery* — not a
  blessing of the eager semantics.
- **WT-17652** (`f282f4d7c0`) *changed* fresh-start ordering from **stable-first to ingest-first**.
  The prior code explicitly tried to "move the stable cursor first ... even if a prepared conflict
  occurs on the ingest cursor" — i.e. an *attempt at deferral* — but it tripped a snapshot-generation
  assertion on retry, so the fix flipped to ingest-first + reset-on-conflict to fix that assertion,
  not to choose eager-conflict semantics.
- **WT-17014** (`24e44c7d53`) was damage control around mid-walk conflicts (crashes / out-of-order),
  not a decision to conflict early.

The behavior is an artifact of the merge implementation (and an abandoned stable-first attempt),
retroactively rationalized as by-design — on a premise (`KEY_ONLY`) the code itself contradicts.

---

## Impact (MongoDB secondary / follower)

A single pending prepared transaction in a follower's ingest blocks **every** forward/backward scan
of that layered table from making any progress until the prepare resolves. It is **not** a
data-correctness issue (no wrong data; already-returned keys are committed and stable under both
outcomes). It is a **liveness / availability / latency** concern for scan-shaped reads (range reads,
collection scans, index-bound scans), disproportionate to the conflict surface: the prepare may
touch one key but stalls scans over the whole table, including keys it can never affect. Point
lookups of unrelated keys are unaffected, limiting blast radius to scans. Mitigation today:
`ignore_prepare=true`/`force` skips prepares (verified), but that *discards* prepare-honoring
semantics and is not parity.

---

## Verdict

**Likely a real bug, fixable; the clean fix is moderate-to-hard.** Over-conservative, diverges from
plain-table semantics with no correctness justification, deferral is provably sound, and the
required primitive (`WT_CURSTD_KEY_ONLY`) already exists and is demonstrably sufficient. Not
by-design — the rationale in the current tests rests on a premise the engine refutes.

### What a fix would look like
1. **Key-only peek** the ingest constituent in `__clayered_iterate_constituents` fresh-start /
   positioning paths (`cur_layered.c:1156-1162`) — key known, no conflict.
2. **Defer the conflict to emit:** return the stable key while it sorts strictly before the
   key-only ingest key; only when about to emit the ingest entry, clear `KEY_ONLY` and resolve its
   value, taking `WT_PREPARE_CONFLICT` then — at the correct sort position.
3. **State-machine plumbing:** teach `__clayered_get_current` (`:652`), `current`/`alternate`
   selection, and the tombstone-skip loop (`__clayered_iterate_int:1257-1264`) to handle an ingest
   constituent positioned-by-key/value-unresolved; preserve the WT-17652 no-inconsistent-state fix.
4. **Tests:** loosen `test_layered_prepare01` `middle`/`last` and `test_layered_prepare_iterate_diff`
   to assert plain-table-equivalent prefixes; add prev-symmetry and prepared-in-place-update cases.
5. **Interim option:** if the merge rework is too risky near-term, document `ignore_prepare`/`force`
   as the supported follower-scan mitigation and track the deferral as a follow-up.

### Risks / unknowns
- Layered iterate state machine is delicate around prepare conflicts (WT-17014/17454/17652 are all
  conflict-state fixes); a "key-known/value-unresolved" state risks new inconsistent-state bugs on
  direction change, checkpoint-advance mid-scan, and the truncate-list reposition path (`:744`).
- `KEY_ONLY` position pins the ingest page (`ref != NULL`) without a resolved value; the fresh-start
  check keys off `ref == NULL` (`:1156`) and must be reconciled with leaving ingest key-positioned.
- Prepared in-place update/remove must conflict *at* the key (not skip it and expose the stale
  stable value); needs a careful test matrix.
- Apply symmetrically to `prev` and column-store next/prev append paths.
- Do not conflate the fix with `ignore_prepare`: the fix preserves prepare-honoring semantics;
  `ignore_prepare` discards them.

---

## Draft Jira ticket

> **Summary:** Layered (disaggregated follower) cursor blocks all iteration when any prepared
> transaction is pending in the ingest
>
> **Type:** Bug · **Component:** Cursor / Disaggregated storage · **Assigned Teams:** Storage Engines
>
> **Description:**
> On a disaggregated follower, the layered cursor merges the in-memory ingest btree with the stable
> checkpoint. If a prepared (uncommitted) transaction has any entry in the ingest, a forward or
> backward scan returns `WT_PREPARE_CONFLICT` on the FIRST `next()`/`prev()` and returns none of the
> committed keys — even committed keys in stable that sort before the prepared key and are unaffected
> by it. A plain (single-btree) table returns the committed keys in order and only conflicts when
> iteration reaches the prepared key. Effect: one pending prepare in the ingest blocks the entire
> scan of a layered table on the follower until it commits/aborts. Point lookups of other keys, and
> all leader access, are unaffected.
>
> **Not a correctness issue** — no wrong data; the conflict is over-eager. It is a liveness/
> availability concern for follower scan workloads.
>
> **Repro:** `test/suite/test_layered_prepare_iterate_diff.py` (layered vs plain) and
> `findings/repro_prepare_iterate_worst_case.py` (prepared key sorts after all committed keys, which
> are still blocked). Both assert the divergent behaviour and pass today.
>
> **Mechanism:** `cur_layered.c:1156-1162` fresh-start positions the ingest constituent first; the
> ordinary btree `next` path resolves the prepared update's visibility and raises `WT_PREPARE_CONFLICT`
> (`txn_inline.h:1599`), which propagates (not swallowed by `WT_ERR_NOTFOUND_OK`) before the stable
> cursor is positioned.
>
> **Fixable:** the conflict can be deferred to the prepared key's true sort position. WT already has
> key-only positioning (`WT_CURSTD_KEY_ONLY`, used by `__wt_btcur_search_prepared` and `largest_key`)
> that yields a prepared entry's key without resolving its value/conflicting; an `ignore_prepare=true`
> reader iterates the layered table correctly today, proving the ordering info is reachable. The cost
> is reworking the layered merge state machine (moderate-to-hard), not new engine capability.
>
> **History:** WT-17652 changed fresh-start from stable-first to ingest-first (abandoning a deferral
> attempt to fix an unrelated snapshot-generation assertion); WT-17257 froze the first-`next()`
> conflict into a regression. So the eager behaviour is incidental, not a designed semantic.
