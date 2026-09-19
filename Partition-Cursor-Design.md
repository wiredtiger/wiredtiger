# Partition Cursor: Positioning a Cursor by Normalized Position

## 1. Goal

Expose the internal "normalized position" machinery (`src/btree/bt_npos.c`) through two new
`WT_CURSOR` methods so an application can:

1. Ask where a positioned cursor is, as a fraction of the whole object.
2. Move a cursor to a given fraction of the object without knowing any key.

The first consumer is the server's collection truncate markers (oplog, change collections,
pre-images), which today oversample the collection with a random cursor at startup. With this
API the server reads exactly N positions at 1/N steps, in key order, with no sort and no
fallback to a full scan. Further consumers are progress reporting for long scans and any
future "split this collection into K ranges" feature such as resharding or parallel scans.

Design constraints:

- Positioning is a hot-path call for some consumers. Selecting behaviour must not require
  parsing a config string on every call.
- MongoDB vendors WiredTiger and compiles it in, so the library and the header always match.
  The API can therefore grow by appending struct fields without a coordinated server change,
  and needs no struct size or version field.
- The current position metric is one of several plausible metrics. The API must not bake it
  in as the only one, but must not ship constants for metrics that do not exist yet.
- No support for deprecated features (column-store objects) unless it falls out of a cleaner
  general implementation.

## 2. Background

### 2.1 Normalized position internals

`__wt_page_npos(session, ref, start, ...)` walks from a leaf ref to the root. At each level it
computes `(slot + remainder) / entries`, where `remainder` starts as the caller-supplied
`start` fraction inside the leaf. The result is clamped to [0, 1]. Every leaf owns a
contiguous, non-overlapping sub-range of [0, 1], and the ranges are ordered like the keys.

`__wt_page_from_npos(session, &ref, npos, read_flags, walk_flags)` does the reverse: from the
root, multiply the position by the child count, descend into the integer part, continue with
the fractional part. It returns a leaf ref with a hazard pointer held. If the exact leaf is
unusable (deleted, locked, or not in memory in cache-only mode), `__find_closest_leaf` walks
to the nearest usable leaf in the requested direction and reports how many pages it visited.
Two wrappers fix the flags for the two existing modes: `__wt_page_from_npos_for_read` (may
read pages from disk) and `__wt_page_from_npos_for_eviction` (`WT_READ_CACHE`, never reads,
never waits).

The fractional remainder at the leaf is currently discarded on descent, and is a fixed
constant on ascent. Refining to a key within the page is a small extension: on ascent use
`(slot + 0.5) / page->entries`; on descent use `floor(remainder * page->entries)`.

Current callers: the eviction walk (saves and restores its walk point per tree) and
rollback-to-stable progress reporting.

Precision: positions are exact only for a perfectly balanced tree. Real trees give a
monotonic but non-uniform mapping. Positions drift as pages split, merge, or are evicted and
re-read, and the in-memory tree shape differs from the on-disk shape. Positions are therefore
"soft": valid for sampling and progress, never for durable pointers.

### 2.2 What is available without reading a leaf

A hazard pointer can only be set on a ref in `WT_REF_MEM` state, and without one a ref pointer
is not stable across splits. So a cursor cannot be pinned to a page that is not in memory.
What the tree does offer without a leaf read is the leaf's separator key: the parent internal
page stores one per child, obtainable with `__wt_ref_key` while the parent is held. Separator
keys have three properties that matter here:

- Leaf-level separators are suffix-truncated by reconciliation
  (`__rec_split_row_promote` in `src/reconcile/rec_write.c`). A separator is greater than the
  last key of the preceding leaf and not greater than the first key of its own leaf, but it is
  usually not equal to any record.
- Slot 0 of a row-store internal page carries no usable key. The leftmost child inherits its
  parent's separator, recursively; the leftmost leaf of the tree has the empty key, meaning
  "before everything".
- A separator may be an overflow item. Instantiated ones are in memory; an on-page overflow
  ref key needs one overflow block read (`__wt_ref_key_onpage_ovfl` path).

A separator is exactly what a truncate marker needs: a boundary that aligns with a page edge so
fast truncation applies. Reaching a real record from it is one `search_near`.

### 2.3 Server consumer today

`WiredTigerRecordStore::RandomCursor` (`wiredtiger_record_store.cpp`) opens a `next_random`
cursor and exposes `next()`. `CollectionTruncateMarkers::createMarkersBySampling`
(`collection_truncate_markers.cpp`) draws `kRandomSamplesPerMarker * numMarkers` samples,
sorts them by RecordId, and keeps every k-th sample as a marker boundary. It falls back to a
full collection scan when it cannot get enough samples.

With positioning, that loop becomes: for i in 1..N, position at i/N, read the key. Samples
arrive in order, at most one leaf read each, deterministic. In key-only mode there is no leaf
read at all.

### 2.4 Existing API precedents

| Pattern | Example | Cost to add |
|---|---|---|
| New `WT_CURSOR` method | `largest_key`, `bound`, `get_raw_key_value` | struct slot in every cursor type, table/layered wrappers, Python binding |
| Open-time config that swaps `next` | `next_random`, `next_random_sample_size` | config parsing in file/table/layered open |
| Dedicated URI | `statistics:`, `backup:` | new data source |

New methods are chosen because they compose with every other cursor operation: after
positioning, `next`, `prev`, `get_key`, `get_value`, transactions, and checkpoint cursors all
work unchanged. Section 9 covers the alternatives.

Two API facts shape the argument design. The config parser has no floating-point type (only
int, boolean, string, list, format), so a fractional position cannot travel in a config
string. And `WT_MODIFY` is the precedent for passing a caller-filled struct by pointer.

## 3. API

### 3.1 Signatures

```c
/*
 * Position the cursor at position `position->pos` in the object. Returns 0 with the cursor
 * positioned (or, in key-only mode, with the key set and the cursor unpositioned), or
 * WT_NOTFOUND with the cursor reset.
 */
int set_position(WT_CURSOR *cursor, WT_POSITION *position);

/*
 * Return the position of the record the cursor is positioned on.
 * The cursor must be positioned; otherwise EINVAL.
 */
int get_position(WT_CURSOR *cursor, double *posp);
```

Both methods sit next to `largest_key` in `struct __wt_cursor`. Adding methods changes the
public struct layout, which WiredTiger does regularly (`largest_key`, `get_raw_key_value`,
`bound` were all added this way).

`get_position` is deliberately a plain function: it has one input (the cursor state) and one
output, and no extension is expected. Everything for `set_position` other than the cursor
travels in the struct, so the signature never changes again.

### 3.2 The `WT_POSITION` struct

```c
struct __wt_position {
    double pos;             /* In: requested position in [0, 1]. */
    uint32_t flags;         /* In: WT_POSITION_* flags; 0 selects all defaults. */
    uint32_t pages_skipped; /* Out: leaf pages skipped to find a usable page; 0 means the
                               target page itself was used. */
    /*
     * Future fields are appended here as needed: a config string for rare options, a metric
     * selector (bytes, record count), a walk limit, the actual position reached.
     */
};
```

Rules:

- A zero-initialised struct is the default behaviour. Every field added later must keep that
  property, so a caller fills in only what it needs.
- Only fields that do something in the current version exist. Extension points are described
  in a comment, not reserved as dead fields or constants.
- Hot-path options are flag bits. An option that needs a value gets its own field.
- Output fields are written on success and on `WT_NOTFOUND`; they are unspecified after other
  errors.

### 3.3 Flags

32-bit, prefixed `WT_POSITION_`. The anchor is a two-bit field; the rest are single bits.

```c
#define WT_POSITION_CACHE_ONLY    0x01u  /* Never read from disk, never wait for a locked page. */
#define WT_POSITION_KEY_ONLY      0x02u  /* Set the page boundary key; do not read the leaf. */
#define WT_POSITION_PREV          0x04u  /* Walk backwards to a visible record; default forwards. */

#define WT_POSITION_ANCHOR_EXACT  0x00u  /* Default: the page remainder selects the slot. */
#define WT_POSITION_ANCHOR_FIRST  0x10u  /* First slot on the page. */
#define WT_POSITION_ANCHOR_MIDDLE 0x20u  /* Middle slot on the page. */
#define WT_POSITION_ANCHOR_LAST   0x30u  /* Last slot on the page. */
#define WT_POSITION_ANCHOR_MASK   0x30u
```

Validation is permissive. Bits the library does not know are ignored, as are bits that have
no effect for the requested operation (for example the anchor in key-only mode). The only
rejected inputs are ones that indicate a caller bug: a NaN position (`EINVAL`), and the
cursor-state conflicts listed in section 6. This keeps a server built against a newer header
working against an older library, at the cost of silently getting default behaviour for the
unknown bits.

Metrics other than normalized position (bytes, record count, absolute or normalized) are a
documented extension point, not a flag field. They need per-child subtree aggregates on
internal pages that the tree does not maintain today (a `WT_REF` knows only its own block's
size), which is a separate project. When it lands, a metric selector field is appended to the
struct, and the descent picks the child whose cumulative aggregate covers `pos`.

### 3.4 Common uses

**Rebuilding truncate markers at startup.** The server needs N boundary keys, in order, and
wants page-aligned boundaries so fast truncation applies. Key-only mode gives that without
reading a single leaf page:

```c
WT_POSITION p = {0};
p.flags = WT_POSITION_KEY_ONLY;
for (i = 1; i < n_markers; ++i) {
    p.pos = (double)i / n_markers;
    if ((ret = cursor->set_position(cursor, &p)) == WT_NOTFOUND)
        break;                              /* Empty object. */
    WT_ERR(ret);
    WT_ERR(cursor->get_key(cursor, &key));  /* Boundary key for marker i. */
    /* Copy the key out; the cursor is not positioned and holds no page. */
}
```

**Sampling N records with their values.** Replace a random cursor with N evenly spaced reads.
Each call reads at most one leaf page and the results arrive in key order:

```c
WT_POSITION p = {0};
for (i = 0; i <= n; ++i) {
    p.pos = (double)i / n;
    WT_ERR_NOTFOUND_OK(cursor->set_position(cursor, &p), true);
    WT_ERR(cursor->get_key(cursor, &key));
    WT_ERR(cursor->get_value(cursor, &value));
    if (p.pages_skipped != 0)
        /* The addressed page was unusable; this sample is a few pages off. */;
}
```

**Progress reporting during a full scan.** A long `next` loop reports how far it has gone
without counting records up front:

```c
while ((ret = cursor->next(cursor)) == 0) {
    ...
    if (++count % 100000 == 0) {
        WT_ERR(cursor->get_position(cursor, &pos));
        report_progress(pos * 100.0);
    }
}
```

**Splitting an object into K ranges for parallel work.** Key-only positions give K-1 boundary
keys; each worker then opens its own bounded cursor between two neighbours:

```c
p.flags = WT_POSITION_KEY_ONLY;
for (k = 1; k < K; ++k) {
    p.pos = (double)k / K;
    WT_ERR(cursor->set_position(cursor, &p));
    WT_ERR(cursor->get_key(cursor, &boundary[k]));   /* Copy out. */
}
/* Worker k: lower bound boundary[k] (inclusive), upper bound boundary[k + 1] (exclusive). */
```

**Sampling only what is in cache.** A diagnostic or a load-shedding heuristic can look at
resident data without causing I/O:

```c
p.flags = WT_POSITION_CACHE_ONLY;
p.pos = 0.5;
ret = cursor->set_position(cursor, &p);   /* WT_NOTFOUND if nothing near 0.5 is in memory. */
```

**Resuming a best-effort background walk.** Save `get_position` when pausing, and later
`set_position` followed by `next` to continue. Because positions are soft, a few records
near the pause point may be seen twice or skipped after concurrent splits; this idiom suits
statistics gathering and compaction-style passes, not anything that needs exactly-once
visiting.

## 4. Semantics

### 4.1 `set_position`, default mode

1. Validate cursor state (section 6) and `pos`.
2. Reset the cursor, releasing any current page. Set `pages_skipped` to 0.
3. Clamp `pos` to [0, 1]. Values below 0 mean the first page, above 1 the last page,
   matching the internal contract.
4. Descend with `__wt_page_from_npos_for_read`, or `__wt_page_from_npos_for_eviction` when
   `CACHE_ONLY` is set, passing `WT_READ_PREV` when `PREV` is set. This yields a leaf ref with
   a hazard pointer, or `NULL` if the tree has no usable leaf. If `__find_closest_leaf` had to
   walk, its page count is stored in `pages_skipped`.
5. Pick the starting slot from the anchor: `0`, `entries - 1`, `entries / 2`, or
   `floor(remainder * entries)` for `EXACT`, where `remainder` is the fraction left after the
   descent and `entries` is the count of on-disk cells on the page. Insert-list records are not
   indexed (the same simplification the random cursor makes for its disk sampling); they are
   still returned by the walk in step 6.
6. Walk from that slot in the requested direction to the first visible record, using the
   existing `__wt_btcur_next` / `__wt_btcur_prev` machinery so visibility, prepared updates,
   and deleted pages are handled exactly as in iteration. The walk may leave the page. Pages
   crossed by this walk are not counted in `pages_skipped`.
7. Return 0 with the cursor positioned and key and value set. If the walk reaches the end of
   the tree, return `WT_NOTFOUND` with the cursor reset.

Anchor and direction are orthogonal: the anchor selects a starting slot, the direction selects
where to look for a visible record from there. `ANCHOR_LAST` without `PREV` often returns the
first record of the following page when the last slot is not visible. "Last visible record on
this page" is `ANCHOR_LAST | PREV`. The documentation shows both idioms.

Cache-only mode confines steps 4 and 6 to pages already in memory. The descent stops at the
deepest in-memory ref and the closest-leaf walk uses `WT_READ_CACHE`. The walk in step 6 must
not read pages either, which needs a btree-cursor flag consulted by the next/prev tree walk for
the duration of the call. Running out of in-memory pages returns `WT_NOTFOUND`.

Transactions: the call reads under the session's current snapshot like any read, so the
returned record is visible to the transaction. Checkpoint cursors position within the
checkpoint's tree via the same `WT_WITH_CHECKPOINT` wrapper `largest_key` uses.

### 4.2 `set_position`, key-only mode

With `KEY_ONLY` the call never touches a leaf page:

1. Descend through internal pages as in 4.1, but stop when the next ref to descend into is a
   leaf. Along the way remember the deepest ref that was reached through a non-zero slot; its
   separator key is the boundary for the target leaf. If every slot on the path was 0 the
   boundary is the empty key.
2. Copy that key into the cursor's key buffer while the hazard pointer on the parent is still held, then release the parent. An on-page overflow ref key is read through the
   existing overflow helpers.
3. Mark the key as externally set (`WT_CURSTD_KEY_EXT`) and leave the cursor unpositioned.
   This is the state `set_key` produces, so the natural next call is `search_near`, `search`,
   or a range truncate using the cursor as a bound.
4. `pages_skipped` is always 0: the leaf's state is irrelevant to its separator, so there is
   no closest-leaf walk.

The anchor and `PREV` have no effect in this mode. `CACHE_ONLY` applies to the internal pages:
if an internal page on the path is not in memory the call returns `WT_NOTFOUND`.

Cost: internal-page descent only (internal pages are a small fraction of the tree and almost
always cached), plus one key copy into a buffer the cursor already owns. No I/O in the common
case, no value, no per-call allocation once the key buffer has grown to the largest key seen.

### 4.3 `get_position`

1. Validate: cursor positioned (`WT_CURSOR_IS_POSITIONED`), otherwise `EINVAL`. A cursor in
   the key-only state after `set_position` is not positioned and fails the same way.
2. Compute the leaf remainder as `(slot + 0.5) / entries`. A record on an insert list uses the
   slot of the on-disk cell it is attached to.
3. Call `__wt_page_npos` with that remainder and return the clamped result.

Round trip: with an unchanged tree, `set_position` with the value returned by `get_position`
lands on the same on-disk slot because `floor(((slot + 0.5) / entries) * entries) == slot`.
Records on insert lists round-trip to the nearest on-disk cell and then walk forward, so they
may land on a neighbour. This is documented.

Ordering: for any two positioned cursors on an unchanged tree, key order implies position
order (non-strict). This is the property sampling relies on and the property the tests check.

### 4.4 Precision reporting

`pages_skipped` tells the caller whether it got the page its position addressed. Zero means
yes. Non-zero means the addressed page was deleted, locked, or (in cache-only mode) not in
memory, and the result is that many leaf pages away in the walk direction. Callers that care
can call `get_position` on the result and compare. Returning the reached position directly
from `set_position` is a natural future output field.

## 5. Cursor types

| Cursor | Support | Notes |
|---|---|---|
| `file:` row-store | full | primary implementation in the btree layer, wrapped in `cur_file.c` |
| `file:` column-store | `ENOTSUP` | deprecated; rejected at the API boundary when the cursor has a record-number key |
| `table:` single colgroup | full | delegate to the primary, as `largest_key` does |
| `table:` multiple colgroups | full | position the primary, then `search` the other colgroups by the returned key, mirroring `__curtable_search_near`; key-only mode delegates to the primary and sets the table key |
| `layered:` | full, one constituent | see below |
| checkpoint cursor | full | positions in the checkpoint tree |
| dump, raw | full | pass-through wrappers |
| `index:` | `ENOTSUP` | feasible later by positioning the index btree then fetching the primary |
| statistics, backup, metadata, log, config, version, history store, prepared discover | `ENOTSUP` | stubs |

### 5.1 Layered tables

Positions are defined over one constituent. `set_position` uses the stable constituent when the
table has one, otherwise the ingest constituent. Content present only in the other
constituent is not part of the position space. After the constituent call returns, the layered
cursor takes the same post-positioning path it takes after `search_near` on that constituent,
so `next` and `prev` continue correctly. Key-only mode delegates in the same way and sets the
layered cursor's key.

`get_position` on a layered cursor:

- If the stable constituent is positioned, return its position.
- Else if the ingest constituent is positioned and a stable constituent exists, `search_near`
  the current key in the stable constituent, take that record's position, and reset the stable
  constituent so the layered cursor's state is unchanged. An empty stable table returns
  `WT_NOTFOUND`.
- Else (no stable constituent) return the ingest constituent's position.

This needs a new `WTI_CLAYERED_MODE_POSITION` operation mode alongside
`WTI_CLAYERED_MODE_LARGEST_KEY`.

## 6. Interactions and limits

- Bounds set on the cursor: `EINVAL`, as for `largest_key`. Supporting bounds means clamping
  the located record into the bounded range and stopping the walk at the bound; that is a
  natural follow-up once the base call exists.
- `next_random` cursors: `EINVAL`; they replace `next` and are single-purpose.
- Bulk cursors: `EINVAL`.
- Record-number keys (column-store): `ENOTSUP`.
- Empty tree: `WT_NOTFOUND`.
- Tree whose only pages are deleted: `__find_closest_leaf` walks past them; `WT_NOTFOUND` if
  nothing remains. Key-only mode still returns a boundary key.
- Concurrent splits: the descent restarts from the root on `WT_RESTART`, as today.
- Cost: one root-to-leaf descent plus at most one leaf read in the default mode; internal
  pages only in key-only mode. The closest-leaf walk is unbounded and, in the worst case (a
  long run of deleted or locked pages), proportional to tree width; the existing
  `npos_read_walk_max` statistic records it, and `pages_skipped` reports it per call. A walk
  limit is a possible future struct field.

## 7. Implementation plan

New and changed files, in build order:

1. `dist/stat_data.py`: `cursor_set_position`, `cursor_get_position`,
   `cursor_set_position_error`, `cursor_get_position_error`.
2. `src/include/wiredtiger.h.in`: `struct __wt_position` and its typedef, `WT_POSITION_*`
   macros, the two method declarations after `largest_key`, Doxygen with snippets, and a
   reference to the new docs page.
3. `src/include/cursor.h`: extend `WT_CURSOR_STATIC_INIT` with the two slots. Add a
   `WT_CBT_POSITION_CACHE_ONLY` flag consulted by the next/prev tree walk.
4. `src/cursor/cur_std.c`: `__wt_cursor_set_position_notsup(WT_CURSOR *, WT_POSITION *)` and
   `__wt_cursor_get_position_notsup(WT_CURSOR *, double *)` stubs.
5. `src/btree/bt_npos.c`: return the leaf remainder and the closest-leaf walk count from the
   descent; add the internal-pages-only descent that yields the separator key; add
   `__wt_btcur_set_position(cbt, position)` and `__wt_btcur_get_position(cbt, posp)`.
6. `src/cursor/cur_file.c`: `__curfile_set_position` / `__curfile_get_position`: API
   wrapping, validation, `WT_WITH_CHECKPOINT`, statistics.
7. `src/cursor/cur_table.c`: delegating implementations. `src/cursor/cur_layered.c`: the
   constituent selection and `get_position` rules from 5.1. Every other `cur_*.c`: stubs.
8. `lang/python/wiredtiger.i`: expose `WT_POSITION`, `NOTFOUND_OK` for `set_position`, an
   output typemap for `double *posp`, and Python-level constants for the flags.
9. `src/docs/cursor-position.dox`: new page; link from `cursors.dox`. `examples/c/ex_all.c`:
   snippets for both methods, the partition-sampling loop, and the key-only truncate-boundary
   idiom.
10. `dist/s_all` to regenerate statistics, prototypes, and formatting.

## 8. Testing

Python suite, `test_cursor_position01.py` and siblings, parameterized over single- and
multi-colgroup tables, layered tables, in-memory and on-disk:

- Ordering: walk the whole table with `next`, calling `get_position` at each record; assert
  non-decreasing.
- Round trip: for every record, `set_position` with its `get_position` value returns the same
  key on an unchanged tree.
- Partition sampling: `set_position(i/N)` for i in 0..N returns non-decreasing keys, with
  `pos <= 0` and `pos >= 1` landing on the first and last records.
- Anchors: `ANCHOR_FIRST` and `ANCHOR_LAST | PREV` return page-boundary keys; verified against
  the white-box csuite test below.
- Key-only: the returned key is less than or equal to the key `ANCHOR_FIRST` returns for the
  same position and greater than the key `ANCHOR_LAST | PREV` returns for the preceding
  position; `search_near` from it lands on the `ANCHOR_FIRST` record; the pages-read
  statistic does not increase when only internal pages are cached.
- Visibility: delete a range in one transaction, position into it from another, confirm the
  walk lands on the first visible record; with `PREV`, the last before the range.
- Cache-only: with a small cache and a table larger than it, `CACHE_ONLY` never increases the
  pages-read statistic; positions in evicted regions return `WT_NOTFOUND` or an in-memory
  neighbour with non-zero `pages_skipped`.
- `pages_skipped`: zero on a fully resident, undamaged tree; non-zero after fast-truncating a
  range of pages and positioning into it.
- Layered: positions over stable only; `get_position` with the ingest constituent positioned
  follows the 5.1 rules and leaves the cursor state unchanged.
- Errors: unpositioned `get_position`, `get_position` after key-only `set_position`, bounds
  set, `next_random` cursor, bulk cursor, column-store object, NaN position, empty table,
  checkpoint-cursor happy path. Unknown flag bits are accepted.
- Determinism guards: no assertions on exact position values; only ordering, round-trip, and
  boundary properties. No sleeps.

C-level: extend `test/csuite/normalized_pos` to compare the public methods against the
internal functions page by page, including the separator-key path. Add a `set_position`
operation to `test/format` alongside the random cursor smoke test so the concurrency paths
(splits, eviction, prepared updates) are exercised under load.

## 9. Alternatives considered

- **Flat parameter list** (`set_position(cursor, pos, flags, config)`): every new option is a
  signature change that must land in the library and the server together. The struct grows by
  appending fields.
- **Config string for options**: per-call parse cost on the hot path.
  `compile_configuration` mitigates but does not eliminate it, and the server would have to
  manage compiled strings for a stateless call. A config string remains a candidate struct
  field for rare options.
- **Deferred positioning**, where `set_position` only records the position and the next
  `next` or `prev` acts on it, as `bound` does. It gives direction for free and reads nothing
  until data is needed, but errors surface late, `get_key` returns nothing until the second
  call, the pending state must be cleared or reconciled by reset, search, insert, remove, and
  bounds, and table and layered cursors must forward the pending position at `next` time.
  Both of its benefits are available without new state: direction is a flag, and key-only
  mode gives "set now, read later" through the existing `set_key` then `search_near` idiom.
- **Reuse `search_near` via a cursor flag**: `search_near` is key-typed; encoding a fraction
  in `set_key` is a type pun.
- **Open-time config, `next_partition=true,next_partition_count=N`**: the cheapest drop-in
  for the server's `RandomCursor`, but expresses only stepping and has no `get_position`.
  Once `set_position` exists this is a thin wrapper and can be added if a `next()`-driven
  interface is wanted.
- **Dedicated `partition:` URI** returning separator keys: does not compose with normal
  cursor operations. Its one advantage, a boundary key without a leaf read, is key-only mode.

## 10. Future work

- Bounds-aware positioning.
- `index:` cursors.
- A metric selector (bytes, record count; absolute or normalized), once internal pages carry
  subtree aggregates.
- Output field for the position actually reached, and a walk limit field.
- A `next`-driven partition stepping cursor layered on `set_position`, if the server prefers
  that interface.
