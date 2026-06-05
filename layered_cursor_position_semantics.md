# WiredTiger cursor position-retention semantics

This documents, per WT_CURSOR operation, whether the cursor is left *positioned* after
the call. It is sourced from the public doxygen contract in `wiredtiger.h.in`, the
btree/file implementation in `cur_file.c`, and the layered implementation in
`cur_layered.c`. All paths are relative to the repo root
`/home/ubuntu/work/git/wiredtiger4`.

## What "positioned" means

The cursor's state is tracked by flags on `WT_CURSOR`
(`src/include/wiredtiger.h.in:794-807`):

- `WT_CURSTD_KEY_INT` (`0x002000000`) — "Key points into tree." This is the
  *positioned* flag: the cursor sits on a real record and a following
  `next`/`prev` will move relative to it. `WT_CURSOR_IS_POSITIONED(cbt)` is
  `cbt->ref != NULL && cbt->ref->page != NULL` (`src/include/cursor.h:571`).
- `WT_CURSTD_KEY_EXT` (`0x001000000`) — "Key points out of tree." A key value is
  *set/accessible* but the cursor is **not** positioned (e.g. an application
  `set_key`, or the key returned by `largest_key`). A following `next` starts at
  the beginning of the table, not relative to this key.
- `WT_CURSTD_VALUE_INT` (`0x200000000`) — value points into the tree (accessible
  via `get_value`).
- `WT_CURSTD_KEY_SET` / `WT_CURSTD_VALUE_SET` are the EXT|INT masks; "no key/value"
  means the masked bits are 0 and `get_key`/`get_value` would fail.

For the file cursor, `WT_CBT_ACTIVE` plus `KEY_INT` is the positioned state and is
asserted at the end of every method. For the layered cursor, the comment at
`src/cursor/cur_layered.c:1097-1101` states the rule explicitly: *"A layered cursor
is considered positioned when the customer-visible `iface` cursor has the
WT_CURSTD_KEY_INT flag set."*

## Per-operation table

| Operation | Position after success | Position after WT_NOTFOUND / error | Key accessible after success? | Value accessible after success? | Notes / layered deviations |
|---|---|---|---|---|---|
| `search` | Positioned (`KEY_INT` + `VALUE_INT`) | Not positioned; constituents reset (layered) | Yes (internal) | Yes | Layered sets `KEY_INT\|VALUE_INT` only on `ret==0`; on error resets constituents. |
| `search_near` | Positioned (`KEY_INT` + `VALUE_INT`) | Not positioned; constituents reset (layered) | Yes (internal) | Yes | Same as search. Layered may internally iterate to skip tombstones. |
| `next` | Positioned (`KEY_INT` + `VALUE_INT`) | Not positioned (end of table); layered resets constituents | Yes (internal) | Yes | Maintains position/key/value. |
| `prev` | Positioned (`KEY_INT` + `VALUE_INT`) | Not positioned (start of table); layered resets constituents | Yes (internal) | Yes | Maintains position/key/value. |
| `reset` | **Not positioned** (no key, no value) | n/a | No | No | Deliberately gives up position; also clears user bounds. |
| `insert` | **Not positioned** (no key, no value*) | Not positioned | No* | No (except WT_DUPLICATE_KEY returns existing value) | *Column-store `append=true` leaves the assigned record number as `KEY_EXT` (still not "positioned"). Layered always clears position. |
| `update` | Positioned (`KEY_INT` + `VALUE_INT`) | Not positioned | Yes (internal) | Yes | Keeps position. Layered preserves an in-progress iteration. |
| `modify` | Positioned (`KEY_INT` + value set, not always INT) | Not positioned | Yes (internal) | Yes | Keeps position. Value may be EXT after modify. |
| `remove` | **Conditional**: positioned (`KEY_INT`, no value) iff cursor was positioned before the call; otherwise not positioned | Not positioned | Only if was positioned (key, possibly INT/EXT/none) | No (never a value after remove) | Unique semantic: position is preserved on the removed key only when starting positioned. |
| `reserve` | Positioned (`KEY_INT` + `VALUE_INT`) — the impl does a `search` afterward | Not positioned | Yes | Yes | Both file and layered end by calling `cursor->search(cursor)` to restore a value. |
| `bound` (set) | Unchanged (it is illegal to *set* a bound on a positioned cursor) | Unchanged | Unchanged | Unchanged | `bound` does not move the cursor; setting a bound on a positioned cursor returns EINVAL. |
| `largest_key` | **Not positioned**; key set as `KEY_EXT` only | Not positioned (key cleared) | Yes (external, not a position) | No | Doxygen: "The cursor ends with no position." The returned key is EXT, so a following `next` starts from the beginning, not from this key. |

## Citations

### Public doxygen contract (`src/include/wiredtiger.h.in`)

- **search** (`:411-414`): "On success, the cursor ends positioned at the returned
  record; to minimize cursor resources, the WT_CURSOR::reset method should be
  called as soon as the record has been retrieved..."
- **search_near** (`:444-447`): "On success, the cursor ends positioned at the
  returned record..."
- **next** (`:370`) / **prev** (`:380`): "Return the next/previous record." (Position
  is maintained — confirmed by the file-cursor assertions below.)
- **reset** (`:390-393`): "Any resources held by the cursor are released, and the
  cursor's key and position are no longer valid. Subsequent iterations with
  WT_CURSOR::next will move to the first record, or with WT_CURSOR::prev will move
  to the last record."
- **insert** (`:487-496`): "The cursor ends with no position, and a subsequent call
  to the WT_CURSOR::next (WT_CURSOR::prev) method will iterate from the beginning
  (end) of the table.... the cursor ends with no key set and a subsequent call to
  the WT_CURSOR::get_key method will fail. The cursor ends with no value set..."
  (Exception: column-store `append=true` returns the new record number via
  `get_key`.)
- **update** (`:576-580`): "On success, the cursor ends positioned at the modified
  record... (The WT_CURSOR::insert method never keeps a cursor position and may be
  more efficient for that reason.)"
- **modify** (`:541-543`): "On success, the cursor ends positioned at the modified
  record..."
- **remove** (`:605-612`): "Any cursor position does not change: if the cursor was
  positioned before the WT_CURSOR::remove call, the cursor remains positioned at
  the removed record... If the cursor was not positioned before the
  WT_CURSOR::remove call, the cursor ends with no position, and a subsequent call
  to the WT_CURSOR::next (WT_CURSOR::prev) method will iterate from the beginning
  (end) of the table."
- **reserve** (`:639-641`): "On success, the cursor ends positioned at the specified
  record..."
- **largest_key** (`:678-679`): "Get the table's largest key, ignoring visibility.
  This method is only supported by file: or table: objects. The cursor ends with
  no position."
- **bound** (`:708-726`): sets/clears range bounds; no statement about position
  because it does not move the cursor.

### File / btree cursor (`src/cursor/cur_file.c`)

- **`__curfile_next`** (`:194-197`): "Next maintains a position, key and value."
  Asserts `WT_CBT_ACTIVE && KEY_SET==KEY_INT && VALUE_SET==VALUE_INT`.
- **`__curfile_prev`** (`:256-259`): "Prev maintains a position, key and value."
  Same assertion.
- **`__curfile_reset`** (`:291-294`): "Reset maintains no position, key or value."
  Asserts `!WT_CBT_ACTIVE && KEY_SET==0 && VALUE_SET==0`. Clears user bounds only on
  an API_USER_ENTRY (`:288-289`).
- **`__curfile_search`** (`:330-333`): "Search maintains a position, key and value."
  Asserts `WT_CBT_ACTIVE && KEY_INT && VALUE_INT`.
- **`__curfile_search_near`** (`:371-374`): "Search-near maintains a position, key
  and value." Same assertion.
- **`__curfile_insert`** (`:410-419`): "Insert maintains no position, key or value
  (except for column-store appends, where we are returning a key)." Asserts
  `!WT_CBT_ACTIVE` and either (`APPEND && KEY_SET==KEY_EXT`) or (`!APPEND &&
  KEY_SET==0`), and `VALUE_SET==0`.
- **`__curfile_update`** (`:525-528`): "Update maintains a position, key and value."
  Asserts `WT_CBT_ACTIVE && KEY_INT && VALUE_INT`.
- **`__curfile_modify`** (`:487-492`): "Modify maintains a position, key and value.
  Unlike update, it's not always an internal value." Asserts `WT_CBT_ACTIVE &&
  KEY_INT` and `VALUE_SET != 0` (value may be EXT).
- **`__curfile_remove`** (`:548-588`): The header comment captures the unique rule:
  "the cursor stays positioned if it starts positioned, otherwise clear the cursor
  on completion." `positioned = F_ISSET(cursor, WT_CURSTD_KEY_INT)` is captured
  *before* the call (`:555`); if an initial position is lost it forces `WT_ROLLBACK`
  (`:571-575`). "Remove with a search-key is fire-and-forget, no position and no
  key. Remove starting from a position maintains the position and a key, but the key
  can end up being internal, external, or not set... There's never a value." Asserts
  `VALUE_SET==0` (`:582`).
- **`__curfile_reserve`** (`:614-631`): "Reserve maintains a position and key, which
  doesn't match the library API, where reserve maintains a value. Fix the API by
  searching after each successful reserve operation." Returns
  `cursor->search(cursor)` (`:631`), so the caller sees a fully positioned cursor
  with a value.
- **`__curfile_largest_key`** (`:1024-1046`): resets the cursor to give up position
  (`:1025`), reads the largest key via `__wt_btcur_prev`, then resets again and
  re-sets the key as **external**: `F_SET(cursor, WT_CURSTD_KEY_EXT)` (`:1039`). No
  value, no position.
- **`__curfile_bound`** (`:991-994`): "It is illegal to set a bound on a positioned
  cursor (it's fine to clear one)" — returns EINVAL if `WT_CURSOR_IS_POSITIONED`.

### Layered cursor (`src/cursor/cur_layered.c`)

The layered cursor follows the same external contract; the differences are about
managing two constituent cursors (`ingest_cursor`, `stable_cursor`) and the
`WT_CLAYERED_ITERATE_NEXT/PREV` iteration flags.

- **`__clayered_search`** (`:1771-1777`): on `ret==0` only, clears `KEY_SET|VALUE_SET`
  and sets `KEY_INT|VALUE_INT` — positioned, key and value accessible. Clears the
  iterate flags up front (`:1753`). On error, `err:` does not set the INT flags, so
  the cursor is unpositioned.
- **`__clayered_search_near`** (`:2063-2067`): identical pattern — sets
  `KEY_INT|VALUE_INT` only on success; `__clayered_search_near_int` resets
  constituents on error (`:2020-2022`).
- **`__clayered_next` / `__clayered_prev`** (`:1304-1309`): `__clayered_iterate` sets
  the iface to `KEY_INT|VALUE_INT` on success and clears `KEY_SET|VALUE_SET` on
  error (`:1308-1309`). On a non-PREPARE_CONFLICT error `__clayered_iterate_int`
  resets constituents (`:1267-1268`).
- **`__clayered_reset`** (`:1431-1440`): resets all positioned constituents and, at
  `err:`, `F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET)` — no key, no value,
  no position. Clears bounds on user entry (`:1434-1437`).
- **`__clayered_insert`** (`:2290-2321`): comment: "Insert doesn't keep the cursor
  positioned. Always clear the iteration flags." (`:2290-2291`). After the put,
  `F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET)` (`:2321`) — ends with no
  key, no value, no position, matching the file cursor. `__clayered_put` only sets
  `current_cursor` for non-INSERT ops (`:2144-2145`).
- **`__clayered_update`** (`:2351-2383`): comment: "Update keeps the cursor
  positioned. Retain the iteration flags if we are in the middle of a cursor
  traversal." (`:2351-2356`). After the put it copies the constituent's key/value
  and `F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT)` (`:2383`) — positioned,
  matching the file cursor.
- **`__clayered_modify`** (`:2865-2904`): comment: "Modify keeps the cursor
  positioned." (`:2865-2870`). Sets `KEY_INT` and copies the value flag mask from the
  current constituent (value may be EXT) (`:2895-2898`).
- **`__clayered_remove`** (`:2408-2442`): captures `positioned = F_ISSET(cursor,
  WT_CURSTD_KEY_INT)` *before* the call (`:2409`). Comment: "Remove keeps the cursor
  positioned." After the remove: `F_CLR(KEY_SET|VALUE_SET)`, then if it was
  positioned `F_SET(cursor, WT_CURSTD_KEY_INT)` (key only, no value); else it resets
  the constituents (`:2438-2442`). The comment notes this "isn't just cosmetic,
  without a reset, iteration on this cursor won't start at the beginning/end of the
  table." The follower path refuses to write a second tombstone on an
  already-deleted positioned record, returning `WT_NOTFOUND` (`:2173-2174`).
- **`__clayered_reserve`** (`:2470-2498`): clears the iterate flags (a search follows
  anyway, `:2470-2474`); ends with `return (ret == 0 ? cursor->search(cursor) :
  ret)` (`:2498`), so on success the cursor is positioned with a value — same as the
  file cursor.
- **`__clayered_largest_key`** (`:2563-2569`): resets constituents, then
  `F_CLR(cursor, WT_CURSTD_KEY_INT)` and `F_SET(cursor, WT_CURSTD_KEY_EXT)`
  (`:2566,2569`) — key is external, not positioned, no value. Matches file cursor.
- **`__clayered_bound`** (`:1501-...`): copies bounds to constituents; like the file
  cursor it does not move the cursor.

#### Layered iteration-flag detail (why update/remove/modify can extend a chain)

The iterate flags `WT_CLAYERED_ITERATE_NEXT/PREV` let a following `next`/`prev` skip
re-positioning the alternate constituent (`:1106-1108`). `update`, `modify`, and
`remove` deliberately **retain** these flags when the cursor is already positioned
(`KEY_INT`), so a positioned write inside a scan does not force the next iteration
to restart from the table boundary:

- update: `if (!F_ISSET(cursor, WT_CURSTD_KEY_INT)) F_CLR(... ITERATE_NEXT|PREV)`
  (`:2355-2356`).
- modify: same guard (`:2869-2870`).
- remove: `if (!positioned) F_CLR(... ITERATE_NEXT|PREV)` (`:2416-2417`).
- insert / reserve / search / search_near / largest_key: always clear the iterate
  flags (`:2291`, `:2474`, `:1753`, `:2041`, `:2521`).

`__clayered_put` (used by insert/update/reserve) only resets the constituent cursors
when not mid-iteration: `if (!F_ISSET(clayered, ITERATE_NEXT|PREV))
__clayered_reset_cursors(...)` (`:2121-2122`); same guard in the follower remove
path (`:2189-2190`).

## Implications for chaining

A "position-holding chain" is a sequence that keeps the cursor on a record so the
next `next`/`prev` continues relative to it. Classifying the operations:

**Keep position — safe to chain freely, can be followed by `next`/`prev`:**

- `search`, `search_near` — leave the cursor positioned with key+value
  (`KEY_INT|VALUE_INT`). These are the natural entry points to a chain.
- `next`, `prev` — maintain position, key and value.
- `update` — keeps position (`KEY_INT|VALUE_INT`); explicitly the documented
  alternative to `insert` when you want to retain position. In layered it also
  preserves an in-progress iteration.
- `modify` — keeps position (`KEY_INT`, value set).
- `reserve` — ends positioned with a value because the implementation issues an
  internal `search`.
- `remove` **only when the cursor was already positioned** — it stays on the removed
  key (`KEY_INT`, no value), so a following `next` advances correctly. Note the value
  is gone, so do not `get_value` between the remove and the next/prev.

**Clear position — break the chain; a following `next`/`prev` restarts from the
table boundary; use sparingly in a position-holding chain:**

- `insert` (`set_key` + `insert`) — **resets position**. Ends with no key and no
  value (column-store append leaves only a `KEY_EXT` record number). A following
  `next`/`prev` iterates from the beginning/end of the table. This is true for both
  the file and layered cursors.
- `remove` **when the cursor was not positioned** (i.e. a `set_key`+`remove`
  fire-and-forget) — ends with no position; a following `next` starts from the table
  beginning.
- `reset` — clears *everything* (no key, no value, no position) and also drops user
  bounds. Hard chain terminator.
- `largest_key` — ends with no position; only an *external* key is set
  (`KEY_EXT`), so a following `next` starts from the beginning of the table, **not**
  from the largest key.
- Any operation that returns `WT_NOTFOUND` or an error — leaves the cursor
  unpositioned (and in the layered case resets the constituent cursors).

**Direct answers to the specific questions:**

- Does `insert` (set_key+insert) reset position? **Yes** — it ends with no position
  and no key (file `cur_file.c:410-419`; layered `cur_layered.c:2290-2321`).
- Does `remove` keep the cursor positioned so a following `next` works? **Only if the
  cursor was positioned before the remove.** Then it stays on the removed key
  (`KEY_INT`, no value) and `next`/`prev` work. A `set_key`+`remove` on an
  unpositioned cursor ends with no position (file `cur_file.c:548-588`; layered
  `cur_layered.c:2408-2442`).
- Does `search`/`search_near` leave it positioned? **Yes** — both end positioned with
  key and value on success (`cur_file.c:330-333,371-374`; `cur_layered.c:1771-1777,
  2063-2067`).
- Does `reset` clear everything? **Yes** — key, value, and position are all cleared,
  and user bounds reset (`cur_file.c:291-294`; `cur_layered.c:1431-1440`).
- Does `update` keep position? **Yes** — ends positioned with key and value
  (`cur_file.c:525-528`; `cur_layered.c:2351-2383`).
