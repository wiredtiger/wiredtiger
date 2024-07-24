The Overall idea is that oligarch tables consist of an ingest table, which
is periodically flushed out to the stable table. The stable table - as the
name suggests - only contains committed transactions. To enable this, we
require that oligarch tables are logged.

# Schema

If the dhandle type is `WT_DHANDLE_TYPE_OLIGARCH`, we use
`__schema_open_oligarch`. This opens the data handle for an oligarch table.
This sets up a `WT_OLIGARCH` structure with the key format, value format,
ingest URI, and stable URI.

The other important member of a `WT_OLIGARCH` is `WT_DATA_HANDLE iface`,
which means we can cast a `WT_OLIGARCH` to/from a `WT_DATA_HANDLE`.

Once this is set up, we no longer have the table write lock. We continue to
open the ingest and stable members without the lock, which is a pretty
simple operation: get the `dhandle` for the URI, bump the in-use counter
(separate to what getting the `dhandle` did), set the `WT_OLIGARCH`
structure's pointer to the `dhandle` we got, and then release the `dhandle`.
Note that the "leaking" behaviour of the `dhandle` reference count is
intentional here.

This functionality will also ensure the oligarch manager thread is started,
and add the ingest and stable tables to the oligarch manager. The details of
this are in another section.

Upon closing an oligarch table (e.g. as part of the sweep server's normal
operation), we free the key/value formats, free the URIs, remove the table
from the oligarch manager, and decrement the in-use counts of the data
handles for the `WT_DATA_HANDLE` structures in the `WT_OLIGARCH`. This
decrement mirrors the increment we performed when opening.

Another part of this change was altering the dhandle/cursor macros, as they
assumed there was a dhandle with a B-Tree under a cursor, which is no longer
true with oligarch tables. So they were changed to directly accept a dhandle
rather than a B-Tree.

# Oligarch manager

The oligarch manager consists of two threads.

## Log application thread

The first thread, the "log application" thread, first checks to make sure
that it's the only thread doing log application by using an atomic CAS. This
is possibly redundant given the guards around starting the oligarch manager
more broadly. Anyway, once it's passed this test and decided to do some log
catchup, it applies `__oligarch_log_replay` to all log records since it last
performed log application.

To identify these records, it saves the last applied LSN on the
`WT_OLIGARCH_MANAGER` structure. For the startup case, this is zero, and we
we set `WT_LOGSCAN_FIRST` so we see the earliest available record and start
applying from there.

`__oligarch_log_replay` will only do work for commit records (to maintain
the invariant that the stable table only contains committed content). When
it spots such a record, it unpacks the transaction ID from the log record
(in order to save it as the high water mark for next time), and replays the
commit by applying the commit's constituent operations.

For each operation in the commit, the high-level idea is that we unpack the
operation based on the type and apply it to our stable table.

This mechanism relies on logs not being cleaned up for some amount of time.
Currently logging only archives a log after a checkpoint has been completed
that contains all relevant content. This has worked fine so far.

## Checkpoint thread

The second thread, the "checkpoint" thread, reviews the oligarch tables and
checkpoints the first with enough accumulated content. It will likely be
changed in future to be more fair.

The definition of "enough" content is hard-coded right now, but once we do
the checkpoint there's a little bookkeeping for the transaction ID, where we
save the maximum applied transaction ID to the ingest B-Tree's
`oldest_live_txnid` member so the ingest table knows what information can be
garbage collected. (The garbage collection is performed as part of
reconciliation, specifically `__wt_update_obsolete_check`.)

## Adding/removing entries

When adding a table to the oligarch manager, we append a
`WT_OLIGARCH_MANAGER_ENTRY` structure to the `WT_OLIGARCH_MANAGER` entry
array. Right now, this array only supports 1000 entries - this is temporary.
We take the `oligarch_lock`, allocate an entry, fill in the ingest ID,
stable ID and the `dhandle` pointer. We need to pick a transaction ID to
support the garbage collection process (which needs the oldest checkpoint ID
to start collecting from). To this end, we conservatively choose the global
oldest ID since nothing will have been written to the ingest table at that
point.

Removing an entry is simpler - simply close the cursor then free/null the
entry. There is a little bit of a dance around shutdown, where the function
gets called redundantly. To avoid any issues, we set a flag
(`WT_OLIGARCH_MANAGER_OFF`) when the oligarch manager is destroyed, and
no-op the removal if this is set.

# Oligarch cursors

These offer the same API as any other cursor, including some of the "usual"
restrictions around not supporting bulk-loading, random positioning,
in-memory databases, or being opened as part of a checkpoint. 

The unique part is the notion of "constituent cursors". An oligarch table is
backed by two "real" tables (the ingest and stable tables), so we maintain
two `WT_CURSOR` pointers for these tables. (We also maintain a third such
pointer so we can remember where we are across iterations, but more on that
later.)

Closing an oligarch cursor is relatively normal - we close our constituent
ingest and stable cursors, close out any running operation, and then fall
back to the generic `__wt_cursor_close` functionality.

Most typical cursor operations (`next`, `reserve`, etc) are wrapped in a
pair of `__coligarch_enter` and `__coligarch_leave` calls. These calls are
essentially for lifetime management: we need to make sure the two
constituent cursors are reset, opened and closed at the same time, as well
as making sure `session->ncursors` is handled correctly (otherwise it would
be possible to open the oligarch table, open the constituent cursors, close
them later, and have the session closed with the "parent" oligarch cursor
left open).

We also use these to be a little bit clever, since cursors open for updates
will only touch the ingest table, so we can save some work.

The data manipulation functions for these cursors follow a pattern of (1)
finding whether to use the ingest or stable table, and (2) performing the
operation there. The worker function here is `__coligarch_put`, which uses a
set of boolean arguments to determine which call to make to the sub-cursor.

`__coligarch_put` is also used for removing data, where we "put" a tombstone
in place of calling `cursor->remove`. This is so a deletion leaves us with
something to shuffle across to the stable table, instead of having values
silently remain (as they would if we didn't have a way to spot them go
missing). Note that the tombstones here are not a "typical" tombstone as
you'd see on the update chain - rather, it's a special key/value. It's
theoretically possible for an application to write the special key/value,
but all that'd happen is a wasted copy from the ingress to the stable table.

The search code is substantially more fiddly than the modification code. We
can't just call `search_near` on the current cursor, as the other
constituent table might have a key that's closer. The semantics are:

* An exact match always wins
* A larger key is preferred if one exists
* Otherwise, a smaller key is returned
* If both constituent tables have a larger key, return the closest.

The search functions rely on a different helper function to the modification
calls, leaning on `__coligarch_lookup`. This isn't particularly complicated,
and mostly exists to encapsulate the logic of "falling back" to the stable
table if a key wasn't found in the ingest table.

`__coligarch_prev` (and the related `next` call) has a subtlety caused by
the "two sub-cursors" situation. For both directions, we need to move the
"smallest" cursor in the relevant direction, so this involves comparing the
keys of both the ingest and stable sub-cursors.
