# Interaction with timestamps

The main thing RTS is concerned with are the durable and stable
timestamps. The high-level details are covered above, what follows is a more
detailed view of how we use these timestamps.

We read `txn_global` for pinned and stable timestamps at the start of RTS,
and in diagnostic builds we read them again at the end to assert they didn't
change underneath us (indicating the system was not quiescent, and therefore
a bug).

Once RTS has finished, we roll back the global durable timestamp to the
stable timestamp, since if there are no unstable updates, `txn_global`'s
durable timestamp can by definition be moved backwards to the stable
timestamp. If we weren't to do this, it wouldn't automatically be a problem,
but we'd likely end up unnecessarily scanning tables the next time RTS is
run.

Timestamps of updates in the history store can have some surprising
properties:

* The start timestamp can be equal to the stop timestamp if the original
  update's commit timestamp is in order.
* During rollback of a prepared transaction, a history store update with a
  stop timestamp may not get removed, leading to a duplocate record. RTS
  ignores these.
* If we've had to fix any timestamps as part of RTS, a newly inserted update
  with an older timestamp may have a durable timestamp smaller than the
  current stop durable timestamp.
* When a checkpoint writes a committed prepared update, it's possible for
  further updates on that key to include history store changes before the
  transaction alters the history store update to have a proper stop
  timestamp. Thus, we can have an update in the history store with a maximum
  stop timestamp in the middle of other updates for the same key.
  
We also have some verification specifically related to the history
store. While iterating over records, we check that the start time of a newly
encountered record is greater than or equal to the previous record. We also
validate that the timestamps in the key and the cell are the same (where
possible).

Fast-truncate is worth mentioning briefly. If the page has a `page_del`
structure, we can look at its `durable_timestamp`, much the same as we would
a normal page.

# Interaction with transaction IDs

This is a little simpler than the timestamp rules - we use normal snapshot
visibility in most of the places we look at timestamps (see the timestamps
section). One exception is that we treat all data as visible when RTS is
called from the API (as opposed to startup or shutdown). 

We also clear the transaction ID of updates we look at during RTS, because
the connection's write generation will be initialised after RTS and the
updates in the cache would be problematic for other parts of the code if
they had a "legitimate" transaction ID.

# Operations on the history store

RTS touches the history store for a lot of things, primarily for deleting,
but it might also need to read an update to move it to the data store.

The deletion code is called from the general update aborting code, where we
might need to delete the first stable update we found when it came from the
history store. There's also a somewhat subtle case where a new key can be
added to a page, and then the page checkpointed, leading to a situation
where we have updates to the key in the history store but not the disk
image.

History store deletions are done using a standard history store cursor,
starting at the highest possible start timestamp, and removing keys from
newest to oldest until we hit an update with a start timestamp larger than
the rollback timestamp.

Note: in-memory databases don't have a history store, so none of this
section will apply.

# Iterating over B-Trees

This is one of the simplest parts of the RTS subsystem, as a metadata cursor
is specifically designed for these sorts of "iterate over a B-Tree"
operations. We use this functionality for RTS, with the caveat that we tweak
a few options so that corrupt or missing files don't raise errors. The
reasoning here is that RTS is orthogonal to salvage, and might be called as
part of salvage, so we simply need to "do our best". We do log the issues we
find, however.

Less obviously, the history store needs one final inspection regardless of
whether any other B-Trees were touched. This is because we usually rely on
getting to the history store "via" the data store, but if we don't touch a
B-Tree, that doesn't mean there aren't updates in the history store after
the stable timestamp.

# Iterating over pages

RTS tries to avoid looking at a page if it can. There are two ways we can
avoid instantiating a page: if it was truncated at/before the stable
timestamp, and if the page state is anything other than on-disk. If neither
of these are satisfied, we need to instantiate the page to look at it, but
that doesn't mean we look at the contents. Rather, we use the existing tree
walk custom skipping functionality to check the page metadata, stuff like
reconciliation results. These checks are fairly low-level, so they're
outside the scope of this document.

One final note is that we deliberately do not call RTS for internal pages.

# Iterating over updates

While the details of what happens here depends on the page type
(row-store, VLCS or FLCS), the overall idea is the same: we start by looking
at the insert list (i.e. any items inserted before the first key on the
page), iterate over all of the keys on the page, then look at the append
list (i.e. any items inserted after the last key on the page). We mark the
page as dirty so that we reconcile it in future.

The mechanics of deleting items (or inserting tombstones) from these places
are quite involved, and out of scope for the architecture guide.

# Interaction with checkpoints and eviction

Checkpoints cannot be performed while RTS is running, even if they start
before RTS. This is part of the contract that the system must be in a
quiescent state before running RTS, but it's also enforced by taking the
session's checkpoint lock.

Upon completion of RTS, a checkpoint is performed to that both in-memory and
on-disk versions of data are the same. It's possible for users to opt out of
this, for example if they intended to do a checkpoint shortly after RTS
anyway.

Eviction doesn't directly interact with RTS in any special way, but it's
worth pointing out that RTS is likely to generate a large number of both
clean and dirty pages, causing some amount of extra cache pressure. The
clean pages aren't a big deal, but the dirty pages will need to be written
out, causing a potentially large amount of I/O and the potential for
application threads to be forced into doing eviction work. Eviction triggers
and targets apply the same way during RTS as they do at any other time.

# Dry-run mode

Dry-run mode is a way of running RTS without making any changes to on-disk
or in-memory data structures, including the history store. It uses the same
code as "normal" RTS wherever possible, making it ideal for checking what
RTS would do on a given system, without disturbing it. One limitation is
that it stills needs to take the same locks as real RTS, so the system must
still be quiescent.

Dry-run mode does not, however, increment all of the same statistics as
"normal" RTS. As a rule, it only updates statistics that reflect what it's
doing. For example, it will increment statistics about the number of trees
and pages visited, but it will not change any statistics about the number of
updates aborted (since it is doing real work visiting pages, but it isn't
aborting any unstable updates).

Dry-run mode will cause similar cache pressure to the real thing, since it
must visit the same trees and pages. There is a slight difference around
eviction though, since it will not mark ny pages as dirty, meaning eviction
should have a much easier time cleaning up after a dry run.

# `rollback_to_stable_one`

There is an internal form of rollback to stable which can operate on a
single object, called `rollback_to_stable_one`. The API requires a URI is
passed in, which contrasts to `rollback_to_stable` where it will
deliberately look at every single tree in the database.

This is primarily used for salvage. Salvage accepts a single URI to attempt
fixing, so we don't want "regular" RTS to potentially read everything,
especially when the database is in a state where salvage needed to be called
in the first place.
