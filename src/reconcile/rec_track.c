/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_ovfl_track_init --
 *     Initialize the overflow tracking structure.
 */
int
__wt_ovfl_track_init(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    return (__wt_calloc_one(session, &page->modify->ovfl_track));
}

/*
 * __ovfl_discard_verbose --
 *     Dump information about a discard overflow record.
 */
static int
__ovfl_discard_verbose(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL *cell, const char *tag)
{
    WT_CELL_UNPACK_KV *unpack, _unpack;
    WT_DECL_ITEM(tmp);

    WT_RET(__wt_scr_alloc(session, 512, &tmp));

    unpack = &_unpack;
    __wt_cell_unpack_kv(session, page->dsk, cell, unpack);

    __wt_verbose(session, WT_VERB_OVERFLOW, "discard: %s%s%p %s", tag == NULL ? "" : tag,
      tag == NULL ? "" : ": ", (void *)page,
      __wt_addr_string(session, unpack->data, unpack->size, tmp));

    __wt_scr_free(session, &tmp);
    return (0);
}

/*
 * __wt_ovfl_discard_add --
 *     Add a new entry to the page's list of overflow records that have been discarded.
 */
int
__wt_ovfl_discard_add(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL *cell)
{
    WT_OVFL_TRACK *track;

    if (page->modify->ovfl_track == NULL)
        WT_RET(__wt_ovfl_track_init(session, page));

    track = page->modify->ovfl_track;
    WT_RET(__wt_realloc_def(
      session, &track->discard_allocated, track->discard_entries + 1, &track->discard));
    track->discard[track->discard_entries++] = cell;

    if (WT_VERBOSE_ISSET(session, WT_VERB_OVERFLOW))
        WT_RET(__ovfl_discard_verbose(session, page, cell, "add"));

    return (0);
}
