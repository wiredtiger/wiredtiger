/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_root_ref_init --
 *     Initialize a tree root reference, and link in the root page.
 */
void
__wt_root_ref_init(WT_SESSION_IMPL *session, WT_REF *root_ref, WT_PAGE *root, bool is_recno)
{
    WT_UNUSED(session); /* Used in a macro for diagnostic builds */
    memset(root_ref, 0, sizeof(*root_ref));

    root_ref->page = root;
    F_SET(root_ref, WT_REF_FLAG_INTERNAL);
    WT_REF_SET_STATE(root_ref, WT_REF_MEM);

    root_ref->ref_recno = is_recno ? 1 : WT_RECNO_OOB;

    root->pg_intl_parent_ref = root_ref;
}