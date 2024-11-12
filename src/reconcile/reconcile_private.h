/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * WT_CHILD_MODIFY_STATE --
 *	We review child pages (while holding the child page's WT_REF lock), during internal-page
 * reconciliation. This structure encapsulates the child page's returned information/state.
 */
typedef struct {
    enum {
        WT_CHILD_IGNORE,   /* Ignored child */
        WT_CHILD_MODIFIED, /* Modified child */
        WT_CHILD_ORIGINAL, /* Original child */
        WT_CHILD_PROXY     /* Deleted child: proxy */
    } state;               /* Returned child state */

    WT_PAGE_DELETED del; /* WT_CHILD_PROXY state fast-truncate information */

    bool hazard; /* If currently holding a child hazard pointer */
} WT_CHILD_MODIFY_STATE;

typedef struct {
    WT_UPDATE *upd;       /* Update to write (or NULL) */
    WT_UPDATE *tombstone; /* The tombstone to write (or NULL) */

    WT_TIME_WINDOW tw;

    bool upd_saved;       /* An element on the row's update chain was saved */
    bool no_ts_tombstone; /* Tombstone without a timestamp */
} WT_UPDATE_SELECT;

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wti_ovfl_reuse_add(WT_SESSION_IMPL *session, WT_PAGE *page, const uint8_t *addr,
  size_t addr_size, const void *value, size_t value_size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_ovfl_reuse_search(WT_SESSION_IMPL *session, WT_PAGE *page, uint8_t **addrp,
  size_t *addr_sizep, const void *value, size_t value_size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_ovfl_track_wrapup(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_ovfl_track_wrapup_err(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_child_modify(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *ref,
  WT_CHILD_MODIFY_STATE *cmsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_col_fix(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *pageref,
  WT_SALVAGE_COOKIE *salvage) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_col_int(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *pageref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_col_var(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *pageref,
  WT_SALVAGE_COOKIE *salvage) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_dictionary_init(WT_SESSION_IMPL *session, WT_RECONCILE *r, u_int slots)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_hs_clear_on_tombstone(WT_SESSION_IMPL *session, WT_RECONCILE *r,
  uint64_t recno, WT_ITEM *rowkey, bool reinsert) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_row_int(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_row_leaf(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *pageref,
  WT_SALVAGE_COOKIE *salvage) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_split(WT_SESSION_IMPL *session, WT_RECONCILE *r, size_t next_len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_split_crossing_bnd(WT_SESSION_IMPL *session, WT_RECONCILE *r, size_t next_len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_split_finish(WT_SESSION_IMPL *session, WT_RECONCILE *r)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_split_grow(WT_SESSION_IMPL *session, WT_RECONCILE *r, size_t add_len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_split_init(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_PAGE *page,
  uint64_t recno, uint64_t primary_size, uint32_t auxiliary_size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_rec_upd_select(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_INSERT *ins,
  WT_ROW *rip, WT_CELL_UNPACK_KV *vpack, WT_UPDATE_SELECT *upd_select)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_rec_col_fix_write_auxheader(WT_SESSION_IMPL *session, uint32_t entries,
  uint32_t aux_start_offset, uint32_t auxentries, uint8_t *image, size_t size);
extern void __wti_rec_dictionary_free(WT_SESSION_IMPL *session, WT_RECONCILE *r);
extern void __wti_rec_dictionary_reset(WT_RECONCILE *r);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
