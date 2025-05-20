/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __cursor_prepared_txn_list_append(WT_SESSION_IMPL *, WT_CURSOR_PREPARE_TXN *, uint64_t);
static int __cursor_prepared_txn_setup(WT_SESSION_IMPL *, WT_CURSOR_PREPARE_TXN *);

/*
 * __cursor_prepared_txn_next --
 *     WT_CURSOR->next method for the prepared transaction cursor type.
 */
static int
__cursor_prepared_txn_next(WT_CURSOR *cursor)
{
    WT_CURSOR_PREPARE_TXN *cursor_prepare;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    cursor_prepare = (WT_CURSOR_PREPARE_TXN *)cursor;
    CURSOR_API_CALL(cursor, session, ret, next, NULL);

    if (cursor_prepare->list == NULL || cursor_prepare->list[cursor_prepare->next] == 0) {
        F_CLR(cursor, WT_CURSTD_KEY_SET);
        WT_ERR(WT_NOTFOUND);
    }

    /* TODO: Is this the right way to set a uint64_t in a cursor item structure? */
    cursor_prepare->iface.key.data = &cursor_prepare->list[cursor_prepare->next];
    cursor_prepare->iface.key.size = sizeof(uint64_t);
    ++cursor_prepare->next;

    F_SET(cursor, WT_CURSTD_KEY_INT);

err:
    API_END_RET(session, ret);
}

/*
 * __cursor_prepared_txn_reset --
 *     WT_CURSOR->reset method for the prepared transaction cursor type.
 */
static int
__cursor_prepared_txn_reset(WT_CURSOR *cursor)
{
    WT_CURSOR_PREPARE_TXN *cursor_prepare;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    cursor_prepare = (WT_CURSOR_PREPARE_TXN *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, reset, NULL);

    cursor_prepare->next = 0;
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

err:
    API_END_RET(session, ret);
}

/*
 * __cursor_prepared_txn_close --
 *     WT_CURSOR->close method for the prepared transaction cursor type.
 */
static int
__cursor_prepared_txn_close(WT_CURSOR *cursor)
{
    WT_CURSOR_PREPARE_TXN *cursor_prepare;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    cursor_prepare = (WT_CURSOR_PREPARE_TXN *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, close, NULL);
err:

    __wt_free(session, cursor_prepare->list);
    __wt_cursor_close(cursor);

    API_END_RET(session, ret);
}

/*
 * __wt_cursor_prepared_txn_open --
 *     WT_SESSION->open_cursor method for the prepared transaction cursor type.
 */
int
__wt_cursor_prepared_txn_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *other,
  const char *cfg[], WT_CURSOR **cursorp)
{
    WT_CURSOR_STATIC_INIT(iface, __wt_cursor_get_key, /* get-key */
      __wti_cursor_get_value_notsup,                  /* get-value */
      __wti_cursor_get_raw_key_value_notsup,          /* get-raw-key-value */
      __wti_cursor_set_key_notsup,                    /* set-key */
      __wti_cursor_set_value_notsup,                  /* set-value */
      __wti_cursor_compare_notsup,                    /* compare */
      __wti_cursor_equals_notsup,                     /* equals */
      __cursor_prepared_txn_next,                     /* next */
      __wt_cursor_notsup,                             /* prev */
      __cursor_prepared_txn_reset,                    /* reset */
      __wt_cursor_notsup,                             /* search */
      __wt_cursor_search_near_notsup,                 /* search-near */
      __wt_cursor_notsup,                             /* insert */
      __wt_cursor_modify_notsup,                      /* modify */
      __wt_cursor_notsup,                             /* update */
      __wt_cursor_notsup,                             /* remove */
      __wt_cursor_notsup,                             /* reserve */
      __wt_cursor_config_notsup,                      /* reconfigure */
      __wt_cursor_notsup,                             /* largest_key */
      __wt_cursor_config_notsup,                      /* bound */
      __wt_cursor_notsup,                             /* cache */
      __wt_cursor_reopen_notsup,                      /* reopen */
      __wt_cursor_checkpoint_id,                      /* checkpoint ID */
      __cursor_prepared_txn_close);                   /* close */
    WT_CURSOR *cursor;
    WT_CURSOR_PREPARE_TXN *cursor_prepare;
    WT_DECL_RET;

    WT_VERIFY_OPAQUE_POINTER(WT_CURSOR_PREPARE_TXN);

    WT_UNUSED(other);
    WT_UNUSED(cfg);

    WT_RET(__wt_calloc_one(session, &cursor_prepare));
    cursor = (WT_CURSOR *)cursor_prepare;
    *cursor = iface;
    cursor->session = (WT_SESSION *)session;
    cursor->key_format = "Q";  /* The key is an unsigned 64 bit number. */
    cursor->value_format = ""; /* Empty for now, will probably have something eventually */

    /*
     * Start the prepared transaction cursor which will fill in the cursor's list. Acquire the
     * schema lock, we need a consistent view of the metadata when scanning for prepared artifacts.
     */
    WT_WITH_CHECKPOINT_LOCK(session,
      WT_WITH_SCHEMA_LOCK(
        session, ret = __cursor_prepared_txn_setup(session, cursor_prepare)));
    WT_ERR(ret);
    WT_ERR(__wt_cursor_init(cursor, uri, NULL, cfg, cursorp));

    if (0) {
err:
        WT_TRET(__cursor_prepared_txn_close(cursor));
        *cursorp = NULL;
    }

    return (ret);
}

/*
 * __cursor_prepared_txn_setup --
 *     Setup a prepared transaction cursor on open. This will populate the data structures for the
 *     cursor to traverse. Some data structures live in this cursor, others live in the connection
 *     handle, since they can be claimed by other sessions while the cursor is open.
 */
static int
__cursor_prepared_txn_setup(
  WT_SESSION_IMPL *session, WT_CURSOR_PREPARE_TXN *cursor_prepare)
{
    /*
     * TODO: Replace this with code that traverses the metadata looking for tables with prepared
     * content, generating a WiredTiger transaction structure for each prepared transaction found,
     * and appending an entry to the cursors list for each unique transaction.
     */
    WT_RET(__cursor_prepared_txn_list_append(session, cursor_prepare, 123));
    return (0);
}

/*
 * __cursor_prepared_txn_list_append --
 *     Append a new prepared transaction identifier to the cursors list.
 */
static int
__cursor_prepared_txn_list_append(
  WT_SESSION_IMPL *session, WT_CURSOR_PREPARE_TXN *cursor_prepare, uint64_t prepared_id)
{
    uint64_t *p;

    /* Leave a NULL at the end to mark the end of the list. */
    WT_RET(__wt_realloc_def(session, &cursor_prepare->list_allocated, cursor_prepare->list_next + 2,
      &cursor_prepare->list));
    p = &cursor_prepare->list[cursor_prepare->list_next];
    p[0] = prepared_id;
    p[1] = 0;

    ++cursor_prepare->list_next;

    return (0);
}
