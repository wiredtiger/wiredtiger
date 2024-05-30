/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * This file defines things that apply to struct __wt_cursor or all cursor types.
 * Each type of cursor has its own file cursor_<type>.h.
 */

/* Get the session from any cursor. */
#define CUR2S(c) ((WT_SESSION_IMPL *)((WT_CURSOR *)c)->session)

/*
 * Initialize a static WT_CURSOR structure.
 */
#define WT_CURSOR_STATIC_INIT(n, get_key, get_value, get_raw_key_value, set_key, set_value,      \
  compare, equals, next, prev, reset, search, search_near, insert, modify, update, remove,       \
  reserve, reconfigure, largest_key, bound, cache, reopen, checkpoint_id, close)                 \
    static const WT_CURSOR n = {                                                                 \
      NULL, /* session */                                                                        \
      NULL, /* uri */                                                                            \
      NULL, /* key_format */                                                                     \
      NULL, /* value_format */                                                                   \
      get_key, get_value, get_raw_key_value, set_key, set_value, compare, equals, next, prev,    \
      reset, search, search_near, insert, modify, update, remove, reserve, checkpoint_id, close, \
      largest_key, reconfigure, bound, cache, reopen, 0, /* uri_hash */                          \
      {NULL, NULL},                                      /* TAILQ_ENTRY q */                     \
      0,                                                 /* recno key */                         \
      {0},                                               /* recno raw buffer */                  \
      NULL,                                              /* json_private */                      \
      NULL,                                              /* lang_private */                      \
      {NULL, 0, NULL, 0, 0},                             /* WT_ITEM key */                       \
      {NULL, 0, NULL, 0, 0},                             /* WT_ITEM value */                     \
      0,                                                 /* int saved_err */                     \
      NULL,                                              /* internal_uri */                      \
      {NULL, 0, NULL, 0, 0},                             /* WT_ITEM lower bound */               \
      {NULL, 0, NULL, 0, 0},                             /* WT_ITEM upper bound */               \
      0                                                  /* uint32_t flags */                    \
    }

/* Call a function without the evict reposition cursor flag, restore afterwards. */
#define WT_WITHOUT_EVICT_REPOSITION(e)                                              \
    do {                                                                            \
        bool __evict_reposition_flag = F_ISSET(cursor, WT_CURSTD_EVICT_REPOSITION); \
        F_CLR(cursor, WT_CURSTD_EVICT_REPOSITION);                                  \
        e;                                                                          \
        if (__evict_reposition_flag)                                                \
            F_SET(cursor, WT_CURSTD_EVICT_REPOSITION);                              \
    } while (0)

#define WT_CURSOR_RECNO(cursor) WT_STREQ((cursor)->key_format, "r")

#define WT_CURSOR_RAW_OK \
    (WT_CURSTD_DUMP_HEX | WT_CURSTD_DUMP_PRETTY | WT_CURSTD_DUMP_PRINT | WT_CURSTD_RAW)

/*
 * This macro provides a consistent way of checking if a cursor has either its lower or upper bound
 * set.
 */
#define WT_CURSOR_BOUNDS_SET(cursor) \
    F_ISSET((cursor), WT_CURSTD_BOUND_LOWER | WT_CURSTD_BOUND_UPPER)
