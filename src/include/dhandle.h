/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Helpers for calling a function with a data handle in session->dhandle then restoring afterwards.
 */
#define WT_WITH_DHANDLE(s, d, e)                        \
    do {                                                \
        WT_DATA_HANDLE *__saved_dhandle = (s)->dhandle; \
        (s)->dhandle = (d);                             \
        e;                                              \
        (s)->dhandle = __saved_dhandle;                 \
    } while (0)

#define WT_WITH_BTREE(s, b, e) WT_WITH_DHANDLE(s, (b)->dhandle, e)

/* Call a function without the caller's data handle, restore afterwards. */
#define WT_WITHOUT_DHANDLE(s, e) WT_WITH_DHANDLE(s, NULL, e)

/*
 * Call a function with the caller's data handle, restore it afterwards in case it is overwritten.
 */
#define WT_SAVE_DHANDLE(s, e) WT_WITH_DHANDLE(s, (s)->dhandle, e)

/* Check if a handle is inactive. */
#define WT_DHANDLE_INACTIVE(dhandle) \
    (F_ISSET(dhandle, WT_DHANDLE_DEAD) || !F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE | WT_DHANDLE_OPEN))

/* Check if a handle could be reopened. */
#define WT_DHANDLE_CAN_REOPEN(dhandle) \
    (!F_ISSET(dhandle, WT_DHANDLE_DEAD | WT_DHANDLE_DROPPED) && F_ISSET(dhandle, WT_DHANDLE_OPEN))

/* The metadata cursor's data handle. */
#define WT_SESSION_META_DHANDLE(s) (((WT_CURSOR_BTREE *)((s)->meta_cursor))->dhandle)

#define WT_DHANDLE_ACQUIRE(dhandle) (void)__wt_atomic_add32(&(dhandle)->session_ref, 1)

#define WT_DHANDLE_RELEASE(dhandle) (void)__wt_atomic_sub32(&(dhandle)->session_ref, 1)

#define WT_DHANDLE_NEXT(session, dhandle, head, field)                                     \
    do {                                                                                   \
        WT_ASSERT(session, FLD_ISSET(session->lock_flags, WT_SESSION_LOCKED_HANDLE_LIST)); \
        if ((dhandle) == NULL)                                                             \
            (dhandle) = TAILQ_FIRST(head);                                                 \
        else {                                                                             \
            WT_DHANDLE_RELEASE(dhandle);                                                   \
            (dhandle) = TAILQ_NEXT(dhandle, field);                                        \
        }                                                                                  \
        if ((dhandle) != NULL)                                                             \
            WT_DHANDLE_ACQUIRE(dhandle);                                                   \
    } while (0)

/*
 * WT_DATA_HANDLE --
 *	A handle for a generic named data source.
 */
struct __wt_data_handle {
    WT_RWLOCK rwlock; /* Lock for shared/exclusive ops */
    TAILQ_ENTRY(__wt_data_handle) q;
    TAILQ_ENTRY(__wt_data_handle) hashq;

    const char *name;       /* Object name as a URI */
    uint64_t name_hash;     /* Hash of name */
    const char *checkpoint; /* Checkpoint name (or NULL) */
    const char **cfg;       /* Configuration information */
    const char *meta_base;  /* Base metadata configuration */

    /*
     * Sessions holding a connection's data handle will have a non-zero reference count; sessions
     * using a connection's data handle will have a non-zero in-use count. Instances of cached
     * cursors referencing the data handle appear in session_cache_ref.
     */
    uint32_t session_ref;          /* Sessions referencing this handle */
    int32_t session_inuse;         /* Sessions using this handle */
    uint32_t excl_ref;             /* Refs of handle by excl_session */
    uint64_t timeofdeath;          /* Use count went to 0 */
    WT_SESSION_IMPL *excl_session; /* Session with exclusive use, if any */

    WT_DATA_SOURCE *dsrc; /* Data source for this handle */
    void *handle;         /* Generic handle */

    enum {
        WT_DHANDLE_TYPE_BTREE,
        WT_DHANDLE_TYPE_TABLE,
        WT_DHANDLE_TYPE_TIERED,
        WT_DHANDLE_TYPE_TIERED_TREE
    } type;

    bool compact_skip; /* If the handle failed to compact */

    /*
     * Data handles can be closed without holding the schema lock; threads walk the list of open
     * handles, operating on them (checkpoint is the best example). To avoid sources disappearing
     * underneath checkpoint, lock the data handle when closing it.
     */
    WT_SPINLOCK close_lock; /* Lock to close the handle */

    /* Data-source statistics */
    WT_DSRC_STATS *stats[WT_COUNTER_SLOTS];
    WT_DSRC_STATS *stat_array;

/*
 * Flags values over 0xfff are reserved for WT_BTREE_*. This lets us combine the dhandle and btree
 * flags when we need, for example, to pass both sets in a function call.
 *
 * To help avoid accidental overrun of the flag values, we add a special flag value that should
 * always be the last and highest. We use this value to assert that the dhandle flags haven't run
 * into the space reserved for btree flags.
 */
/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_DHANDLE_DEAD 0x001u         /* Dead, awaiting discard */
#define WT_DHANDLE_DISCARD 0x002u      /* Close on release */
#define WT_DHANDLE_DISCARD_KILL 0x004u /* Mark dead on release */
#define WT_DHANDLE_DROPPED 0x008u      /* Handle is dropped */
#define WT_DHANDLE_EXCLUSIVE 0x010u    /* Exclusive access */
#define WT_DHANDLE_HS 0x020u           /* History store table */
#define WT_DHANDLE_IS_METADATA 0x040u  /* Metadata handle */
#define WT_DHANDLE_LOCK_ONLY 0x080u    /* Handle only used as a lock */
#define WT_DHANDLE_OPEN 0x100u         /* Handle is open */
#define WT_DHANDLE_ZZZ_ENDFLAG 0x200u  /* One past highest flag value */
                                       /* AUTOMATIC FLAG VALUE GENERATION STOP */
    uint32_t flags;
#define WT_DHANDLE_MAX_FLAG 0x1000u /* Used to ensure we don't overflow legal flag values */
#if WT_DHANDLE_ZZZ_ENDFLAG > WT_DHANDLE_MAX_FLAG
#error "Too many dhandle flags"
#endif

/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_DHANDLE_ASSERT_TS_READ_ALWAYS 0x001u /* Assert read always checking. */
#define WT_DHANDLE_ASSERT_TS_READ_NEVER 0x002u  /* Assert read never checking. */
#define WT_DHANDLE_ASSERT_TS_WRITE 0x004u       /* Assert write checking. */
#define WT_DHANDLE_TS_ALWAYS 0x008u             /* Handle using always checking. */
#define WT_DHANDLE_TS_KEY_CONSISTENT 0x010u     /* Handle using key consistency checking. */
#define WT_DHANDLE_TS_MIXED_MODE 0x020u         /* Handle using mixed mode timestamps checking. */
#define WT_DHANDLE_TS_NEVER 0x040u              /* Handle never using timestamps checking. */
#define WT_DHANDLE_TS_ORDERED 0x080u            /* Handle using ordered timestamps checking. */
#define WT_DHANDLE_VERB_TS_WRITE 0x100u         /* Handle verbose logging for timestamps usage. */
                                                /* AUTOMATIC FLAG VALUE GENERATION STOP */
    uint32_t ts_flags;
};
