/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * This file defines __wt_cursor_join_iter, __wt_cursor_join_endpoint, __wt_cursor_join_entry,
 * __wt_cursor_join and related definitions.
 */

/*
 * A join iterator structure is used to generate candidate primary keys. It is the responsibility of
 * the caller of the iterator to filter these primary key against the other conditions of the join
 * before returning them the caller of WT_CURSOR::next.
 *
 * For a conjunction join (the default), entry_count will be 1, meaning that the iterator only
 * consumes the first entry (WT_CURSOR_JOIN_ENTRY). That is, it successively returns primary keys
 * from a cursor for the first index that was joined. When the values returned by that cursor are
 * exhausted, the iterator has completed. For a disjunction join, exhausting a cursor just means
 * that the iterator advances to the next entry. If the next entry represents an index, a new cursor
 * is opened and primary keys from that index are then successively returned.
 *
 * When positioned on an entry that represents a nested join, a new child iterator is created that
 * will be bound to the nested WT_CURSOR_JOIN. That iterator is then used to generate candidate
 * primary keys. When its iteration is completed, that iterator is destroyed and the parent iterator
 * advances to the next entry. Thus, depending on how deeply joins are nested, a similarly deep
 * stack of iterators is created.
 */
struct __wt_cursor_join_iter {
    WT_SESSION_IMPL *session;
    WT_CURSOR_JOIN *cjoin;
    WT_CURSOR_JOIN_ENTRY *entry;
    WT_CURSOR_JOIN_ITER *child;
    WT_CURSOR *cursor; /* has null projection */
    WT_ITEM *curkey;   /* primary key */
    WT_ITEM idxkey;
    u_int entry_pos;   /* the current entry */
    u_int entry_count; /* entries to walk */
    u_int end_pos;     /* the current endpoint */
    u_int end_count;   /* endpoints to walk */
    u_int end_skip;    /* when testing for inclusion */
                       /* can we skip current end? */
    bool positioned;
    bool is_equal;
};

/*
 * A join endpoint represents a positioned cursor that is 'captured' by a WT_SESSION::join call.
 */
struct __wt_cursor_join_endpoint {
    WT_ITEM key;
    uint8_t recno_buf[10]; /* holds packed recno */
    WT_CURSOR *cursor;

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CURJOIN_END_EQ 0x1u         /* include values == cursor */
#define WT_CURJOIN_END_GT 0x2u         /* include values >  cursor */
#define WT_CURJOIN_END_LT 0x4u         /* include values <  cursor */
#define WT_CURJOIN_END_OWN_CURSOR 0x8u /* must close cursor */
/* AUTOMATIC FLAG VALUE GENERATION STOP 8 */
#define WT_CURJOIN_END_GE (WT_CURJOIN_END_GT | WT_CURJOIN_END_EQ)
#define WT_CURJOIN_END_LE (WT_CURJOIN_END_LT | WT_CURJOIN_END_EQ)
    uint8_t flags; /* range for this endpoint */
};
#define WT_CURJOIN_END_RANGE(endp) \
    ((endp)->flags & (WT_CURJOIN_END_GT | WT_CURJOIN_END_EQ | WT_CURJOIN_END_LT))

/*
 * Each join entry typically represents an index's participation in a join. For example, if 'k' is
 * an index, then "t.k > 10 && t.k < 20" would be represented by a single entry, with two endpoints.
 * When the index and subjoin fields are NULL, the join is on the main table. When subjoin is
 * non-NULL, there is a nested join clause.
 */
struct __wt_cursor_join_entry {
    WT_INDEX *index;
    WT_CURSOR *main;           /* raw main table cursor */
    WT_CURSOR_JOIN *subjoin;   /* a nested join clause */
    WT_BLOOM *bloom;           /* Bloom filter handle */
    char *repack_format;       /* target format for repack */
    uint32_t bloom_bit_count;  /* bits per item in bloom */
    uint32_t bloom_hash_count; /* hash functions in bloom */
    uint64_t count;            /* approx number of matches */

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CURJOIN_ENTRY_BLOOM 0x1u           /* use a bloom filter */
#define WT_CURJOIN_ENTRY_DISJUNCTION 0x2u     /* endpoints are or-ed */
#define WT_CURJOIN_ENTRY_FALSE_POSITIVES 0x4u /* don't filter false pos */
#define WT_CURJOIN_ENTRY_OWN_BLOOM 0x8u       /* this entry owns the bloom */
                                              /* AUTOMATIC FLAG VALUE GENERATION STOP 8 */
    uint8_t flags;

    WT_CURSOR_JOIN_ENDPOINT *ends; /* reference endpoints */
    size_t ends_allocated;
    u_int ends_next;

    WT_JOIN_STATS stats; /* Join statistics */
};

struct __wt_cursor_join {
    WT_CURSOR iface;

    WT_TABLE *table;
    const char *projection;
    WT_CURSOR *main;           /* main table with projection */
    WT_CURSOR_JOIN *parent;    /* parent of nested group */
    WT_CURSOR_JOIN_ITER *iter; /* chain of iterators */
    WT_CURSOR_JOIN_ENTRY *entries;
    size_t entries_allocated;
    u_int entries_next;
    uint8_t recno_buf[10]; /* holds packed recno */

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CURJOIN_DISJUNCTION 0x1u /* Entries are or-ed */
#define WT_CURJOIN_ERROR 0x2u       /* Error in initialization */
#define WT_CURJOIN_INITIALIZED 0x4u /* Successful initialization */
                                    /* AUTOMATIC FLAG VALUE GENERATION STOP 8 */
    uint8_t flags;
};
