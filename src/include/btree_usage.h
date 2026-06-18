/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Compact per-btree usage statistics collected via session-level sampling.
 *
 * Every WT_BTREE_USAGE_SAMPLE_PERIOD cursor operations a session samples the current btree; leaf
 * splits are sampled separately at 1/WT_BTREE_USAGE_SPLIT_PERIOD. Counters are period-scaled (a
 * fired sample adds its period, sums add period * value) so every value is a true-scale estimate
 * and ratios across stats sampled at different periods stay correct.
 *
 * The values are held in a flat int64 array indexed by the WT_BTREE_USAGE_* enum generated from
 * WT_BTREE_USAGE_STATS_LIST below. That single list is the source of truth for the index, the
 * storage slot, the sampling site, and (via the same macro, expanded elsewhere) the human-readable
 * description -- which matters because the connection statistics cursor exposes this array as
 * virtual stat entries, one per top-N (plus sample) btree, without bloating WT_CONNECTION_STATS.
 * int64 matches the cursor's native type, so exposure is a widening-free copy, and 64 bits removes
 * any period-scaled overflow concern.
 *
 * Inline sampling functions live in btree_usage_inline.h.
 */

#pragma once

/* Approximate number of cursor ops between samples per session. */
#define WT_BTREE_USAGE_SAMPLE_PERIOD 1000

/* Approximate number of leaf page splits between split-stat samples. */
#define WT_BTREE_USAGE_SPLIT_PERIOD 64

/* Number of top-N btree slots maintained by the sweep server (plus one random "sample" slot). */
#define WT_BTREE_USAGE_TOP_N 16

/* Maximum URI length stored in a snapshot slot (truncated if longer). */
#define WT_BTREE_USAGE_URI_MAX 256

/*
 * Once a sum-of-squares slot reaches this, stop accumulating variance: the mean stays accurate and
 * we avoid signed overflow. 2^62 leaves ample room below INT64_MAX for one more period-scaled add.
 * This would understate the variance for extreme situations.
 */
#define WT_BTREE_USAGE_SUMSQ_SATURATE INT64_C(0x4000000000000000)

/* Operation indices passed to __wt_btree_usage_op_sample (op order within each position block). */
#define WT_BTREE_USAGE_OP_INSERT 0
#define WT_BTREE_USAGE_OP_UPDATE 1
#define WT_BTREE_USAGE_OP_REMOVE 2
#define WT_BTREE_USAGE_OP_SEARCH 3
#define WT_BTREE_USAGE_OP_SEARCH_NEAR 4
#define WT_BTREE_USAGE_OP_MODIFY 5
#define WT_BTREE_USAGE_OP_INSERT_OVERWRITE 6 /* insert that overwrote an existing key */
#define WT_BTREE_USAGE_OP_COUNT 7

/*
 * Leaf position of the sampled op. LEFT/RIGHT are the edge leaves of the tree; NEAR_LEFT/NEAR_RIGHT
 * are non-edge leaves whose parent is the edge internal page (within ~one fan-out of the edge),
 * which captures wide "Pareto" locality; MIDDLE is everything else. A sole-leaf "both" folds into
 * middle (see the op sampler).
 */
#define WT_BTREE_USAGE_POS_LEFT 0
#define WT_BTREE_USAGE_POS_NEAR_LEFT 1
#define WT_BTREE_USAGE_POS_MIDDLE 2
#define WT_BTREE_USAGE_POS_NEAR_RIGHT 3
#define WT_BTREE_USAGE_POS_RIGHT 4
#define WT_BTREE_USAGE_POS_COUNT 5

/* Key-size levels, indexed from the leaf up; deeper levels are reserved for a future hook. */
#define WT_BTREE_USAGE_LEVEL_COUNT 4
#define WT_BTREE_USAGE_LEVEL_LEAF 0

/* Field offsets within a key-size level's {count, sum, sum-of-squares} triple. */
#define WT_BTREE_USAGE_KEY_FIELD_N 0
#define WT_BTREE_USAGE_KEY_FIELD_SUM 1
#define WT_BTREE_USAGE_KEY_FIELD_SUMSQ 2

/*
 * WT_BTREE_USAGE_STATS_LIST --
 *     The single source of truth: (symbol, description) per stat slot. Expanded into the index enum
 *     here and into the description table at the cursor. Descriptions are field-level phrases; the
 *     cursor prepends a per-slot prefix ("table N: ") so a final stat reads, e.g.,
 *     "table 0: number of sampled inserts on the rightmost leaf".
 *
 *     Order matters: the op block is position-major (left, then middle, then right) with op order
 *     insert/update/remove/search/search_near/modify/insert-overwrite, so indexing by position
 *     and op lands on the matching symbol; the key block is level-major with {n, sum,
 *     sum-of-squares} per level.
 */
#define WT_BTREE_USAGE_STATS_LIST(WT_X)                                                            \
    WT_X(LEFT_INSERT, "number of sampled inserts on the leftmost leaf")                            \
    WT_X(LEFT_UPDATE, "number of sampled updates on the leftmost leaf")                            \
    WT_X(LEFT_REMOVE, "number of sampled removes on the leftmost leaf")                            \
    WT_X(LEFT_SEARCH, "number of sampled searches on the leftmost leaf")                           \
    WT_X(LEFT_SEARCH_NEAR, "number of sampled search-near calls on the leftmost leaf")             \
    WT_X(LEFT_MODIFY, "number of sampled modifies on the leftmost leaf")                           \
    WT_X(LEFT_INSERT_OVERWRITE, "number of sampled insert-overwrites on the leftmost leaf")        \
    WT_X(NEAR_LEFT_INSERT, "number of sampled inserts near the leftmost leaf")                     \
    WT_X(NEAR_LEFT_UPDATE, "number of sampled updates near the leftmost leaf")                     \
    WT_X(NEAR_LEFT_REMOVE, "number of sampled removes near the leftmost leaf")                     \
    WT_X(NEAR_LEFT_SEARCH, "number of sampled searches near the leftmost leaf")                    \
    WT_X(NEAR_LEFT_SEARCH_NEAR, "number of sampled search-near calls near the leftmost leaf")      \
    WT_X(NEAR_LEFT_MODIFY, "number of sampled modifies near the leftmost leaf")                    \
    WT_X(NEAR_LEFT_INSERT_OVERWRITE, "number of sampled insert-overwrites near the leftmost leaf") \
    WT_X(MIDDLE_INSERT, "number of sampled inserts on a middle leaf")                              \
    WT_X(MIDDLE_UPDATE, "number of sampled updates on a middle leaf")                              \
    WT_X(MIDDLE_REMOVE, "number of sampled removes on a middle leaf")                              \
    WT_X(MIDDLE_SEARCH, "number of sampled searches on a middle leaf")                             \
    WT_X(MIDDLE_SEARCH_NEAR, "number of sampled search-near calls on a middle leaf")               \
    WT_X(MIDDLE_MODIFY, "number of sampled modifies on a middle leaf")                             \
    WT_X(MIDDLE_INSERT_OVERWRITE, "number of sampled insert-overwrites on a middle leaf")          \
    WT_X(NEAR_RIGHT_INSERT, "number of sampled inserts near the rightmost leaf")                   \
    WT_X(NEAR_RIGHT_UPDATE, "number of sampled updates near the rightmost leaf")                   \
    WT_X(NEAR_RIGHT_REMOVE, "number of sampled removes near the rightmost leaf")                   \
    WT_X(NEAR_RIGHT_SEARCH, "number of sampled searches near the rightmost leaf")                  \
    WT_X(NEAR_RIGHT_SEARCH_NEAR, "number of sampled search-near calls near the rightmost leaf")    \
    WT_X(NEAR_RIGHT_MODIFY, "number of sampled modifies near the rightmost leaf")                  \
    WT_X(                                                                                          \
      NEAR_RIGHT_INSERT_OVERWRITE, "number of sampled insert-overwrites near the rightmost leaf")  \
    WT_X(RIGHT_INSERT, "number of sampled inserts on the rightmost leaf")                          \
    WT_X(RIGHT_UPDATE, "number of sampled updates on the rightmost leaf")                          \
    WT_X(RIGHT_REMOVE, "number of sampled removes on the rightmost leaf")                          \
    WT_X(RIGHT_SEARCH, "number of sampled searches on the rightmost leaf")                         \
    WT_X(RIGHT_SEARCH_NEAR, "number of sampled search-near calls on the rightmost leaf")           \
    WT_X(RIGHT_MODIFY, "number of sampled modifies on the rightmost leaf")                         \
    WT_X(RIGHT_INSERT_OVERWRITE, "number of sampled insert-overwrites on the rightmost leaf")      \
    WT_X(KEY_LEAF_N, "sampled key-size observation count at the leaf")                             \
    WT_X(KEY_LEAF_SUM, "sampled key-size byte sum at the leaf")                                    \
    WT_X(KEY_LEAF_SUMSQ, "sampled key-size sum of squares at the leaf")                            \
    WT_X(KEY_L1_N, "sampled key-size observation count one level above the leaf")                  \
    WT_X(KEY_L1_SUM, "sampled key-size byte sum one level above the leaf")                         \
    WT_X(KEY_L1_SUMSQ, "sampled key-size sum of squares one level above the leaf")                 \
    WT_X(KEY_L2_N, "sampled key-size observation count two levels above the leaf")                 \
    WT_X(KEY_L2_SUM, "sampled key-size byte sum two levels above the leaf")                        \
    WT_X(KEY_L2_SUMSQ, "sampled key-size sum of squares two levels above the leaf")                \
    WT_X(KEY_L3_N, "sampled key-size observation count three or more levels above the leaf")       \
    WT_X(KEY_L3_SUM, "sampled key-size byte sum three or more levels above the leaf")              \
    WT_X(KEY_L3_SUMSQ, "sampled key-size sum of squares three or more levels above the leaf")      \
    WT_X(DATA_N, "sampled value-size observation count")                                           \
    WT_X(DATA_SUM, "sampled value-size byte sum")                                                  \
    WT_X(DATA_SUMSQ, "sampled value-size sum of squares")                                          \
    WT_X(SPLIT_COUNT, "estimated number of leaf splits")                                           \
    WT_X(SPLIT_PAGES, "number of pages resulting from sampled leaf splits")                        \
    WT_X(SPLIT_KEYS, "estimated total keys across sampled splitting pages")

/* Generated index enum; WT_BTREE_USAGE_STAT_COUNT is the array size. */
#define WT_BTREE_USAGE_ENUM_ENTRY(name, desc) WT_BTREE_USAGE_##name,
enum { WT_BTREE_USAGE_STATS_LIST(WT_BTREE_USAGE_ENUM_ENTRY) WT_BTREE_USAGE_STAT_COUNT };
#undef WT_BTREE_USAGE_ENUM_ENTRY

/* Block bases and index helpers for the contiguous, computed-index blocks. */
#define WT_BTREE_USAGE_OP_BASE WT_BTREE_USAGE_LEFT_INSERT
#define WT_BTREE_USAGE_OP_IDX(pos, op) \
    (WT_BTREE_USAGE_OP_BASE + (pos)*WT_BTREE_USAGE_OP_COUNT + (op))
#define WT_BTREE_USAGE_KEY_BASE WT_BTREE_USAGE_KEY_LEAF_N
#define WT_BTREE_USAGE_KEY_IDX(level, field) (WT_BTREE_USAGE_KEY_BASE + (level)*3 + (field))

/*
 * Snapshot slot map: the activity-ranked top-N, then a pinned slot for the history store -- always
 * present regardless of rank, identified by WT itself (WT_IS_HS) -- then the random "sample" slot.
 */
#define WT_BTREE_USAGE_SLOT_PIN_HS WT_BTREE_USAGE_TOP_N
#define WT_BTREE_USAGE_SLOT_SAMPLE (WT_BTREE_USAGE_TOP_N + 1)
#define WT_BTREE_USAGE_SLOT_COUNT (WT_BTREE_USAGE_TOP_N + 2)

/*
 * The statistics cursor appends two views of the snapshot to the connection stats:
 *  (1) A leaderboard -- WT_BTREE_USAGE_TOP_N ranks, each carrying the btree id and its access
 * total, keyed by rank ("usage_rank_01" .. "_16") and sorted so rank 01 is the most active. (2)
 * Per-btree
 * detail -- every slot's full field set (the WT_BTREE_USAGE_STAT_COUNT sampled stats plus the
 * persistence streak), keyed by identity ("usage_(id=N)_<uri>" / "usage_hs" / "usage_sample") so a
 * btree's series stays stable across intervals regardless of which rank slot it occupies. All
 * values are numeric (identity lives in the key), so the whole snapshot is FTDC-native.
 */
#define WT_BTREE_USAGE_RANK_FIELDS 2
#define WT_BTREE_USAGE_RANK_BTREE_ID 0
#define WT_BTREE_USAGE_RANK_ACCESS_TOTAL 1
#define WT_BTREE_USAGE_LEADERBOARD_COUNT (WT_BTREE_USAGE_TOP_N * WT_BTREE_USAGE_RANK_FIELDS)

#define WT_BTREE_USAGE_DETAIL_STREAK \
    WT_BTREE_USAGE_STAT_COUNT /* persistence streak follows the sampled stats */
#define WT_BTREE_USAGE_DETAIL_TYPE \
    (WT_BTREE_USAGE_STAT_COUNT + 1) /* WT_BTREE_TYPE of the btree (row vs column store) */
#define WT_BTREE_USAGE_DETAIL_FIELDS (WT_BTREE_USAGE_STAT_COUNT + 2)
#define WT_BTREE_USAGE_DETAIL_COUNT (WT_BTREE_USAGE_SLOT_COUNT * WT_BTREE_USAGE_DETAIL_FIELDS)

/*
 * FTDC usage schema version, emitted as a connection-level entry so a reader can tell which field
 * set to expect. Bump when fields are renamed or their meaning changes (adding fields does not
 * require a bump -- readers match by name and ignore unknown fields).
 */
#define WT_BTREE_USAGE_VERSION 1

/* Connection-level summary entries: schema version, then the count of btrees with any activity. */
#define WT_BTREE_USAGE_SUMMARY_VERSION 0
#define WT_BTREE_USAGE_SUMMARY_ACTIVE 1
#define WT_BTREE_USAGE_SUMMARY_COUNT 2

#define WT_BTREE_USAGE_VIRTUAL_COUNT \
    (WT_BTREE_USAGE_SUMMARY_COUNT + WT_BTREE_USAGE_LEADERBOARD_COUNT + WT_BTREE_USAGE_DETAIL_COUNT)

/*
 * WT_BTREE_USAGE_STATS --
 *     Compact per-btree usage stats embedded in WT_BTREE. Updated via relaxed atomics; slight
 *     imprecision is acceptable for statistics.
 */
struct __wt_btree_usage_stats {
    wt_shared int64_t v[WT_BTREE_USAGE_STAT_COUNT];
};

/*
 * WT_BTREE_USAGE_SNAPSHOT --
 *     Point-in-time copy of one btree's usage stats in the connection-level top-N array. Written by
 *     the sweep server, read by the statistics cursor. The btree_id correlates with the URI logged
 *     at each sweep; the URI is kept here too for the sweep's log line.
 */
struct __wt_btree_usage_snapshot {
    WT_BTREE_USAGE_STATS stats;
    int64_t score;                    /* Total sampled op count used for ranking. */
    int64_t streak;                   /* Consecutive sweep intervals in the top set. */
    uint32_t btree_id;                /* WT btree ID (btree->id). */
    uint8_t type;                     /* WT_BTREE_TYPE: row vs column store. */
    bool valid;                       /* Slot contains a valid snapshot. */
    char uri[WT_BTREE_USAGE_URI_MAX]; /* Btree URI, NUL-terminated, truncated if longer. */
};
