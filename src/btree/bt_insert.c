/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
/*
 * __validate_next_stack --
 *     Verify that for each level in the provided next_stack that higher levels on the stack point
 *     to larger inserts than lower levels, and all inserts are larger than the srch_key used in
 *     building the next_stack.
 */
static WT_INLINE int
__validate_next_stack(
  WT_SESSION_IMPL *session, WT_INSERT *next_stack[WT_SKIP_MAXDEPTH], WT_ITEM *srch_key)
{
    WT_COLLATOR *collator;
    WT_ITEM lower_key, upper_key;
    int32_t cmp, i;

    /*
     * Hide the flag check for non-diagnostics builds, too.
     */
#ifndef HAVE_DIAGNOSTIC
    return (0);
#endif

    collator = S2BT(session)->collator;
    WT_CLEAR(upper_key);
    WT_CLEAR(lower_key);
    cmp = 0;

    for (i = WT_SKIP_MAXDEPTH - 2; i >= 0; i--) {

        /* If lower levels point to the end of the skiplist, higher levels must as well. */
        if (next_stack[i] == NULL)
            WT_ASSERT_ALWAYS(session, next_stack[i + 1] == NULL,
              "Invalid next_stack: Level %d is NULL but higher level %d has pointer %p", i, i + 1,
              (void *)next_stack[i + 1]);

        /* We only need to compare when both levels point to different, non-NULL inserts. */
        if (next_stack[i] == NULL || next_stack[i + 1] == NULL ||
          next_stack[i] == next_stack[i + 1])
            continue;

        lower_key.data = WT_INSERT_KEY(next_stack[i]);
        lower_key.size = WT_INSERT_KEY_SIZE(next_stack[i]);

        upper_key.data = WT_INSERT_KEY(next_stack[i + 1]);
        upper_key.size = WT_INSERT_KEY_SIZE(next_stack[i + 1]);

        WT_RET(__wt_compare(session, collator, &upper_key, &lower_key, &cmp));
        WT_ASSERT_ALWAYS(session, cmp >= 0,
          "Invalid next_stack: Lower level points to larger key: Level %d = %s, Level %d = %s", i,
          (char *)lower_key.data, i + 1, (char *)upper_key.data);
    }

    if (next_stack[0] != NULL) {
        /*
         * Finally, confirm that next_stack[0] is greater than srch_key. We've already confirmed
         * that all keys on higher levels are larger than next_stack[0] and therefore also larger
         * than srch_key.
         */
        lower_key.data = WT_INSERT_KEY(next_stack[0]);
        lower_key.size = WT_INSERT_KEY_SIZE(next_stack[0]);

        WT_RET(__wt_compare(session, collator, srch_key, &lower_key, &cmp));
        WT_ASSERT_ALWAYS(session, cmp < 0,
          "Invalid next_stack: Search key is larger than keys on next_stack: srch_key = %s, "
          "next_stack[0] = %s",
          (char *)srch_key->data, (char *)lower_key.data);
    }

    return (0);
}

/*
 * __wt_search_insert --
 *     Search a row-store insert list, creating a skiplist stack as we go.
 */
int
__wt_search_insert(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_INSERT_HEAD *ins_head, WT_ITEM *srch_key)
{
    WT_BTREE *btree;
    WT_COLLATOR *collator;
    WT_INSERT *ins, **insp, *last_ins;
    WT_ITEM key;
    size_t match, skiphigh, skiplow;
    int cmp, i;

    btree = S2BT(session);
    collator = btree->collator;
    cmp = 0; /* -Wuninitialized */

    /*
     * The insert list is a skip list: start at the highest skip level, then go as far as possible
     * at each level before stepping down to the next.
     */
    match = skiphigh = skiplow = 0;
    ins = last_ins = NULL;
    for (i = WT_SKIP_MAXDEPTH - 1, insp = &ins_head->head[i]; i >= 0;) {
        /*
         * The algorithm requires that the skip list insert pointer is only read once within the
         * loop. While the compiler can change the code in a way that it reads the insert pointer
         * value from memory again in the following code.
         *
         * In addition, a CPU with weak memory ordering, such as ARM, may reorder the reads and read
         * a stale value. It is not OK and the reason is explained in the following comment.
         *
         * Place an acquire barrier here to avoid these issues.
         */
        WT_ACQUIRE_READ_WITH_BARRIER(ins, *insp);
        if (ins == NULL) {
            cbt->next_stack[i] = NULL;
            cbt->ins_stack[i--] = insp--;
            continue;
        }

        /*
         * Comparisons may be repeated as we drop down skiplist levels; don't repeat comparisons,
         * they might be expensive.
         */
        if (ins != last_ins) {
            last_ins = ins;
            key.data = WT_INSERT_KEY(ins);
            key.size = WT_INSERT_KEY_SIZE(ins);
            /*
             * We have an optimization to reduce the number of bytes we need to compare during the
             * search if we know a prefix of the search key matches the keys we have already
             * compared on the upper stacks. This works because we know the keys become denser down
             * the stack.
             *
             * However, things become tricky if we have another key inserted concurrently next to
             * the search key. The current search may or may not see the concurrently inserted key
             * but it should always see a valid skip list. In other words,
             *
             * 1) at any level of the list, keys are in sorted order;
             *
             * 2) if a reader sees a key in level N, that key is also in all levels below N.
             *
             * Otherwise, we may wrongly skip the comparison of a prefix and land on the wrong spot.
             * Here's an example:
             *
             * Suppose we have a skip list:
             *
             * L1: AA -> BA
             *
             * L0: AA -> BA
             *
             * and we want to search AB and a key AC is inserted concurrently. If we see the
             * following skip list in the search:
             *
             * L1: AA -> AC -> BA
             *
             * L0: AA -> BA
             *
             * Since we have compared with AA and AC on level 1 before dropping down to level 0, we
             * decide we can skip comparing the first byte of the key. However, since we don't see
             * AC on level 0, we compare with BA and wrongly skip the comparison with prefix B.
             *
             * On architectures with strong memory ordering, the requirement is satisfied by
             * inserting the new key to the skip list from lower stack to upper stack using an
             * atomic compare and swap operation, which functions as a full barrier. However, it is
             * not enough on the architecture that has weaker memory ordering, such as ARM.
             * Therefore, an extra acquire barrier is needed for these platforms.
             */
            match = WT_MIN(skiplow, skiphigh);
            WT_RET(__wt_compare_skip(session, collator, srch_key, &key, &cmp, &match));
        }

        if (cmp > 0) { /* Keep going at this level */
            insp = &ins->next[i];
            skiplow = match;
        } else if (cmp < 0) { /* Drop down a level */
            cbt->next_stack[i] = ins;
            cbt->ins_stack[i--] = insp--;
            skiphigh = match;
        } else
            for (; i >= 0; i--) {
                /*
                 * It is possible that we read an old value down the stack due to read reordering on
                 * CPUs with weak memory ordering. Add an acquire barrier to avoid this issue.
                 */
                WT_ACQUIRE_READ_WITH_BARRIER(cbt->next_stack[i], ins->next[i]);
                cbt->ins_stack[i] = &ins->next[i];
            }
    }

    /*
     * For every insert element we review, we're getting closer to a better choice; update the
     * compare field to its new value. If we went past the last item in the list, return the last
     * one: that is used to decide whether we are positioned in a skiplist.
     */
    cbt->compare = -cmp;
    cbt->ins = (ins != NULL) ? ins : last_ins;
    cbt->ins_head = ins_head;

    /*
     * This is an expensive call on a performance-critical path, so we only want to enable it behind
     * the stress_skiplist session flag.
     */
    if (FLD_ISSET(S2C(session)->debug_flags, WT_CONN_DEBUG_STRESS_SKIPLIST))
        WT_RET(__validate_next_stack(session, cbt->next_stack, srch_key));

    return (0);
}