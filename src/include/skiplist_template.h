/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * PLEASE READ THE FOLLOWING DESCRIPTION CAREFULLY BEFORE INCLUDING THIS FILE.
 *
 * This is a template for building skip lists. Ideally, we would like to use C++ templates, but
 * because this language feature is not available in C, we have to do use the following trick: Set
 * up all relevant function names, type names, and "template" parameters using macros, and only then
 * include this file. This will define the corresponding templatized functions and types.
 */

// --------------- vv this goes to a different file vv ------------------

#define WT_SKIP_T_CURSOR WT_CURSOR_BTREE
#define WT_SKIP_T_ELEMENT WT_INSERT
#define WT_SKIP_T_HEAD WT_INSERT_HEAD
#define WT_SKIP_T_KEY WT_ITEM

#define WT_SKIP_T_KEY_ASSIGN(key, element) \
    (key)->data = WT_INSERT_KEY(element);  \
    (key)->size = WT_INSERT_KEY_SIZE(element);
#define WT_SKIP_T_KEY_COMPARE(session, srch_key, key, cmp) \
    WT_RET(__wt_compare(session, S2BT(session)->collator, srch_key, key, cmp));
#define WT_SKIP_T_KEY_COMPARE_SKIP(session, srch_key, key, cmp, match) \
    WT_RET(__wt_compare_skip(session, S2BT(session)->collator, srch_key, key, cmp, match));

#define WT_SKIP_T_FN_APPEND_SEARCH __wt_skip_append_search__insert
#define WT_SKIP_T_FN_SEARCH __wt_skip_search__insert
#define WT_SKIP_T_FN_INSERT __wt_skip_insert__insert

// --------------- ^^ this goes to a different file ^^ ------------------

/*
 * WT_SKIP_T_FN_APPEND_SEARCH --
 *     Fast append search of a skiplist, creating a skiplist stack as we go. In other words, we
 * quickly check if the given item can be appended to the end of the skiplist, in which case we set
 * the "done" flag to true, and create the stack accordingly. If the key we would like to insert is
 * an exact match, return it via the cursor's "tmp" field.
 */
static inline int
WT_SKIP_T_FN_APPEND_SEARCH(WT_SESSION_IMPL *session, WT_SKIP_T_CURSOR *cbt,
  WT_SKIP_T_HEAD *ins_head, WT_SKIP_T_KEY *srch_key, bool *donep)
{
    WT_SKIP_T_ELEMENT *ins;
    WT_SKIP_T_KEY key;
    int cmp, i;

    *donep = 0;

    if ((ins = WT_SKIP_LAST(ins_head)) == NULL)
        return (0);
    /*
     * Since the head of the skip list doesn't get mutated within this function, the compiler may
     * move this assignment above within the loop below if it needs to (and may read a different
     * value on each loop due to other threads mutating the skip list).
     *
     * Place a read barrier here to avoid this issue.
     */
    WT_READ_BARRIER();
    WT_SKIP_T_KEY_ASSIGN(&key, ins);

    WT_SKIP_T_KEY_COMPARE(session, srch_key, &key, &cmp);
    if (cmp >= 0) {
        /*
         * !!!
         * We may race with another appending thread.
         *
         * To catch that case, rely on the atomic pointer read above
         * and set the next stack to NULL here.  If we have raced with
         * another thread, one of the next pointers will not be NULL by
         * the time they are checked against the next stack inside the
         * serialized insert function.
         */
        for (i = WT_SKIP_MAXDEPTH - 1; i >= 0; i--) {
            cbt->ins_stack[i] = (i == 0)  ? &ins->next[0] :
              (ins_head->tail[i] != NULL) ? &ins_head->tail[i]->next[i] :
                                            &ins_head->head[i];
            cbt->next_stack[i] = NULL;
        }
        cbt->compare = -cmp;
        cbt->ins = ins;
        cbt->ins_head = ins_head;

        /*
         * If we find an exact match, copy the key into the temporary buffer, our callers expect to
         * find it there.
         */
        if (cbt->compare == 0)
            WT_SKIP_T_KEY_ASSIGN(cbt->tmp, cbt->ins);

        *donep = 1;
    }
    return (0);
}

/*
 * WT_SKIP_T_FN_SEARCH --
 *     Search a skiplist, creating a skiplist stack as we go.
 */
static inline int
WT_SKIP_T_FN_SEARCH(WT_SESSION_IMPL *session, WT_SKIP_T_CURSOR *cbt, WT_SKIP_T_HEAD *ins_head,
  WT_SKIP_T_KEY *srch_key)
{
    WT_INSERT *ins, **insp, *last_ins;
    WT_ITEM key;
    size_t match, skiphigh, skiplow;
    int cmp, i;

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
         * Place a read barrier here to avoid these issues.
         */
        WT_ORDERED_READ_WEAK_MEMORDER(ins, *insp);
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
            WT_SKIP_T_KEY_ASSIGN(&key, ins);
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
             * Therefore, an extra read barrier is needed for these platforms.
             */
            match = WT_MIN(skiplow, skiphigh);
            WT_SKIP_T_KEY_COMPARE_SKIP(session, srch_key, &key, &cmp, &match);
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
                 * CPUs with weak memory ordering. Add a read barrier to avoid this issue.
                 */
                WT_ORDERED_READ_WEAK_MEMORDER(cbt->next_stack[i], ins->next[i]);
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
    // if (FLD_ISSET(S2C(session)->debug_flags, WT_CONN_DEBUG_STRESS_SKIPLIST))
    //     WT_RET(__validate_next_stack(session, cbt->next_stack, srch_key));

    return (0);
}

/*
 * __wt_insert_serial --
 *     Insert a skiplist entry. The cursor must be already positioned.
 */
static inline int
WT_SKIP_T_FN_INSERT(WT_SESSION_IMPL *session, WT_SPINLOCK *lock, WT_SKIP_T_CURSOR *cbt,
  WT_SKIP_T_ELEMENT *new_ins, u_int skipdepth, bool exclusive)
{
    WT_DECL_RET;
    WT_SKIP_T_ELEMENT ***ins_stack;
    WT_SKIP_T_ELEMENT *old_ins;
    WT_SKIP_T_HEAD *ins_head;
    u_int i;
    bool simple;

    ins_head = cbt->ins_head;
    ins_stack = cbt->ins_stack;

    /* The cursor should be positioned. */
    WT_ASSERT(session, ins_stack[0] != NULL);

    /*
     * Check if this is the simple case: If we do not need to modify the "tail" of the skiplist, we
     * do not need to acquire the lock.
     */
    simple = true;
    for (i = 0; i < skipdepth; i++)
        if (new_ins->next[i] == NULL)
            simple = false;

    /*
     * Update the skiplist elements referencing the new item. If we fail connecting one of
     * the upper levels in the skiplist, return success: the levels we updated are correct and
     * sufficient. Even though we don't get the benefit of the memory we allocated, we can't roll
     * back.
     *
     * All structure setup must be flushed before the structure is entered into the list. We need a
     * write barrier here, our callers depend on it. Don't pass complex arguments to the macro, some
     * implementations read the old value multiple times.
     */
    if (simple)
        for (i = 0; i < skipdepth; i++) {
            /*
             * The insert stack position must be read only once - if the compiler chooses to re-read
             * the shared variable it could lead to skip list corruption. Specifically the
             * comparison against the next pointer might indicate that the skip list location is
             * still valid, but that may no longer be true when the atomic_cas operation executes.
             *
             * Place a read barrier here to avoid this issue.
             */
            WT_ORDERED_READ(old_ins, *ins_stack[i]);
            if (old_ins != new_ins->next[i] || !__wt_atomic_cas_ptr(ins_stack[i], old_ins, new_ins))
                return (i == 0 ? WT_RESTART : 0);
        }
    else {
        if (!exclusive)
            __wt_spin_lock(session, lock);

        for (i = 0; i < skipdepth; i++) {
            /*
             * The insert stack position must be read only once - if the compiler chooses to re-read
             * the shared variable it could lead to skip list corruption. Specifically the
             * comparison against the next pointer might indicate that the skip list location is
             * still valid, but that may no longer be true when the atomic_cas operation executes.
             *
             * Place a read barrier here to avoid this issue.
             */
            WT_ORDERED_READ(old_ins, *ins_stack[i]);
            if (old_ins != new_ins->next[i] ||
              !__wt_atomic_cas_ptr(ins_stack[i], old_ins, new_ins)) {
                ret = (i == 0 ? WT_RESTART : 0);
                break;
            }
            if (ins_head->tail[i] == NULL || ins_stack[i] == &ins_head->tail[i]->next[i])
                ins_head->tail[i] = new_ins;
        }

        if (!exclusive)
            __wt_spin_unlock(session, lock);
    }

    return (ret);
}

/* Undefine the template macros to avoid macro redefinition warnings. */
#undef WT_SKIP_T_CURSOR
#undef WT_SKIP_T_ELEMENT
#undef WT_SKIP_T_HEAD
#undef WT_SKIP_T_KEY

#undef WT_SKIP_T_KEY_ASSIGN
#undef WT_SKIP_T_KEY_COMPARE
#undef WT_SKIP_T_KEY_COMPARE_SKIP

#undef WT_SKIP_T_FN_APPEND_SEARCH
#undef WT_SKIP_T_FN_SEARCH
#undef WT_SKIP_T_FN_INSERT
