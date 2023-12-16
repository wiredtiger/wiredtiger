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
 *
 * Please refer to skiplist.h for additional details and for an example of these macros.
 */

/*
 * TODO: We would like additional functions here, such as going forward and backward within a skip
 * list, or certain functions for allocating and freeing the skip lists. We will add them later, if
 * we agree that this is the right overall approach.
 */

/*
 * TMPL_FN_APPEND_SEARCH --
 *     Fast append search of a skiplist, creating a skiplist stack as we go. In other words, we
 * quickly check if the given item can be appended to the end of the skiplist, in which case we set
 * the "done" flag to true, and create the stack accordingly. If the key we would like to insert is
 * an exact match, return it via the "key" argument. Return the result of the last comparison via
 * the "cmp" and "element" pointers.
 */
static inline int
TMPL_FN_APPEND_SEARCH(WT_SESSION_IMPL *session, TMPL_HEAD *ins_head, TMPL_ELEMENT ***ins_stack,
  TMPL_ELEMENT **next_stack, TMPL_KEY *srch_key, TMPL_KEY *keyp, TMPL_ELEMENT **elementp, int *cmpp,
  bool *donep)
{
    TMPL_ELEMENT *ins;
    TMPL_KEY key;
    int cmp, i;
    TMPL_KEY_COMPARE_EXTRA_VARS;

    WT_UNUSED(session); /* May or may not be used, depending on the template macros. */

    TMPL_KEY_COMPARE_INIT;
    *donep = 0;

    /*
     * Since the head of the skip list doesn't get mutated within this function, the compiler may
     * move this assignment above within the loop below if it needs to (and may read a different
     * value on each loop due to other threads mutating the skip list).
     *
     * Place a read barrier here to avoid this issue.
     */
    WT_ORDERED_READ(ins, WT_SKIP_LAST(ins_head));

    /* If there's no insert chain to search, we're done. */
    if (ins == NULL)
        return (0);

    TMPL_KEY_ASSIGN(&key, ins);
    TMPL_KEY_COMPARE(session, srch_key, &key, &cmp);
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
            ins_stack[i] = (i == 0)       ? &ins->next[0] :
              (ins_head->tail[i] != NULL) ? &ins_head->tail[i]->next[i] :
                                            &ins_head->head[i];
            next_stack[i] = NULL;
        }

        if (cmpp != NULL)
            *cmpp = -cmp;
        if (elementp != NULL)
            *elementp = ins;

        /*
         * If we find an exact match, copy the key into the temporary buffer, our callers expect to
         * find it there.
         */
        if (cmp == 0 && keyp != NULL)
            TMPL_KEY_ASSIGN(keyp, ins);

        *donep = 1;
    }
    return (0);
}

/*
 * TMPL_FN_INSERT_SEARCH --
 *     Search a skiplist in preparation for an insert, creating a skiplist stack as we go.
 */
static inline int
TMPL_FN_INSERT_SEARCH(WT_SESSION_IMPL *session, TMPL_HEAD *ins_head, TMPL_ELEMENT ***ins_stack,
  TMPL_ELEMENT **next_stack, TMPL_KEY *srch_key, TMPL_KEY *keyp, TMPL_ELEMENT **elementp, int *cmpp)
{
    TMPL_ELEMENT *ins, **insp, *last_ins;
    TMPL_KEY key;
    size_t match, skiphigh, skiplow;
    int cmp, i;
    TMPL_KEY_COMPARE_EXTRA_VARS;

    WT_UNUSED(session); /* May or may not be used, depending on the template macros. */

    TMPL_KEY_COMPARE_INIT;
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
            next_stack[i] = NULL;
            ins_stack[i--] = insp--;
            continue;
        }

        /*
         * Comparisons may be repeated as we drop down skiplist levels; don't repeat comparisons,
         * they might be expensive.
         */
        if (ins != last_ins) {
            last_ins = ins;
            TMPL_KEY_ASSIGN(&key, ins);
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
            TMPL_KEY_COMPARE_SKIP(session, srch_key, &key, &cmp, &match);
        }

        /*
         * When no exact match is found, the search returns the smallest key larger than the
         * searched-for key, or the largest key smaller than the searched-for key, if there is no
         * larger key. Our callers depend on that: specifically, the fixed-length column store
         * cursor code interprets returning a key smaller than the searched-for key to mean the
         * searched-for key is larger than any key on the page. Don't change that behavior, things
         * will break.
         */

        if (cmp > 0) { /* Keep going at this level */
            insp = &ins->next[i];
            skiplow = match;
        } else if (cmp < 0) { /* Drop down a level */
            next_stack[i] = ins;
            ins_stack[i--] = insp--;
            skiphigh = match;
        } else
            for (; i >= 0; i--) {
                /*
                 * It is possible that we read an old value down the stack due to read reordering on
                 * CPUs with weak memory ordering. Add a read barrier to avoid this issue.
                 */
                WT_ORDERED_READ_WEAK_MEMORDER(next_stack[i], ins->next[i]);
                ins_stack[i] = &ins->next[i];
            }
    }

    /*
     * For every insert element we review, we're getting closer to a better choice; update the
     * compare field to its new value. If we went past the last item in the list, return the last
     * one: that is used to decide whether we are positioned in a skiplist.
     */
    if (ins == NULL)
        ins = last_ins;
    if (cmpp != NULL)
        *cmpp = -cmp;
    if (elementp != NULL)
        *elementp = ins;

    if (cmp == 0 && ins != NULL && keyp != NULL)
        TMPL_KEY_ASSIGN(keyp, ins);

    /*
     * This is an expensive call on a performance-critical path, so we only want to enable it behind
     * the stress_skiplist session flag.
     */
    // TODO Do this later.
    //
    // if (FLD_ISSET(S2C(session)->debug_flags, WT_CONN_DEBUG_STRESS_SKIPLIST))
    //     WT_RET(__validate_next_stack(session, cbt->next_stack, srch_key));

    return (0);
}

/*
 * TMPL_FN_INSERT_INTERNAL --
 *     Insert a skiplist entry. The cursor must be already positioned.
 */
static inline int
TMPL_FN_INSERT_INTERNAL(WT_SESSION_IMPL *session, WT_SPINLOCK *lock, TMPL_CURSOR *cbt,
  TMPL_ELEMENT *new_ins, u_int skipdepth, bool exclusive)
{
    WT_DECL_RET;
    TMPL_ELEMENT ***ins_stack;
    TMPL_ELEMENT *old_ins;
    TMPL_HEAD *ins_head;
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
     * Update the skiplist elements referencing the new item. If we fail connecting one of the upper
     * levels in the skiplist, return success: the levels we updated are correct and sufficient.
     * Even though we don't get the benefit of the memory we allocated, we can't roll back.
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

/*
 * TMPL_FN_INSERT --
 *     Insert a skiplist entry. This is a convenience function, which we currently use only for
 * testing.
 */
static inline int
TMPL_FN_INSERT(WT_SESSION_IMPL *session, WT_SPINLOCK *lock, TMPL_HEAD *head, TMPL_ELEMENT *node,
  u_int skipdepth, bool exclusive)
{
    WT_DECL_RET;
    TMPL_CURSOR cursor;
    TMPL_KEY key;
    u_int i;

    memset(&cursor, 0, sizeof(cursor));
    TMPL_KEY_ASSIGN(&key, node);

    do {
        /* Position the cursor. */
        WT_RET(TMPL_FN_INSERT_SEARCH(session, head, cursor.ins_stack, cursor.next_stack, &key, NULL,
          &cursor.ins, &cursor.compare));

        /* We don't currently support duplicate keys, or modifying existing keys. */
        if (cursor.compare == 0 && cursor.ins != NULL)
            return (EEXIST);

        /* Copy the next stack. */
        for (i = 0; i < skipdepth; i++)
            node->next[i] = cursor.next_stack[i];

        /* Insert. */
        cursor.ins_head = head; // XXX
        ret = TMPL_FN_INSERT_INTERNAL(session, lock, &cursor, node, skipdepth, exclusive);
    } while (ret == WT_RESTART);

    return (ret);
}

/*
 * TMPL_FN_CONTAINS --
 *     Check if a key exists. This is a just a convenience function used for testing.
 */
static inline bool
TMPL_FN_CONTAINS(WT_SESSION_IMPL *session, TMPL_HEAD *head, TMPL_KEY *key)
{
    WT_DECL_RET;
    TMPL_CURSOR cursor;
    memset(&cursor, 0, sizeof(cursor));
    ret = TMPL_FN_INSERT_SEARCH(
      session, head, cursor.ins_stack, cursor.next_stack, key, NULL, &cursor.ins, &cursor.compare);
    return (ret == 0 && cursor.compare == 0 && cursor.ins != NULL);
}

/* Undefine the template macros to avoid macro redefinition warnings. */
#undef TMPL_CURSOR
#undef TMPL_ELEMENT
#undef TMPL_HEAD
#undef TMPL_KEY

#undef TMPL_KEY_ASSIGN
#undef TMPL_KEY_COMPARE
#undef TMPL_KEY_COMPARE_EXTRA_VARS
#undef TMPL_KEY_COMPARE_INIT
#undef TMPL_KEY_COMPARE_SKIP

#undef TMPL_FN_APPEND_SEARCH
#undef TMPL_FN_INSERT_SEARCH
#undef TMPL_FN_INSERT_INTERNAL
#undef TMPL_FN_INSERT
#undef TMPL_FN_CONTAINS
