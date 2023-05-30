/*
 * __wt_skip_first --
 *     Get the first key on the skip list.
 */
static inline WT_INSERT *
__wt_skip_first(WT_INSERT_HEAD *ins_head, int memorder)
{
    WT_INSERT *ins;
    if (ins_head == NULL)
        return (NULL);
    else {
        WT_C_MEMMODEL_ATOMIC_LOAD(ins, &ins_head->head[0], memorder);
        return (ins);
    }
}

/*
 * __wt_skip_last --
 *     Get the last key on the skip list.
 */
static inline WT_INSERT *
__wt_skip_last(WT_INSERT_HEAD *ins_head, int memorder)
{
    WT_INSERT *ins;
    if (ins_head == NULL)
        return (NULL);
    else {
        WT_C_MEMMODEL_ATOMIC_LOAD(ins, &ins_head->tail[0], memorder);
        return (ins);
    }
}

/*
 * __wt_skip_next --
 *     Get the next key on the skip list.
 */
static inline WT_INSERT *
__wt_skip_next(WT_INSERT *ins, int memorder)
{
    WT_C_MEMMODEL_ATOMIC_LOAD(ins, &ins->next[0], memorder);
    return (ins);
}

#define WT_SKIP_FOREACH(ins, ins_head, memorder)                     \
    for ((ins) = __wt_skip_first(ins_head, memorder); (ins) != NULL; \
         ins = __wt_skip_next(ins, memorder))
