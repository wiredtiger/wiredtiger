/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_addr_string --
 *     Load a buffer with a printable, nul-terminated representation of an address.
 */
const char *
__wt_addr_string(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, WT_ITEM *buf)
{
    WT_BM *bm;
    WT_BTREE *btree;

    btree = S2BT_SAFE(session);

    WT_ASSERT(session, buf != NULL);

    if (addr == NULL || addr_size == 0) {
        buf->data = WT_NO_ADDR_STRING;
        buf->size = strlen(WT_NO_ADDR_STRING);
    } else if (btree == NULL || (bm = btree->bm) == NULL ||
      bm->addr_string(bm, session, buf, addr, addr_size) != 0) {
        buf->data = WT_ERR_STRING;
        buf->size = strlen(WT_ERR_STRING);
    }
    return (buf->data);
}
/*
 * __wti_ref_addr_safe_free --
 *     Any thread that is reviewing the address in a WT_REF, must also be holding a split generation
 *     to ensure that the page index they are using remains valid. Utilize the same generation type
 *     to safely free the address once all users of it have left the generation.
 */
void
__wti_ref_addr_safe_free(WT_SESSION_IMPL *session, void *p, size_t len)
{
    WT_DECL_RET;
    uint64_t split_gen;

    /*
     * The reading thread is always inside a split generation when it reads the ref, so we make use
     * of WT_GEN_SPLIT type generation mechanism to protect the address in a WT_REF rather than
     * creating a whole new generation counter. There are no page splits taking place.
     */
    split_gen = __wt_gen(session, WT_GEN_SPLIT);
    WT_TRET(__wt_stash_add(session, WT_GEN_SPLIT, split_gen, p, len));
    __wt_gen_next(session, WT_GEN_SPLIT, NULL);

    if (ret != 0)
        WT_IGNORE_RET(__wt_panic(session, ret, "fatal error during ref address free"));
}

/*
 * __wt_ref_addr_free --
 *     Free the address in a reference, if necessary.
 */
void
__wt_ref_addr_free(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_PAGE *home;
    void *ref_addr;

    /*
     * In order to free the WT_REF.addr field we need to read and clear the address without a race.
     * The WT_REF may be a child of a page being split, in which case the addr field could be
     * instantiated concurrently which changes the addr field. Once we swap in NULL we effectively
     * own the addr. Then provided the addr is off page we can free the memory.
     *
     * However as we could be the child of a page being split the ref->home pointer which tells us
     * whether the addr is on or off page could change concurrently. To avoid this we save the home
     * pointer before we do the compare and swap. While the second acquire read should be sufficient
     * we use an acquire read on the ref->home pointer as that is the standard mechanism to
     * guarantee we read the current value.
     *
     * We don't reread this value inside loop as if it was to change then we would be pointing at a
     * new parent, which would mean that our ref->addr must have been instantiated and thus we are
     * safe to free it at the end of this function.
     */
    WT_ACQUIRE_READ_WITH_BARRIER(home, ref->home);
    do {
        WT_ACQUIRE_READ_WITH_BARRIER(ref_addr, ref->addr);
        if (ref_addr == NULL)
            return;
    } while (!__wt_atomic_cas_ptr(&ref->addr, ref_addr, NULL));

    /* Encourage races. */
    if (FLD_ISSET(S2C(session)->timing_stress_flags, WT_TIMING_STRESS_SPLIT_8)) {
        __wt_yield();
        __wt_yield();
    }

    if (home == NULL || __wt_off_page(home, ref_addr)) {
        __wti_ref_addr_safe_free(session, ((WT_ADDR *)ref_addr)->addr, ((WT_ADDR *)ref_addr)->size);
        __wti_ref_addr_safe_free(session, ref_addr, sizeof(WT_ADDR));
    }
}