/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_page_custom_alloc_row_leaf --
 *     Entry point for cool custom allocator.
 */
int
__wt_page_custom_alloc_row_leaf(WT_SESSION_IMPL *session, uint32_t entries, WT_PAGE **pagep)
{
    WT_PAGE *page;
    size_t size;

    size = sizeof(WT_PAGE) + (entries * sizeof(WT_ROW));

    WT_RET(__wt_calloc(session, 1, size, &page));

    page->pg_row = entries == 0 ? NULL : (WT_ROW *)((uint8_t *)page + sizeof(WT_PAGE));

    *pagep = page;

    return (0);
}

/*
 * __row_leaf_key_free --
 *     Words to make s all happy.
 */
static void
__row_leaf_key_free(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip)
{
    WT_IKEY *ikey;
    void *copy;

    /* The row-store key can change underfoot; explicitly take a copy. */
    copy = WT_ROW_KEY_COPY(rip);

    /*
     * If the key was a WT_IKEY allocation (that is, if it points somewhere other than the original
     * page), free the memory.
     */
    __wt_row_leaf_key_info(page, copy, &ikey, NULL, NULL, NULL, NULL);
    __wt_free(session, ikey);
}

/*
 * __wt_page_custom_free_row_leaf --
 *     Words to make s all happy.
 */
void
__wt_page_custom_free_row_leaf(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_PAGE_HEADER *dsk;
    WT_ROW *rip;
    uint32_t i;

    WT_ASSERT(session, page->type == WT_PAGE_ROW_LEAF);

    /* Free any allocated memory used by instantiated keys. */
    WT_ROW_FOREACH (page, rip, i)
        __row_leaf_key_free(session, page, rip);

    /* Discard any allocated disk image. */
    dsk = (WT_PAGE_HEADER *)page->dsk;
    if (F_ISSET_ATOMIC_16(page, WT_PAGE_DISK_ALLOC))
        /* TODO - not clear if we'll override disk image allocation. */
        __wt_overwrite_and_free_len(session, dsk, dsk->mem_size);

    __wt_overwrite_and_free(session, page);
}

/*
 * __wt_upd_custom_alloc_row_leaf --
 *     Words to make s all happy.
 */
int
__wt_upd_custom_alloc_row_leaf(
  WT_SESSION_IMPL *session, WT_PAGE *page, size_t allocsz, WT_UPDATE **updp)
{
    WT_UNUSED(page);
    return (__wt_calloc(session, 1, allocsz, updp));
}

/*
 * __upd_custom_free --
 *     Words to make s all happy.
 */
static void
__upd_custom_free(WT_SESSION_IMPL *session, WT_PAGE *page, WT_UPDATE *upd)
{
    /*
     * TODO - can probably be a no-nop, since updates are freed when the page is freed.
     */
    WT_UNUSED(page);
    __wt_free(session, upd);
}

/*
 * __wt_upd_free --
 *     Words to make s all happy.
 */
void
__wt_upd_free(WT_SESSION_IMPL *session, WT_PAGE *page, WT_UPDATE **updp)
{
    WT_UPDATE *upd;

    upd = *updp;
    *updp = NULL;

    if (page && page->type == WT_PAGE_ROW_LEAF)
        __upd_custom_free(session, page, upd);
    else
        __wt_free(session, upd);
}

/* ---------------------------------------------------------------------------- */

/* potentially overkill consider winding down to 1024 */
#define BT_ARENA_BITMAP_SIZE (4096u * 8u)
#define BT_ARENA_BITMAP_SIZE_BYTES ((BT_ARENA_BITMAP_SIZE) >> 3)

/* This will need to hang off the btree somewhere. */
struct bt_arena {
    /* virtual memory */
    size_t vsize;
    void  *vmem;

    /* concurrency protection? */

    uint32_t region_count;                          /* used to infer maximal region size */
    size_t largest;                                 /* size of largest region in bytes */
    uint8_t region_map[BT_ARENA_BITMAP_SIZE_BYTES]; /* bitmap of utilized regions */
};

/*
 * Page Region header
 * u32 : amount allocated -- enough?
 * u32 : pad
 * u32 : offset of first regular alloc ptr
 * u32 : pad
 */
struct bt_region_header {
    uint32_t region_used;
    uint32_t pad1_;
    uint32_t offset;
    uint32_t pad2_;
};

int bt_arena_ctor(struct bt_arena *arena, size_t vmem_size);
int bt_arena_dtor(struct bt_arena *arena);

int bt_arena_page_alloc(struct bt_arena *arena, size_t alloc_size, void **page_pp);
int bt_arena_page_free(struct bt_arena *arena, WT_PAGE *page);

/* zero-ed memory without the extra calloc parameter */
int bt_arena_zalloc(struct bt_arena *arena, size_t alloc_size, WT_PAGE *page, void **mem_pp);
int bt_arena_free(struct bt_arena *arena, size_t alloc_size, WT_PAGE *page, void *mem_p);

int bt_arena_ctor(struct bt_arena *arena, size_t vmem_size)
{
    if (arena == NULL || vmem_size == 0) {
        return EINVAL;
    }
    /* reserve virtual memory */
    /* initialize bitmap */
    /* initialize arena */
    return ENOTSUP;
}

int bt_arena_dtor(struct bt_arena *arena);
