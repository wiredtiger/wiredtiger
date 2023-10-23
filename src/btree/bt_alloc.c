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

/* use a single vm page -- though potentially overkill consider winding down to 1024 */
#define BT_ARENA_BITMAP_SIZE (4096u * 8u)
#define BT_ARENA_BITMAP_SIZE_BYTES ((BT_ARENA_BITMAP_SIZE) >> 3)

#define BT_ARENA_PAGE_SIZE (4096u)

/* This will need to hang off the btree somewhere. */
struct bt_arena {
    /* virtual memory */
    size_t vsize;
    uintptr_t vmem;

    /* concurrency protection? */

    size_t region_max;          /* maximal region size */
    size_t region_largest;      /* size of largest region in bytes */
    uint32_t region_count;      /* used to infer maximal region size */
    uint32_t region_highest;    /* highest bit offset for a region */

    /* bitmap of utilized regions 0 = used, 1 = unused */
    _Alignas(size_t) uint8_t region_map[BT_ARENA_BITMAP_SIZE_BYTES];
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
} __attribute__((packed));


int bt_arena_ctor(struct bt_arena *arena, size_t vmem_size);
int bt_arena_dtor(struct bt_arena *arena);

int bt_arena_page_alloc(struct bt_arena *arena, size_t alloc_size, void **page_pp);
int bt_arena_page_free(struct bt_arena *arena, WT_PAGE *page);

/* zero-ed memory without the extra calloc parameter */
int bt_arena_zalloc(struct bt_arena *arena, size_t alloc_size, WT_PAGE *page, void **mem_pp);
int bt_arena_free(struct bt_arena *arena, size_t alloc_size, WT_PAGE *page, void *mem_p);


static uint32_t next_highest_pow2(uint32_t v)
{
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    v += (v == 0);
    return v;
}


int bt_arena_ctor(struct bt_arena *arena, size_t vmem_size)
{
    size_t actual_size;
    void *vmem;

    if (arena == NULL || vmem_size == 0) {
        return EINVAL;
    }

    /* reserve virtual memory */
    actual_size = (vmem_size + BT_ARENA_PAGE_SIZE) & (BT_ARENA_PAGE_SIZE - 1);
    vmem = mmap(NULL, actual_size, PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
    if (vmem == MAP_FAILED) {
        return errno;
    }

    /* initialize bitmap */
    memset(arena->region_map, 0xff, sizeof(arena->region_map));

    /* initialize arena */
    arena->vsize = actual_size;
    arena->vmem = (uintptr_t)vmem;
    arena->region_max = actual_size;
    arena->region_largest = 0;
    arena->region_highest = 0;
    arena->region_count = 0;
    return 0;
}


int bt_arena_dtor(struct bt_arena *arena)
{
    int retval;

    if (arena == NULL) {
        return EINVAL;
    }
    /* iteratively free threaded out of region allocations */
    /* decommit virtual memory */
    retval = munmap((void*)arena->vmem, arena->vsize);
    return (retval) ? errno : retval;
}


int bt_arena_page_alloc(struct bt_arena *arena, size_t alloc_size, void **page_pp)
{
    uint64_t *map_iter;
    uint32_t r_count;
    size_t new_max;

    if (arena == NULL || alloc_size == 0 || page_pp == NULL) {
        return EINVAL;
    }

    if (arena->region_count >= BT_ARENA_BITMAP_SIZE) {
        /* how?: need warning log message as to why */
        return ENOMEM;
    }

    WT_ASSERT(NULL, arena->region_highest <= arena->region_count);
    /* which branch is unlikely??? */
    if (arena->region_highest == arena->region_count) {
        /*
         * append a new region
         *
         * potentially sub-divides the available space for a region
         */
        r_count = arena->region_count + 1;
        new_max = arena->vsize / next_highest_pow2(r_count);
        if (new_max < arena->region_largest) {
            /* how?: need warning log message as to why */
            return ENOMEM;
        }
        arena->region_max = new_max;
        WT_ASSERT(NULL, 0 && "not implemented");
    } else {
        /*
         * scan to find the first free region
         *
         * guaranteed to exist by earlier region count check
         */
        for (map_iter = (void *)&arena->region_map; *map_iter != 0; ++map_iter) {
            WT_ASSERT(NULL, ((uintptr_t)map_iter - (uintptr_t)&arena->region_map) < BT_ARENA_BITMAP_SIZE_BYTES);
        }
        WT_ASSERT(NULL, 0 && "not implemented");
    }

    return ENOTSUP;
}

int bt_arena_page_free(struct bt_arena *arena, WT_PAGE *page)
{
    if (arena == NULL || page == NULL) {
        return EINVAL;
    }
    WT_ASSERT(NULL, 0 && "not implemented");
    return ENOTSUP;
}
