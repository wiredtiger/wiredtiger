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
    WT_BTREE *tree;
    WT_PAGE *page;
    size_t size;

    tree = S2BT(session);
    size = sizeof(WT_PAGE) + (entries * sizeof(WT_ROW));

    WT_RET(__wt_calloc(session, 1, size, &page));

    __wt_verbose_info(session, WT_VERB_BT_ALLOC,
      "[ALLOC_LEAF] tree_id=%" PRIu32 " page_addr=%p entries=%" PRIu32 " sz=%" WT_SIZET_FMT "B", tree->id, page, entries,
      size);

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
    uint32_t i, rows_freed;

    WT_ASSERT(session, page->type == WT_PAGE_ROW_LEAF);

    /* Free any allocated memory used by instantiated keys. */
    WT_ROW_FOREACH (page, rip, i) {
        ++rows_freed;
        __row_leaf_key_free(session, page, rip);
    }
    __wt_verbose_info(session, WT_VERB_BT_ALLOC, "[FREE_LEAF] page_addr=%p freed=%" PRIu32, page, rows_freed);

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
    WT_DECL_RET;

    ret = __wt_calloc(session, 1, allocsz, updp);

    __wt_verbose_info(session, WT_VERB_BT_ALLOC, "[ALLOC_UPD] page_addr=%p upd_addr=%p size=%" WT_SIZET_FMT, page, *updp, allocsz);

    return (ret);
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
    __wt_verbose_info(session, WT_VERB_BT_ALLOC, "[FREE_UPD] page_addr=%p upd_addr=%p", page, upd);
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
#include <stdint.h>


#define BT_ALLOC_MIB(n) ((ptrdiff_t)(n) * (1 << 20))


/*
 * Region size should a multiple of vm page size and be large enough to accommodate the largest
 * initial page.
 */
#define BT_ALLOC_REGION_SIZE BT_ALLOC_MIB(128)

/*
 * Number of memory regions available to the allocator. Also absoluate maximum number of pages for
 * the tree.
 */
#define BT_ALLOC_REGION_COUNT 4096

#define BT_ALLOC_VMSIZE (BT_ALLOC_REGION_COUNT * BT_ALLOC_REGION_SIZE)

#define BT_ALLOC_INVALID_REGION UINT32_MAX

#define BT_ALLOC_GIANT_END UINTPTR_MAX


/* Allocator context. */
typedef struct {
    uintptr_t vmem_start;       /* Start address of reserved vmem. */
    uint32_t region_count;      /* Number of active regions. */
    uint32_t region_high;       /* Active region high water mark. */
    uint32_t region_first_free; /* First free region in bitmap. */

    _Alignas(size_t) uint8_t region_map[BT_ALLOC_REGION_COUNT / 8 ];
} bt_allocator;


/*
 * Header for first region used by a page.
 */
typedef struct {
    size_t used;                /* Total bytes used in region. */
    uintptr_t last_giant;       /* Pointer to last giant allocation in this region. */
    uint32_t spill;             /* Region id location of first spill region. */
    uint32_t reserved1;         /* Reserved for future use. */
} __attribute__((packed)) bt_alloc_page_region;


/*
 * Header for spill region.
 */
typedef struct {
    size_t used;                /* Total bytes used in region. */
    uint32_t prior_region;      /* Region id of prior spill or page region. */
    uint32_t next_spill;        /* Region id of next spill region. */
} __attribute__((packed)) bt_alloc_spill_region;


/* Giant allocation reference. */
typedef struct {
    uintptr_t alloc_ptr;
    uintptr_t prev_giant;
} __attribute__((packed)) bt_alloc_giant;


/* TODO static assertion sizeof(WT_PAGE) < BT_ALLOC_PR_SIZE */


/* Offset in byte array bit is located. */
static inline uint32_t
_bit_to_byte(uint32_t bit)
{
    return bit >> 3;
}


/* Find the next free region. */
static uint32_t
_next_free_region(bt_allocator *allocator, uint32_t start_bit)
{
    uint32_t i;
    uint8_t b;

    WT_ASSERT(NULL, allocator->region_count < allocator->region_high);

    /* Ideally the map would be a multiple of 8/4 bytes so we could use a larger stride. */
    for (i = _bit_to_byte(start_bit); i < sizeof(allocator->region_map); i++) {
        if (allocator->region_map[i]) {
            b = allocator->region_map[i];
            b = b & (b - 1);
            return i + 4 * (b & 0x88) + 2 * (b & 0x44) + 1 * (b & 0x22);
        }
    }
    WT_ASSERT(NULL, 0 && "Unable to find free region in bitmap.");

    /* This will point outside the virtual memory region and hhopefully result in a segfault. */
    return sizeof(allocator->region_map) * sizeof(uint8_t);
}


static void *
_region_ptr(bt_allocator *allocator, uint32_t region)
{
    uintptr_t addr = allocator->vmem_start + (region * BT_ALLOC_REGION_SIZE);
    return (void *)addr;
}


static void *
_region_offset_ptr(bt_allocator *allocator, uint32_t region, uint32_t offset)
{
    uintptr_t addr;

    WT_ASSERT(NULL, offset < BT_ALLOC_REGION_SIZE);

    addr = allocator->vmem_start + (region * BT_ALLOC_REGION_SIZE) + offset;

    /* When working inside the allocator, should be dealing only in alignment sizes. */
    WT_ASSERT(NULL, (offset & 3) == 0);

    return (void *)addr;
}


int bt_alloc_ctor(bt_allocator *allocator);
int bt_alloc_dtor(bt_allocator *allocator);
int bt_alloc_page_alloc(bt_allocator *allocator, size_t alloc_size, WT_PAGE **page_pp);
int bt_alloc_page_free(bt_allocator *allocator, WT_PAGE *page);
int bt_alloc_zalloc(bt_allocator *alloc, size_t alloc_size, WT_PAGE *page, void **mem_pp);


int bt_alloc_ctor(bt_allocator *allocator)
{
    void *vmem;

    if (allocator == NULL) {
        return EINVAL;
    }

    /* Reserve virtual memory. */
    vmem = mmap(NULL, BT_ALLOC_VMSIZE, PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
    if (vmem == MAP_FAILED) {
        return errno;
    }

    /* Initialize empty bitmap. */
    memset(allocator->region_map, 0xff, sizeof(allocator->region_map));

    allocator->vmem_start = (uintptr_t)vmem;
    allocator->region_count = 0;
    allocator->region_high = 0;
    allocator->region_first_free = 0;
    return 0;
}


int bt_alloc_dtor(bt_allocator *allocator)
{
    int retval;

    if (allocator == NULL) {
        return EINVAL;
    }

    /* TODO Iterate over all regions freeing giant allocation sequences. */

    /* Decommit virtual memory. */
    retval = munmap((void*)allocator->vmem_start, BT_ALLOC_VMSIZE);
    return (retval) ? errno : retval;
}


int bt_alloc_page_alloc(bt_allocator *allocator, size_t alloc_size, WT_PAGE **page_pp)
{
    uint32_t region;
    bt_alloc_page_region *hdr;

    WT_ASSERT(NULL, alloc_size <= (BT_ALLOC_REGION_SIZE - sizeof(bt_alloc_page_region)));

    if (allocator == NULL || alloc_size == 0 || page_pp == NULL) {
        return EINVAL;
    }

    if (allocator->region_count >= BT_ALLOC_REGION_COUNT) {
        /* TODO log error */
        return ENOMEM;
    }

    region = allocator->region_first_free;
    allocator->region_count++;
    if (allocator->region_count < allocator->region_high) {
        allocator->region_first_free = _next_free_region(allocator, region + 1);
    } else {
        allocator->region_first_free = allocator->region_count + 1;
    }

    hdr = _region_ptr(allocator, region);
    hdr->used = alloc_size;
    hdr->spill = BT_ALLOC_INVALID_REGION;
    hdr->last_giant = BT_ALLOC_GIANT_END;
    *page_pp = _region_offset_ptr(allocator, region, sizeof(hdr));

    return 0;
}


int bt_alloc_page_free(bt_allocator *allocator, WT_PAGE *page)
{
    uintptr_t paddr;
    bt_alloc_page_region *hdr;

    if (allocator == NULL || page == NULL) {
        return EINVAL;
    }

    paddr = (uintptr_t)page;
    if (paddr < allocator->vmem_start || paddr >= (allocator->vmem_start + BT_ALLOC_VMSIZE)) {
        /* TODO log error */
        return EINVAL;
    }

    hdr = (void *)page;
    if (hdr->last_giant != BT_ALLOC_GIANT_END) {
        /* TODO Free giant allocation sequence. */
        WT_ASSERT(NULL, 0 && "Not implemented.");
    }

    /* TODO Iterate spill regions associated with this page. Decommit each region, and mark free in
     * the region map. */

    return ENOTSUP;
}


static size_t
_free_mem_in_region(bt_alloc_page_region *pghdr)
{
    return BT_ALLOC_REGION_SIZE - sizeof(*pghdr) - pghdr->used;
}


int bt_alloc_zalloc(bt_allocator *alloc, size_t alloc_size, WT_PAGE *page, void **mem_pp)
{
    bt_alloc_page_region *pghdr;

    if (alloc == NULL || page == NULL || mem_pp == NULL) {
        return EINVAL;
    }

    if (alloc_size == 0) {
        *mem_pp = NULL;
        return 0;
    }

    pghdr = (void *)((uintptr_t)page - sizeof(bt_alloc_page_region));
    if (alloc_size < _free_mem_in_region(pghdr)) {
        /* Allocate out of this region. */
        WT_ASSERT(NULL, 0 && "Not implemented.");
    }

    return ENOTSUP;
}
