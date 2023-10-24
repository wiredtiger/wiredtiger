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


/*
 * Allocator Context
 *
 */
typedef struct {
    uintptr_t vmem_start;       /* Start address of reserved vmem. */
    uint32_t region_count;      /* Number of active regions. */
    uint32_t region_high;       /* Region high water mark. If region_high < region_count also
                                 * corresponds to first free page. */

    _Alignas(size_t) uint8_t region_map[BT_ALLOC_REGION_COUNT / 8 ];
} bt_allocator;


/*
 * Page Region Header
 *
 * Embedded at the beginning of the region containing the page allocation, that is the first memory
 * region associated with a WT page.
 */
typedef struct {
    size_t used;                /* Total bytes used in region. */
    uintptr_t last_giant;       /* Pointer to last giant allocation in this region. */
    uint32_t spill;             /* Region id location of first spill region. */
    uint32_t reserved1;         /* Reserved for future use. */
} __attribute__((packed)) bt_alloc_prh;


/*
 * Spill Region Header
 */
typedef struct {
    size_t used;                /* Total bytes used in region. */
    uint32_t next_spill;        /* Region id of next spill region. */
    uint32_t prior_region;      /* Region id of prior spill or page region. */
} bt_alloc_srh;


/* Giant allocation reference. */
typedef struct {
    uintptr_t alloc_ptr;        /* Memory allocated by system. */
    uintptr_t prev_giant;       /* Memory offset to previous giant allocation. */
} bt_alloc_giant;


/* Offset to the byte in a byte array the corresponding bit would be located. */
static inline uint32_t
_bit_to_byte(uint32_t bit)
{
    return bit >> 3;
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
    return 0;
}


int bt_alloc_dtor(bt_allocator *allocator)
{
    int retval;

    if (allocator == NULL) {
        return EINVAL;
    }

    WT_ASSERT(NULL, allocator->region_count == 0);

    /* Decommit virtual memory. */
    retval = munmap((void*)allocator->vmem_start, BT_ALLOC_VMSIZE);
    return (retval) ? errno : retval;
}


static uint32_t
_take_next_free_region(bt_allocator *allocator)
{
    uint32_t region;
    unsigned int i;
    uint8_t blk;

    if (allocator->region_count == BT_ALLOC_REGION_COUNT) {
        goto ret_failed;
    }

    if (allocator->region_high < allocator->region_count) {
        region = allocator->region_high;
    } else {
        /* Need to search for a free region. */
        for (i = 0; i < sizeof(allocator->region_map[0]); i++) {
            if (allocator->region_map[i] != 0) {
                break;
            }
        }
        if (i >= sizeof(allocator->region_map)) {
            /* This is really bad: there should be at least one free region.  */
            goto ret_failed;
        }

        blk = ~allocator->region_map[i];
        region = (i * 8) + (unsigned int)__builtin_clz(blk);
        allocator->region_map[i] |= UINT8_C(1) << (unsigned int)__builtin_clz(blk);
    }

    allocator->region_high++;
    allocator->region_count++;
    return region;

ret_failed:
    return BT_ALLOC_INVALID_REGION;
}


int bt_alloc_page_alloc(bt_allocator *allocator, size_t alloc_size, WT_PAGE **page_pp)
{
    uint32_t region;
    bt_alloc_prh *hdr;

    WT_ASSERT(NULL, alloc_size <= (BT_ALLOC_REGION_SIZE - sizeof(bt_alloc_prh)));

    if (allocator == NULL || alloc_size == 0 || page_pp == NULL) {
        return EINVAL;
    }

    region = _take_next_free_region(allocator);
    if (region == BT_ALLOC_INVALID_REGION) {
        __wt_verbose(NULL, WT_VERB_BT_ALLOC, "Exhausted allocator: used %" PRIu32 " regions.",
          allocator->region_count);
        return ENOMEM;
    }

    hdr = _region_ptr(allocator, region);
    hdr->used = alloc_size;
    hdr->spill = BT_ALLOC_INVALID_REGION;
    hdr->last_giant = BT_ALLOC_GIANT_END;
    *page_pp = _region_offset_ptr(allocator, region, sizeof(hdr));

    return 0;
}


static void
_free_giants(bt_alloc_prh *pghdr)
{
    uintptr_t next;
    bt_alloc_giant *giant;

    next = pghdr->last_giant;
    while (next != BT_ALLOC_GIANT_END) {
        giant = (void *)next;
        next = giant->prev_giant;
        free((void *)giant->alloc_ptr);
    }
}


static void
_free_spill_pages(bt_allocator *allocator, bt_alloc_prh *pghdr)
{
    uint32_t next;
    bt_alloc_srh *spillhdr;
    int ret;

    next = pghdr->spill;
    while (next != BT_ALLOC_INVALID_REGION) {
        /* TODO add assertion to ensure still within allocator range. */

        spillhdr = _region_ptr(allocator, next);

        ret = posix_madvise((void *)pghdr, BT_ALLOC_REGION_SIZE, POSIX_MADV_DONTNEED);
        if (ret != 0) {
            __wt_verbose(NULL, WT_VERB_BT_ALLOC,
              "bt_alloc posix_madvise error: %s", strerror(errno));
        }

        next = spillhdr->next_spill;
    }
}


int bt_alloc_page_free(bt_allocator *allocator, WT_PAGE *page)
{
    uintptr_t paddr;
    bt_alloc_prh *pghdr;
    int ret;

    if (allocator == NULL || page == NULL) {
        return EINVAL;
    }

    paddr = (uintptr_t)page;
    if (paddr < allocator->vmem_start || paddr >= (allocator->vmem_start + BT_ALLOC_VMSIZE)) {
        __wt_verbose(NULL, WT_VERB_BT_ALLOC,
          "Request to free page outside of reserved vmspace page=%zu", paddr);
        return EINVAL;
    }

    pghdr = (void *)page;
    _free_giants(pghdr);
    _free_spill_pages(allocator, pghdr);

    ret = posix_madvise((void *)pghdr, BT_ALLOC_REGION_SIZE, POSIX_MADV_DONTNEED);
    if (ret != 0) {
        __wt_verbose(NULL, WT_VERB_BT_ALLOC,
          "bt_alloc posix_madvise  page=%zu  error=%s", paddr, strerror(errno));
    }
    return 0;
}


#define BT_ALLOC_REGION_MAX (BT_ALLOC_REGION_SIZE - sizeof(*pghdr))


static void *
_pr_free_mem_start(bt_alloc_prh *pagehdr)
{
    uintptr_t ptr;
    ptr = (uintptr_t)pagehdr + sizeof(*pagehdr) + pagehdr->used;
    WT_ASSERT(NULL, (ptr & 7u) == 0);
    return (void *)ptr;
}

/* Memory available for allocation in region beginning with the page head */
static size_t
_pr_free_mem_size(bt_alloc_prh *pghdr)
{
    return BT_ALLOC_REGION_SIZE - sizeof(*pghdr) - pghdr->used;
}


static size_t
_sr_free_mem_size(bt_alloc_srh *spillhdr)
{
    return BT_ALLOC_REGION_SIZE - sizeof(*spillhdr) - spillhdr->used;
}


static void *
_intra_region_alloc(bt_allocator *allocator, bt_alloc_prh *pghdr, size_t alloc_size)
{
    void *ptr;

    WT_ASSERT(NULL, pghdr != NULL && alloc_size > 0);

    alloc_size = (alloc_size + 7u) & ~7u;

    if (_pr_free_mem_size(pghdr) >= alloc_size) {
        ptr = _pr_free_mem_start(pghdr);
        pghdr->used += alloc_size;
    } else {
        /* TODO */
        (void)allocator;
        ptr = NULL;
    }

    return ptr;
}


static void *
_giant_alloc(bt_allocator *allocator, bt_alloc_prh *pghdr, size_t alloc_size)
{
    void *sysmem;
    bt_alloc_giant *giant;

    WT_ASSERT(NULL, pghdr != NULL && alloc_size > 0);

    sysmem = calloc(1, alloc_size);
    if (sysmem == NULL) {
        return NULL;
    }

    giant = _intra_region_alloc(allocator, pghdr, sizeof(*giant));
    if (giant == NULL) {
        free(sysmem);
        errno = ENOMEM;
        return NULL;
    }

    giant->alloc_ptr = (uintptr_t)sysmem;
    giant->prev_giant = pghdr->last_giant;
    pghdr->last_giant = (uintptr_t)giant;
    return sysmem;
}


int bt_alloc_zalloc(bt_allocator *allocator, size_t alloc_size, WT_PAGE *page, void **mem_pp)
{
    bt_alloc_prh *pghdr;
    void *ptr;

    if (allocator == NULL || page == NULL || mem_pp == NULL) {
        return EINVAL;
    }

    if (alloc_size == 0) {
        *mem_pp = NULL;
        return 0;
    }

    pghdr = (void *)((uintptr_t)page - sizeof(bt_alloc_prh));

    if (alloc_size <= BT_ALLOC_REGION_MAX) {
        ptr = _intra_region_alloc(allocator, pghdr, alloc_size);
    } else {
        ptr = _giant_alloc(allocator, pghdr, alloc_size);
    }

    if (ptr != NULL) {
        *mem_pp = ptr;
        return 0;
    }

    return errno;
}
