
#include <stdint.h>
#include <pthread.h>

#define BT_ALLOC_GIB(n) ((size_t)(n) * (1 << 30))
#define BT_ALLOC_MIB(n) ((size_t)(n) * (1 << 20))
#define BT_ALLOC_KIB(n) ((size_t)(n) * (1 << 10))

#define BT_ALLOC_INVALID_REGION UINT32_MAX

#define BT_ALLOC_GIANT_END UINTPTR_MAX


#define USE_LOCK(x) x

/*
 * Allocator Context
 *
 */
typedef struct bt_allocator_ {
    /* Allocator configuration. */
    size_t region_size;
    size_t region_max;

    uintptr_t vmem_start;       /* Start address of reserved vmem. */
    uint32_t region_count;      /* Number of active regions. */
    uint32_t region_high;       /* Region high water mark. If region_high < region_count also
                                 * corresponds to first free page. */

    uint64_t stat_intra;
    uint64_t stat_spill;
    uint64_t stat_giant;
    uint64_t stat_page;

    USE_LOCK(pthread_mutex_t lock);


    /* TODO alignas not working on will's compiler. */
    uint8_t region_map[];
} bt_allocator;


/*
 * Page Region Header
 *
 * Embedded at the beginning of the region containing the page allocation, that is the first memory
 * region associated with a WT page.
 */
typedef struct bt_alloc_prh_ {
    size_t used;                /* Total bytes used in region. */
    uintptr_t last_giant;       /* Pointer to last giant allocation. */
    uint32_t spill;             /* Region id location of first spill region. */
    uint32_t reserved1;         /* Reserved for future use. */
} bt_alloc_prh;


/*
 * Spill Region Header
 */
typedef struct bt_alloc_srh_ {
    size_t used;                /* Total bytes used in region. */
    uint32_t next_spill;        /* Region id of next spill region. */
    uint32_t prior_region;      /* Region id of prior spill or page region. */
} bt_alloc_srh;


/* Giant allocation reference. */
typedef struct bt_alloc_giant_ {
    uintptr_t alloc_ptr;        /* Memory allocated by system. */
    uintptr_t prev_giant;       /* Memory offset to previous giant allocation. */
} bt_alloc_giant;


int bt_alloc_ctor(bt_allocator *allocator);
int bt_alloc_create(bt_allocator **allocator, size_t region_size, size_t region_max);
int bt_alloc_dtor(bt_allocator *allocator);
int bt_alloc_destroy(bt_allocator **allocator);
int bt_alloc_page_alloc(bt_allocator *allocator, size_t alloc_size, WT_PAGE **page_pp);
int bt_alloc_page_free(bt_allocator *allocator, WT_PAGE *page);
int bt_alloc_zalloc(bt_allocator *alloc, size_t alloc_size, WT_PAGE *page, void **mem_pp);
