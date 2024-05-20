/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define DEBUG_HEAP

#ifdef DEBUG_HEAP
#undef HAVE_POSIX_MEMALIGN
#define DEBUG_HEAP_GUARD_PRE1 0xa0a1a2a3a4a5a6a7
#define DEBUG_HEAP_GUARD_PRE2 0xa8a9aaabacadaeaf
#define DEBUG_HEAP_GUARD_POST1 0xb0b1b2b3b4b5b6b7
#define DEBUG_HEAP_GUARD_POST2 0xb8b9babbbcbdbebf
#define DEBUG_HEAP_UNINIT 0xaa
#define DEBUG_HEAP_FREE 0xcc

typedef struct {
    uint64_t guard1,guard11;
    uint64_t size;
    uint64_t guard2;
} debug_heap_pre;

typedef struct {
    uint64_t guard1;
    uint64_t size;
    uint64_t guard2,guard22;
} debug_heap_post;

static void __debug_heap_init_pre(void *p, size_t requested_size) {
    debug_heap_pre *pre = (debug_heap_pre*)((char*)p - sizeof(debug_heap_pre));
    pre->guard1 = pre->guard11 = DEBUG_HEAP_GUARD_PRE1;
    pre->size = requested_size;
    pre->guard2 = DEBUG_HEAP_GUARD_PRE2;
}

static void __debug_heap_init_post(void *p, size_t requested_size) {
    debug_heap_post *post = (debug_heap_post*)((char*)p + requested_size);
    post->guard1 = DEBUG_HEAP_GUARD_POST1;
    post->size = requested_size;
    post->guard2 = post->guard22 = DEBUG_HEAP_GUARD_POST2;
}

static void debug_heap_init_guards(void *p, size_t requested_size) {
    __debug_heap_init_pre(p, requested_size);
    __debug_heap_init_post(p, requested_size);
}

static void debug_heap_init_block(void *p, size_t requested_size) {
    debug_heap_init_guards(p, requested_size);
    memset(p, DEBUG_HEAP_UNINIT, requested_size);
    if (requested_size == sizeof(WT_REF)) {
        printf("  alloc ref=%p\n", (void *)p);
    }
}

static void debug_heap_before_free(void *p) {
    debug_heap_pre *pre = (debug_heap_pre*)((char*)p - sizeof(debug_heap_pre));
    debug_heap_post *post = (debug_heap_post*)((char*)p + pre->size);
    pre->guard11 = DEBUG_HEAP_GUARD_PRE1 ^ 0xffffffffffffffff;
    post->guard22 = DEBUG_HEAP_GUARD_POST2 ^ 0xffffffffffffffff;
    if (pre->size == sizeof(WT_REF)) {
        printf("  free ref=%p\n", (void *)p);
    } else if (pre->size > sizeof(WT_REF)) {
        WT_REF *ref = (WT_REF *)p;
        if (ref->lru_all__guard1 == __GUARD1) {
            printf("  MULTI free ref! (%d)\n", (int)(pre->size / sizeof(WT_REF)));
            for (int i = 0; i < (int)(pre->size / sizeof(WT_REF)); i++) {
                ref = (WT_REF *)p + i;
                if (ref->lru_all__guard1 != __GUARD1) {
                    printf("    ... free ref=%p\n", (void *)ref);
                }
            }
        }
    }
    memset(p, DEBUG_HEAP_FREE, pre->size);
}

static void debug_heap_check_memory_block(void *p) {
    if (p == NULL) return;
    do {
        debug_heap_pre *pre = (debug_heap_pre*)((char*)p - sizeof(debug_heap_pre));
        debug_heap_post *post = (debug_heap_post*)((char*)p + pre->size);
        WT_ASSERT_ALWAYS(NULL, pre->guard1 == DEBUG_HEAP_GUARD_PRE1, "pre->guard1=0x%" PRIu64 "x", pre->guard1);
        WT_ASSERT_ALWAYS(NULL, pre->guard11 == DEBUG_HEAP_GUARD_PRE1, "pre->guard11=0x%" PRIu64 "x", pre->guard11);
        WT_ASSERT_ALWAYS(NULL, pre->guard2 == DEBUG_HEAP_GUARD_PRE2, "pre->guard2=0x%" PRIu64 "x", pre->guard2);
        WT_ASSERT_ALWAYS(NULL, post->guard1 == DEBUG_HEAP_GUARD_POST1, "post->guard1=0x%" PRIu64 "x", post->guard1);
        WT_ASSERT_ALWAYS(NULL, post->guard2 == DEBUG_HEAP_GUARD_POST2, "post->guard2=0x%" PRIu64 "x", post->guard2);
        WT_ASSERT_ALWAYS(NULL, post->guard22 == DEBUG_HEAP_GUARD_POST2, "post->guard22=0x%" PRIu64 "x", post->guard22);
        WT_ASSERT_ALWAYS(NULL, pre->size == post->size, "pre->size=%" PRIu64 " post->size=%" PRIu64, pre->size, post->size);
    } while(0);
}

#define DEBUG_HEAP_MALLOC_SIZE(s) (s + sizeof(debug_heap_pre) + sizeof(debug_heap_post))
#define DEBUG_HEAP_PTR_FROM_MALLOC(p) ((void*)((char*)(p) + sizeof(debug_heap_pre)))
#define DEBUG_HEAP_PTR_TO_MALLOC(p) ((void*)((char*)(p) - sizeof(debug_heap_pre)))
#define DEBUG_HEAP_PTR_TO_MALLOC_OR_0(p) ((p) ? ((void*)((char*)(p) - sizeof(debug_heap_pre))) : NULL)

#else
#define debug_heap_init_guards(p, requested_size)
#define debug_heap_init_block(p, requested_size)
#define debug_heap_mark_block_before_free(p)
#define debug_heap_check_memory_block(p)
#defing DEBUG_HEAP_MALLOC_SIZE(s) s
#define DEBUG_HEAP_PTR_FROM_MALLOC(p) p
#define DEBUG_HEAP_PTR_TO_MALLOC_OR_0(p) p
#define DEBUG_HEAP_PTR_TO_MALLOC(p) p
#endif

/*
 * __wt_calloc --
 *     ANSI calloc function.
 */
int
__wt_calloc(WT_SESSION_IMPL *session, size_t number, size_t size, void *retp)
  WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
#ifdef DEBUG_HEAP
    int ret = __wt_malloc(session, number * size, retp);
    if (ret != 0) return ret;
    memset(*(void **)retp, 0, number * size);
    return 0;
#else
    void *p;

    /*
     * Defensive: if our caller doesn't handle errors correctly, ensure a free won't fail.
     */
    *(void **)retp = NULL;

    /*
     * !!!
     * This function MUST handle a NULL WT_SESSION_IMPL handle.
     */
    WT_ASSERT(session, number != 0 && size != 0);

    if (session != NULL)
        WT_STAT_CONN_INCR(session, memory_allocation);

    if ((p = calloc(number, size)) == NULL)
        WT_RET_MSG(session, __wt_errno(), "memory allocation of %" WT_SIZET_FMT " bytes failed",
          size * number);

    *(void **)retp = p;
    return (0);
#endif
}

/*
 * __wt_malloc --
 *     ANSI malloc function.
 */
int
__wt_malloc(WT_SESSION_IMPL *session, size_t bytes_to_allocate, void *retp)
{
    void *p;

    /*
     * Defensive: if our caller doesn't handle errors correctly, ensure a free won't fail.
     */
    *(void **)retp = NULL;

    /*
     * !!!
     * This function MUST handle a NULL WT_SESSION_IMPL handle.
     */
    WT_ASSERT(session, bytes_to_allocate != 0);

    if (session != NULL)
        WT_STAT_CONN_INCR(session, memory_allocation);

    if ((p = malloc(DEBUG_HEAP_MALLOC_SIZE(bytes_to_allocate))) == NULL)
        WT_RET_MSG(session, __wt_errno(), "memory allocation of %" WT_SIZET_FMT " bytes failed",
          bytes_to_allocate);

    *(void **)retp = DEBUG_HEAP_PTR_FROM_MALLOC(p);
    debug_heap_init_block(*(void **)retp, bytes_to_allocate);

    return (0);
}

/*
 * __realloc_func --
 *     ANSI realloc function.
 */
static int
__realloc_func(WT_SESSION_IMPL *session, size_t *bytes_allocated_ret, size_t bytes_to_allocate,
  bool clear_memory, void *retp)
{
    size_t bytes_allocated;
    void *p, *tmpp;

    WT_ASSERT_ALWAYS(session, !(bytes_allocated_ret == NULL && clear_memory),
      "bytes allocated must be passed in if clear_memory is set, otherwise use "
      "__wt_realloc_noclear");

    /*
     * !!!
     * This function MUST handle a NULL WT_SESSION_IMPL handle.
     *
     * Sometimes we're allocating memory and we don't care about the
     * final length -- bytes_allocated_ret may be NULL.
     */
    p = *(void **)retp;
    debug_heap_check_memory_block(p);
    bytes_allocated = (bytes_allocated_ret == NULL) ? 0 : *bytes_allocated_ret;
    WT_ASSERT(session,
      (p == NULL && bytes_allocated == 0) ||
        (p != NULL && (bytes_allocated_ret == NULL || bytes_allocated != 0)));
    WT_ASSERT(session, bytes_to_allocate != 0);
    WT_ASSERT(session, bytes_allocated < bytes_to_allocate);

    if (session != NULL) {
        if (p == NULL)
            WT_STAT_CONN_INCR(session, memory_allocation);
        else
            WT_STAT_CONN_INCR(session, memory_grow);
    }

    /*
     * If realloc_malloc is enabled, force a new memory allocation by using malloc, copy to the new
     * memory, scribble over the old memory then free it.
     */
    tmpp = p;
    if (session != NULL && FLD_ISSET(S2C(session)->debug_flags, WT_CONN_DEBUG_REALLOC_MALLOC) &&
      (bytes_allocated_ret != NULL)) {
        if ((p = malloc(DEBUG_HEAP_MALLOC_SIZE(bytes_to_allocate))) == NULL)
            WT_RET_MSG(session, __wt_errno(), "memory allocation of %" WT_SIZET_FMT " bytes failed",
              bytes_to_allocate);
        p = DEBUG_HEAP_PTR_FROM_MALLOC(p);
        debug_heap_init_guards(p, bytes_to_allocate);
        if (tmpp != NULL) {
            memcpy(DEBUG_HEAP_PTR_FROM_MALLOC(p), tmpp, *bytes_allocated_ret);
            __wt_explicit_overwrite(tmpp, bytes_allocated);
            __wt_free(session, tmpp);
        }
    } else {
        if ((p = realloc(DEBUG_HEAP_PTR_TO_MALLOC_OR_0(p), DEBUG_HEAP_MALLOC_SIZE(bytes_to_allocate))) == NULL)
            WT_RET_MSG(session, __wt_errno(), "memory allocation of %" WT_SIZET_FMT " bytes failed",
              bytes_to_allocate);
        p = DEBUG_HEAP_PTR_FROM_MALLOC(p);
        debug_heap_init_guards(p, bytes_to_allocate);
#ifdef DEBUG_HEAP
        if (bytes_to_allocate > bytes_allocated)
            memset((uint8_t *)p + bytes_allocated, DEBUG_HEAP_UNINIT, bytes_to_allocate - bytes_allocated);
#endif
    }

    /*
     * Clear the allocated memory, parts of WiredTiger depend on allocated memory being cleared.
     */
    if (clear_memory)
        memset((uint8_t *)p + bytes_allocated, 0, bytes_to_allocate - bytes_allocated);

    /* Update caller's bytes allocated value. */
    if (bytes_allocated_ret != NULL)
        *bytes_allocated_ret = bytes_to_allocate;

    *(void **)retp = p;
    return (0);
}

/*
 * __wt_realloc --
 *     WiredTiger's realloc API.
 */
int
__wt_realloc(
  WT_SESSION_IMPL *session, size_t *bytes_allocated_ret, size_t bytes_to_allocate, void *retp)
{
    return (__realloc_func(session, bytes_allocated_ret, bytes_to_allocate, true, retp));
}

/*
 * __wt_realloc_noclear --
 *     WiredTiger's realloc API, not clearing allocated memory.
 */
int
__wt_realloc_noclear(
  WT_SESSION_IMPL *session, size_t *bytes_allocated_ret, size_t bytes_to_allocate, void *retp)
{
    return (__realloc_func(session, bytes_allocated_ret, bytes_to_allocate, false, retp));
}

/*
 * __wt_realloc_aligned --
 *     ANSI realloc function that aligns to buffer boundaries, configured with the
 *     "buffer_alignment" key to wiredtiger_open.
 */
int
__wt_realloc_aligned(
  WT_SESSION_IMPL *session, size_t *bytes_allocated_ret, size_t bytes_to_allocate, void *retp)
{
#if defined(HAVE_POSIX_MEMALIGN)
    WT_DECL_RET;

    /*
     * !!!
     * This function MUST handle a NULL WT_SESSION_IMPL handle.
     */
    if (session != NULL && S2C(session)->buffer_alignment > 0) {
        void *p, *newp;
        size_t bytes_allocated;

        /*
         * Sometimes we're allocating memory and we don't care about the final length --
         * bytes_allocated_ret may be NULL.
         */
        newp = NULL;
        p = *(void **)retp;
        bytes_allocated = (bytes_allocated_ret == NULL) ? 0 : *bytes_allocated_ret;
        WT_ASSERT(session,
          (p == NULL && bytes_allocated == 0) ||
            (p != NULL && (bytes_allocated_ret == NULL || bytes_allocated != 0)));
        WT_ASSERT(session, bytes_to_allocate != 0);
        WT_ASSERT(session, bytes_allocated <= bytes_to_allocate);

        /*
         * We are going to allocate an aligned buffer. When we do this repeatedly, the allocator is
         * expected to start on a boundary each time, account for that additional space by never
         * asking for less than a full alignment size. The primary use case for aligned buffers is
         * Linux direct I/O, which requires that the size be a multiple of the alignment anyway.
         */
        bytes_to_allocate = WT_ALIGN(bytes_to_allocate, S2C(session)->buffer_alignment);

        WT_STAT_CONN_INCR(session, memory_allocation);

        if ((ret = posix_memalign(&newp, S2C(session)->buffer_alignment, bytes_to_allocate)) != 0)
            WT_RET_MSG(session, ret, "memory allocation of %" WT_SIZET_FMT " bytes failed",
              bytes_to_allocate);

        if (p != NULL)
            memcpy(newp, p, bytes_allocated);
        __wt_free(session, p);
        p = newp;

        /* Update caller's bytes allocated value. */
        if (bytes_allocated_ret != NULL)
            *bytes_allocated_ret = bytes_to_allocate;

        *(void **)retp = p;
        return (0);
    }
#endif
    /*
     * If there is no posix_memalign function, or no alignment configured, fall back to realloc.
     *
     * Windows note: Visual C CRT memalign does not match POSIX behavior and would also double each
     * allocation so it is bad for memory use.
     */
    return (__realloc_func(session, bytes_allocated_ret, bytes_to_allocate, false, retp));
}

/*
 * __wt_memdup --
 *     Duplicate a byte string of a given length.
 */
int
__wt_memdup(WT_SESSION_IMPL *session, const void *str, size_t len, void *retp)
{
    void *p;

    WT_RET(__wt_malloc(session, len, &p));

    WT_ASSERT(session, p != NULL); /* quiet clang scan-build */

    memcpy(p, str, len);

    *(void **)retp = p;
    return (0);
}

/*
 * __wt_strndup --
 *     ANSI strndup function.
 */
int
__wt_strndup(WT_SESSION_IMPL *session, const void *str, size_t len, void *retp)
{
    uint8_t *p;

    if (str == NULL) {
        *(void **)retp = NULL;
        return (0);
    }

    /* Copy and nul-terminate. */
    WT_RET(__wt_malloc(session, len + 1, &p));

    WT_ASSERT(session, p != NULL); /* quiet clang scan-build */

    memcpy(p, str, len);
    p[len] = '\0';

    *(void **)retp = p;
    return (0);
}

/*
 * __wt_free_int --
 *     ANSI free function.
 */
void
__wt_free_int(WT_SESSION_IMPL *session, const void *p_arg)
  WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    void *p;

    p = *(void **)p_arg;
    if (p == NULL) /* ANSI C free semantics */
        return;

    /*
     * If there's a serialization bug we might race with another thread. We can't avoid the race
     * (and we aren't willing to flush memory), but we minimize the window by clearing the free
     * address, hoping a racing thread will see, and won't free, a NULL pointer.
     */
    *(void **)p_arg = NULL;

    /*
     * !!!
     * This function MUST handle a NULL WT_SESSION_IMPL handle.
     */
    if (session != NULL)
        WT_STAT_CONN_INCR(session, memory_free);

    debug_heap_check_memory_block(p);
    debug_heap_before_free(p);

    free(DEBUG_HEAP_PTR_TO_MALLOC(p));
}
