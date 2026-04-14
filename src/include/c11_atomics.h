/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include <stdatomic.h>

#if defined(__GNUC__) /* UNRELATED THINGS (JUST FOR COMPILATION) */

#define WT_PTRDIFFT_FMT "td" /* ptrdiff_t format string */
#define WT_SIZET_FMT "zu"    /* size_t format string */

/* GCC-specific attributes. */
#define WT_PACKED_STRUCT_BEGIN(name)             \
    /* NOLINTNEXTLINE(misc-macro-parentheses) */ \
    struct __attribute__((__packed__)) name {
#define WT_PACKED_STRUCT_END \
    }                        \
    ;

/*
 * Attribute are only permitted on function declarations, not definitions. This macro is a marker
 * for function definitions that is rewritten by dist/s_prototypes to create extern.h.
 */
#define WT_GCC_FUNC_ATTRIBUTE(x)
#define WT_GCC_FUNC_DECL_ATTRIBUTE(x) __attribute__(x)

#else /* MSVC SPECIFIC STUFF */
#ifndef _M_AMD64
#error "Only x64 is supported with MSVC"
#endif

#ifndef __cplusplus
#define inline __inline
#endif

/* MSVC Doesn't provide __PRETTY_FUNCTION__, it has __FUNCSIG__ */
#ifdef _MSC_VER
#define __PRETTY_FUNCTION__ __FUNCSIG__
#endif

#define WT_PTRDIFFT_FMT "Id" /* ptrdiff_t format string */
#define WT_SIZET_FMT "Iu"    /* size_t format string */

/* MSVC-specific attributes. */
#define WT_PACKED_STRUCT_BEGIN(name) __pragma(pack(push, 1)) struct name {

#define WT_PACKED_STRUCT_END \
    }                        \
    ;                        \
    __pragma(pack(pop))

#define WT_GCC_FUNC_ATTRIBUTE(x)
#define WT_GCC_FUNC_DECL_ATTRIBUTE(x)
#endif /* GCC/MSVC SPECIFIC STUFF END */

/*
 * C11 atomics implementation of WiredTiger atomic operations. This provides an alternative to the
 * GCC builtin atomics used in gcc.h for compilers that support C11 atomics.
 */

/* Memory barriers using C11 atomics */
#define WT_FULL_BARRIER() atomic_thread_fence(memory_order_seq_cst)
#define WT_ACQUIRE_BARRIER() atomic_thread_fence(memory_order_acquire)
#define WT_RELEASE_BARRIER() atomic_thread_fence(memory_order_release)

#define WT_COMPILER_BARRIER() __asm__ volatile("" ::: "memory")

/* Platform-specific pause instruction - fall back to compiler barrier if not available */
#if defined(x86_64) || defined(__x86_64__)
#define WT_PAUSE() __asm__ volatile("pause\n" ::: "memory")
#elif defined(__aarch64__)
#define WT_PAUSE() __asm__ volatile("isb" ::: "memory")
#elif defined(__PPC64__) || defined(PPC64)
#define WT_PAUSE() __asm__ volatile("ori 0,0,0" ::: "memory")
#elif defined(__mips64el__) || defined(__mips__) || defined(__mips64__) || defined(__mips64)
#define WT_PAUSE() __asm__ volatile("pause\n" ::: "memory")
#elif defined(__s390x__)
#define WT_PAUSE() __asm__ volatile("lr 0,0" ::: "memory")
#elif defined(__sparc__)
#define WT_PAUSE() __asm__ volatile("rd %%ccr, %%g0" ::: "memory")
#elif defined(__riscv) && (__riscv_xlen == 64)
#define WT_PAUSE() __asm__ volatile("nop" ::: "memory")
#elif defined(__loongarch64)
#define WT_PAUSE() __asm__ volatile("nop\n" ::: "memory")
#else
#define WT_PAUSE() WT_COMPILER_BARRIER()
#endif

/* C11 atomic store/load functions */
#define WT_ATOMIC_FUNC_STORE_LOAD(suffix, _type, _atomic_type)                             \
    static inline _type __wt_atomic_load_##suffix##_relaxed(_type *vp)                     \
    {                                                                                      \
        return (atomic_load_explicit((_atomic_type *)vp, memory_order_relaxed));           \
    }                                                                                      \
    static inline void __wt_atomic_store_##suffix##_relaxed(_type *vp, _type v)            \
    {                                                                                      \
        atomic_store_explicit((_atomic_type *)vp, v, memory_order_relaxed);                \
    }                                                                                      \
    static inline _type __wt_atomic_load_##suffix##_acquire(_type *vp)                     \
    {                                                                                      \
        return (atomic_load_explicit((_atomic_type *)vp, memory_order_acquire));           \
    }                                                                                      \
    static inline void __wt_atomic_store_##suffix##_release(_type *vp, _type v)            \
    {                                                                                      \
        atomic_store_explicit((_atomic_type *)vp, v, memory_order_release);                \
    }                                                                                      \
    static inline _type __wt_atomic_load_##suffix##_v_relaxed(volatile _type *vp)          \
    {                                                                                      \
        return (atomic_load_explicit((volatile _atomic_type *)vp, memory_order_relaxed));  \
    }                                                                                      \
    static inline void __wt_atomic_store_##suffix##_v_relaxed(volatile _type *vp, _type v) \
    {                                                                                      \
        atomic_store_explicit((volatile _atomic_type *)vp, v, memory_order_relaxed);       \
    }                                                                                      \
    static inline _type __wt_atomic_load_##suffix##_v_acquire(volatile _type *vp)          \
    {                                                                                      \
        return (atomic_load_explicit((volatile _atomic_type *)vp, memory_order_acquire));  \
    }                                                                                      \
    static inline void __wt_atomic_store_##suffix##_v_release(volatile _type *vp, _type v) \
    {                                                                                      \
        atomic_store_explicit((volatile _atomic_type *)vp, v, memory_order_release);       \
    }

#define WT_ATOMIC_CAS_FUNC(suffix, _type, _atomic_type)                                        \
    static inline bool __wt_atomic_cas_##suffix(_type *vp, _type old, _type newv)              \
    {                                                                                          \
        return (atomic_compare_exchange_strong((_atomic_type *)vp, &old, newv));               \
    }                                                                                          \
    static inline bool __wt_atomic_cas_##suffix##_v(volatile _type *vp, _type old, _type newv) \
    {                                                                                          \
        return (atomic_compare_exchange_strong((volatile _atomic_type *)vp, &old, newv));      \
    }

#define WT_ATOMIC_FUNC(suffix, _type, _atomic_type)                                          \
    static inline _type __wt_atomic_add_##suffix(_type *vp, _type v)                         \
    {                                                                                        \
        return (atomic_fetch_add((_atomic_type *)vp, v) + v);                                \
    }                                                                                        \
    static inline _type __wt_atomic_add_##suffix##_relaxed(_type *vp, _type v)               \
    {                                                                                        \
        return (atomic_fetch_add_explicit((_atomic_type *)vp, v, memory_order_relaxed) + v); \
    }                                                                                        \
    static inline _type __wt_atomic_fetch_add_##suffix(_type *vp, _type v)                   \
    {                                                                                        \
        return (atomic_fetch_add((_atomic_type *)vp, v));                                    \
    }                                                                                        \
    static inline _type __wt_atomic_sub_##suffix(_type *vp, _type v)                         \
    {                                                                                        \
        return (atomic_fetch_sub((_atomic_type *)vp, v) - v);                                \
    }                                                                                        \
    static inline _type __wt_atomic_add_##suffix##_v(volatile _type *vp, _type v)            \
    {                                                                                        \
        return (atomic_fetch_add((volatile _atomic_type *)vp, v) + v);                       \
    }                                                                                        \
    static inline _type __wt_atomic_fetch_add_##suffix##_v(volatile _type *vp, _type v)      \
    {                                                                                        \
        return (atomic_fetch_add((volatile _atomic_type *)vp, v));                           \
    }                                                                                        \
    static inline _type __wt_atomic_sub_##suffix##_v(volatile _type *vp, _type v)            \
    {                                                                                        \
        return (atomic_fetch_sub((volatile _atomic_type *)vp, v) - v);                       \
    }                                                                                        \
    WT_ATOMIC_CAS_FUNC(suffix, _type, _atomic_type)                                          \
    WT_ATOMIC_FUNC_STORE_LOAD(suffix, _type, _atomic_type)

/* Generate atomic functions for all supported types */
WT_ATOMIC_FUNC(uint8, uint8_t, atomic_uint_least8_t)
WT_ATOMIC_FUNC(uint16, uint16_t, atomic_uint_least16_t)
WT_ATOMIC_FUNC(uint32, uint32_t, atomic_uint_least32_t)
WT_ATOMIC_FUNC(uint64, uint64_t, atomic_uint_least64_t)
WT_ATOMIC_FUNC(int8, int8_t, atomic_int_least8_t)
WT_ATOMIC_FUNC(int16, int16_t, atomic_int_least16_t)
WT_ATOMIC_FUNC(int32, int32_t, atomic_int_least32_t)
WT_ATOMIC_FUNC(int64, int64_t, atomic_int_least64_t)
WT_ATOMIC_FUNC(size, size_t, atomic_size_t)

/* Boolean atomics */
WT_ATOMIC_FUNC_STORE_LOAD(bool, bool, atomic_bool)

/*
 * __wt_atomic_load_double_relaxed --
 *     Atomically read a double variable using C11 atomics.
 */
static inline double
__wt_atomic_load_double_relaxed(double *vp)
{
    return (atomic_load_explicit((_Atomic double *)vp, memory_order_relaxed));
}

/*
 * __wt_atomic_store_double_relaxed --
 *     Atomically set a double variable using C11 atomics.
 */
static inline void
__wt_atomic_store_double_relaxed(double *vp, double v)
{
    atomic_store_explicit((_Atomic double *)vp, v, memory_order_relaxed);
}

/* Size-based generic atomic operations using sizeof() */
#define ATOMIC_LOAD_GENERIC_BY_SIZE(vp)                                             \
    (sizeof(*(vp)) == 1 ?                                                           \
        atomic_load_explicit((atomic_uint_least8_t *)(vp), memory_order_relaxed) :  \
        sizeof(*(vp)) == 2 ?                                                        \
        atomic_load_explicit((atomic_uint_least16_t *)(vp), memory_order_relaxed) : \
        sizeof(*(vp)) == 4 ?                                                        \
        atomic_load_explicit((atomic_uint_least32_t *)(vp), memory_order_relaxed) : \
        atomic_load_explicit((atomic_uint_least64_t *)(vp), memory_order_relaxed))

#define ATOMIC_STORE_GENERIC_BY_SIZE(vp, v)                                             \
    (sizeof(*(vp)) == 1 ?                                                               \
        atomic_store_explicit((atomic_uint_least8_t *)(vp), v, memory_order_relaxed) :  \
        sizeof(*(vp)) == 2 ?                                                            \
        atomic_store_explicit((atomic_uint_least16_t *)(vp), v, memory_order_relaxed) : \
        sizeof(*(vp)) == 4 ?                                                            \
        atomic_store_explicit((atomic_uint_least32_t *)(vp), v, memory_order_relaxed) : \
        atomic_store_explicit((atomic_uint_least64_t *)(vp), v, memory_order_relaxed))

#define __wt_atomic_load_enum_relaxed(vp) ATOMIC_LOAD_GENERIC_BY_SIZE(vp)
#define __wt_atomic_store_enum_relaxed(vp, v) ATOMIC_STORE_GENERIC_BY_SIZE(vp, v)

#define __wt_atomic_load_ptr_relaxed(vp) \
    atomic_load_explicit((_Atomic(__typeof__(*(vp))) *)vp, memory_order_relaxed)
#define __wt_atomic_store_ptr_relaxed(vp, v) \
    atomic_store_explicit((_Atomic(__typeof__(*(vp))) *)vp, (void *)v, memory_order_relaxed)
#define __wt_atomic_load_ptr_acquire(vp) \
    atomic_load_explicit((_Atomic(__typeof__(*(vp))) *)vp, memory_order_acquire)
#define __wt_atomic_store_ptr_release(vp, v) \
    atomic_store_explicit((_Atomic(__typeof__(*(vp))) *)vp, v, memory_order_release)

/*
 * __wt_atomic_cas_ptr --
 *     Pointer compare and swap.
 */
static inline bool
__wt_atomic_cas_ptr(void *vp, void *old, void *newv)
{
    return (atomic_compare_exchange_strong((_Atomic(__typeof__(*(void **)vp)) *)vp, &old, newv));
}

/* Generic atomic operations using size-based dispatch */
#define __wt_atomic_and_generic_relaxed(vp, v)                                              \
    (sizeof(*(vp)) == 1 ?                                                                   \
        atomic_fetch_and_explicit((atomic_uint_least8_t *)(vp), v, memory_order_relaxed) :  \
        sizeof(*(vp)) == 2 ?                                                                \
        atomic_fetch_and_explicit((atomic_uint_least16_t *)(vp), v, memory_order_relaxed) : \
        sizeof(*(vp)) == 4 ?                                                                \
        atomic_fetch_and_explicit((atomic_uint_least32_t *)(vp), v, memory_order_relaxed) : \
        atomic_fetch_and_explicit((atomic_uint_least64_t *)(vp), v, memory_order_relaxed))

#define __wt_atomic_or_generic_relaxed(vp, v)                                              \
    (sizeof(*(vp)) == 1 ?                                                                  \
        atomic_fetch_or_explicit((atomic_uint_least8_t *)(vp), v, memory_order_relaxed) :  \
        sizeof(*(vp)) == 2 ?                                                               \
        atomic_fetch_or_explicit((atomic_uint_least16_t *)(vp), v, memory_order_relaxed) : \
        sizeof(*(vp)) == 4 ?                                                               \
        atomic_fetch_or_explicit((atomic_uint_least32_t *)(vp), v, memory_order_relaxed) : \
        atomic_fetch_or_explicit((atomic_uint_least64_t *)(vp), v, memory_order_relaxed))

#define __wt_atomic_load_generic_relaxed(vp) ATOMIC_LOAD_GENERIC_BY_SIZE(vp)
#define __wt_atomic_store_generic_relaxed(vp, v) ATOMIC_STORE_GENERIC_BY_SIZE(vp, v)
