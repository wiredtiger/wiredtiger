/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#define WT_COMPAT_MSG_PREFIX "Version incompatibility detected: "

#define WT_DEBUG_POINT ((void *)(uintptr_t)0xdeadbeef)
#define WT_DEBUG_BYTE (0xab)

/* In DIAGNOSTIC mode, yield in places where we want to encourage races. */
#if defined HAVE_DIAGNOSTIC && defined NON_BARRIER_DIAGNOSTIC_YIELDS
#define WT_DIAGNOSTIC_YIELD      \
    do {                         \
        __wt_yield_no_barrier(); \
    } while (0)
#elif defined HAVE_DIAGNOSTIC && !defined NON_BARRIER_DIAGNOSTIC_YIELDS
#define WT_DIAGNOSTIC_YIELD \
    do {                    \
        __wt_yield();       \
    } while (0)
#else
#define WT_DIAGNOSTIC_YIELD
#endif

#define __wt_err(session, error, ...) \
    __wt_err_func(                    \
      session, error, __PRETTY_FUNCTION__, __LINE__, WT_VERBOSE_CATEGORY_DEFAULT, __VA_ARGS__)
#define __wt_errx(session, ...) \
    __wt_errx_func(session, __PRETTY_FUNCTION__, __LINE__, WT_VERBOSE_CATEGORY_DEFAULT, __VA_ARGS__)
#define __wt_panic(session, error, ...) \
    __wt_panic_func(                    \
      session, error, __PRETTY_FUNCTION__, __LINE__, WT_VERBOSE_CATEGORY_DEFAULT, __VA_ARGS__)
#define __wt_set_return(session, error) \
    __wt_set_return_func(session, __PRETTY_FUNCTION__, __LINE__, error)

/* Set "ret" and branch-to-err-label tests. */
#define WT_ERR(a)             \
    do {                      \
        if ((ret = (a)) != 0) \
            goto err;         \
    } while (0)
#define WT_ERR_MSG(session, v, ...)          \
    do {                                     \
        ret = (v);                           \
        __wt_err(session, ret, __VA_ARGS__); \
        goto err;                            \
    } while (0)
#define WT_ERR_TEST(a, v, keep) \
    do {                        \
        if (a) {                \
            ret = (v);          \
            goto err;           \
        } else if (!(keep))     \
            ret = 0;            \
    } while (0)
#define WT_ERR_ERROR_OK(a, e, keep) WT_ERR_TEST((ret = (a)) != 0 && ret != (e), ret, keep)
#define WT_ERR_NOTFOUND_OK(a, keep) WT_ERR_ERROR_OK(a, WT_NOTFOUND, keep)

/* Return WT_PANIC regardless of earlier return codes. */
#define WT_ERR_PANIC(session, v, ...) WT_ERR(__wt_panic(session, v, __VA_ARGS__))

/* Return tests. */
#define WT_RET(a)               \
    do {                        \
        int __ret;              \
        if ((__ret = (a)) != 0) \
            return (__ret);     \
    } while (0)
#define WT_RET_TRACK(a)               \
    do {                              \
        int __ret;                    \
        if ((__ret = (a)) != 0) {     \
            WT_TRACK_OP_END(session); \
            return (__ret);           \
        }                             \
    } while (0)
#define WT_RET_MSG(session, v, ...)            \
    do {                                       \
        int __ret = (v);                       \
        __wt_err(session, __ret, __VA_ARGS__); \
        return (__ret);                        \
    } while (0)
#define WT_RET_TEST(a, v) \
    do {                  \
        if (a)            \
            return (v);   \
    } while (0)
#define WT_RET_ERROR_OK(a, e)                           \
    do {                                                \
        int __ret = (a);                                \
        WT_RET_TEST(__ret != 0 && __ret != (e), __ret); \
    } while (0)
#define WT_RET_BUSY_OK(a) WT_RET_ERROR_OK(a, EBUSY)
#define WT_RET_NOTFOUND_OK(a) WT_RET_ERROR_OK(a, WT_NOTFOUND)
/* Set "ret" if not already set. */
#define WT_TRET(a)                                                                           \
    do {                                                                                     \
        int __ret;                                                                           \
        if ((__ret = (a)) != 0 &&                                                            \
          (__ret == WT_PANIC || ret == 0 || ret == WT_DUPLICATE_KEY || ret == WT_NOTFOUND || \
            ret == WT_RESTART))                                                              \
            ret = __ret;                                                                     \
    } while (0)
#define WT_TRET_ERROR_OK(a, e)                                                               \
    do {                                                                                     \
        int __ret;                                                                           \
        if ((__ret = (a)) != 0 && __ret != (e) &&                                            \
          (__ret == WT_PANIC || ret == 0 || ret == WT_DUPLICATE_KEY || ret == WT_NOTFOUND || \
            ret == WT_RESTART))                                                              \
            ret = __ret;                                                                     \
    } while (0)
#define WT_TRET_BUSY_OK(a) WT_TRET_ERROR_OK(a, EBUSY)
#define WT_TRET_NOTFOUND_OK(a) WT_TRET_ERROR_OK(a, WT_NOTFOUND)

/* Return WT_PANIC regardless of earlier return codes. */
#define WT_RET_PANIC(session, v, ...) return (__wt_panic(session, v, __VA_ARGS__))

/* Called on unexpected code path: locate the failure. */
#define __wt_illegal_value(session, v)             \
    __wt_panic(session, EINVAL, "%s: 0x%" PRIxMAX, \
      "encountered an illegal file format or internal value", (uintmax_t)(v))

/* Branch prediction hints. likely. */
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

/*
 * TRIGGER_ABORT --
 *  Abort the program.
 *
 * When unit testing assertions we don't want to call __wt_abort, but we do want to track that we
 * should have done so.
 */
#ifdef HAVE_UNITTEST_ASSERTS
#define TRIGGER_ABORT(session, exp, ...)                                                       \
    do {                                                                                       \
        size_t _offset;                                                                        \
        if ((session) == NULL) {                                                               \
            __wt_errx(                                                                         \
              session, "A non-NULL session must be provided when unit testing assertions");    \
            __wt_abort(session);                                                               \
        }                                                                                      \
                                                                                               \
        /*                                                                                     \
         * Normally these are two errx messages, but we need to stitch them both together into \
         * unittest_assert_msg. To do so insert the second message immediately after the first \
         */                                                                                    \
        _offset = (size_t)__wt_snprintf((session)->unittest_assert_msg,                        \
          WT_SESSION_UNITTEST_BUF_LEN, "Assertion '%s' failed: ", #exp);                       \
        WT_IGNORE_RET(__wt_snprintf((session)->unittest_assert_msg + _offset,                  \
          WT_SESSION_UNITTEST_BUF_LEN - _offset, __VA_ARGS__));                                \
                                                                                               \
        (session)->unittest_assert_hit = true;                                                 \
    } while (0)
#else
#define TRIGGER_ABORT(session, exp, ...)                     \
    do {                                                     \
        __wt_errx(session, "Assertion '%s' failed: ", #exp); \
        __wt_errx(session, __VA_ARGS__);                     \
        __wt_abort(session);                                 \
    } while (0)
#endif

/*
 * DIAGNOSTIC_ASSERTS_ENABLED --
 *  Fetch whether diagnostic asserts are enabled for this session. In most
 * cases the session inherits this value from the connection.
 */
#define DIAGNOSTIC_ASSERTS_ENABLED(session)                                        \
    (session != NULL) &&                                                           \
      (likely((session)->diagnostic_asserts_level == DIAG_ASSERTS_CONN) ?          \
          FLD_ISSET(S2C(session)->debug_flags, WT_CONN_DEBUG_DIAGNOSTIC_ASSERTS) : \
          (session)->diagnostic_asserts_level == DIAG_ASSERTS_ON)

/*
 * WT_ASSERT --
 *  Assert an expression provided that diagnostic asserts are enabled.
 *
 * FIXME-WT-10045 - WT_ASSERT should now always take an error message as an argument.
 */
#define WT_ASSERT(session, exp)                            \
    do {                                                   \
        if (unlikely(DIAGNOSTIC_ASSERTS_ENABLED(session))) \
            if (unlikely(!(exp))) {                        \
                TRIGGER_ABORT(session, exp, "");           \
            }                                              \
    } while (0)

/*
 * WT_ASSERT_ALWAYS --
 *  Assert an expression regardless of whether diagnostic asserts are enabled.
 */
#define WT_ASSERT_ALWAYS(session, exp, ...)           \
    do {                                              \
        if (unlikely(!(exp))) {                       \
            TRIGGER_ABORT(session, exp, __VA_ARGS__); \
        }                                             \
    } while (0)

/*
 * WT_ERR_ASSERT --
 *  Assert an expression. If diagnostic asserts are enabled then abort the program,
 * otherwise return an error.
 */
#define WT_ERR_ASSERT(session, exp, v, ...)               \
    do {                                                  \
        if (unlikely(!(exp))) {                           \
            if (DIAGNOSTIC_ASSERTS_ENABLED(session)) {    \
                TRIGGER_ABORT(session, exp, __VA_ARGS__); \
            } else                                        \
                WT_ERR_MSG(session, v, __VA_ARGS__);      \
        }                                                 \
    } while (0)

/*
 * WT_RET_ASSERT --
 *  Assert an expression. If diagnostic asserts are enabled then abort the program,
 * otherwise print a message and early return from the function.
 */
#define WT_RET_ASSERT(session, exp, v, ...)               \
    do {                                                  \
        if (unlikely(!(exp))) {                           \
            if (DIAGNOSTIC_ASSERTS_ENABLED(session)) {    \
                TRIGGER_ABORT(session, exp, __VA_ARGS__); \
            } else                                        \
                WT_RET_MSG(session, v, __VA_ARGS__);      \
        }                                                 \
    } while (0)

/*
 * WT_RET_PANIC_ASSERT --
 *  Assert an expression. If diagnostic asserts are enabled then abort the
 * program, otherwise return WT_PANIC from the function.
 */
#define WT_RET_PANIC_ASSERT(session, exp, v, ...)         \
    do {                                                  \
        if (unlikely(!(exp))) {                           \
            if (DIAGNOSTIC_ASSERTS_ENABLED(session)) {    \
                TRIGGER_ABORT(session, exp, __VA_ARGS__); \
            } else                                        \
                WT_RET_PANIC(session, v, __VA_ARGS__);    \
        }                                                 \
    } while (0)
