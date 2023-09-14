/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef __WIREDTIGER_ERROR_MODULE_H_
#define __WIREDTIGER_ERROR_MODULE_H_

#include "misc_module.h"
#include "gcc_decl.h"

#include <stdarg.h>
#include <errno.h>

/* Copied from WiredTiger.in */
#ifndef WT_VERBOSE_CATEGORY_DEFAULT
#define WT_VERBOSE_CATEGORY_DEFAULT WT_VERB_DEFAULT
#endif
/*!
 * WiredTiger verbose event categories.
 * Note that the verbose categories cover a wide set of sub-systems and operations
 * within WiredTiger. As such, the categories are subject to change and evolve
 * between different WiredTiger releases.
 */
#ifndef WT_VERBOSE_CATEGORY_DECL
#define WT_VERBOSE_CATEGORY_DECL
typedef enum {
/* VERBOSE ENUM START */
    WT_VERB_API,                  /*!< API messages. */
    WT_VERB_BACKUP,               /*!< Backup messages. */
    WT_VERB_BLKCACHE,
    WT_VERB_BLOCK,                /*!< Block manager messages. */
    WT_VERB_CHECKPOINT,           /*!< Checkpoint messages. */
    WT_VERB_CHECKPOINT_CLEANUP,
    WT_VERB_CHECKPOINT_PROGRESS,  /*!< Checkpoint progress messages. */
    WT_VERB_CHUNKCACHE,           /*!< Chunk cache messages. */
    WT_VERB_COMPACT,              /*!< Compact messages. */
    WT_VERB_COMPACT_PROGRESS,     /*!< Compact progress messages. */
    WT_VERB_DEFAULT,
    WT_VERB_ERROR_RETURNS,
    WT_VERB_EVICT,                /*!< Eviction messages. */
    WT_VERB_EVICTSERVER,          /*!< Eviction server messages. */
    WT_VERB_EVICT_STUCK,
    WT_VERB_EXTENSION,            /*!< Extension messages. */
    WT_VERB_FILEOPS,
    WT_VERB_GENERATION,
    WT_VERB_HANDLEOPS,
    WT_VERB_HS,                   /*!< History store messages. */
    WT_VERB_HS_ACTIVITY,          /*!< History store activity messages. */
    WT_VERB_LOG,                  /*!< Log messages. */
    WT_VERB_LSM,                  /*!< LSM messages. */
    WT_VERB_LSM_MANAGER,
    WT_VERB_MUTEX,
    WT_VERB_METADATA,             /*!< Metadata messages. */
    WT_VERB_OUT_OF_ORDER,
    WT_VERB_OVERFLOW,
    WT_VERB_READ,
    WT_VERB_RECONCILE,            /*!< Reconcile messages. */
    WT_VERB_RECOVERY,             /*!< Recovery messages. */
    WT_VERB_RECOVERY_PROGRESS,    /*!< Recovery progress messages. */
    WT_VERB_RTS,                  /*!< RTS messages. */
    WT_VERB_SALVAGE,              /*!< Salvage messages. */
    WT_VERB_SHARED_CACHE,
    WT_VERB_SPLIT,
    WT_VERB_TEMPORARY,
    WT_VERB_THREAD_GROUP,
    WT_VERB_TIERED,               /*!< Tiered storage messages. */
    WT_VERB_TIMESTAMP,            /*!< Timestamp messages. */
    WT_VERB_TRANSACTION,          /*!< Transaction messages. */
    WT_VERB_VERIFY,               /*!< Verify messages. */
    WT_VERB_VERSION,              /*!< Version messages. */
    WT_VERB_WRITE,
/* VERBOSE ENUM STOP */
    WT_VERB_NUM_CATEGORIES
} WT_VERBOSE_CATEGORY;
#endif
/* Copied from WiredTiger.in */

/* Copied from verbose.h */
/* Default category for messages that don't explicitly specify a category. */
#ifndef WT_VERBOSE_CATEGORY_DEFAULT
#define WT_VERBOSE_CATEGORY_DEFAULT WT_VERB_DEFAULT
#endif
/* Copied from verbose.h */

extern void __wt_errx_func(WT_SESSION_IMPL *session, const char *func, int line,
  WT_VERBOSE_CATEGORY category, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 5, 6)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));

extern void
__wt_abort(WT_SESSION_IMPL *session) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn))
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));

#define __wt_errx(session, ...) \
    __wt_errx_func(session, __PRETTY_FUNCTION__, __LINE__, WT_VERBOSE_CATEGORY_DEFAULT, __VA_ARGS__)

/*
 * Branch prediction hints. If an expression is likely to return true/false we can use this
 * information to improve performance at runtime. This is not supported for MSVC compilers.
 */
#if !defined(_MSC_VER)
#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

/* Return tests. */
#define WT_RET(a)               \
    do {                        \
        int __ret;              \
        if ((__ret = (a)) != 0) \
            return (__ret);     \
    } while (0)

#define WT_ERR_MSG_BUF_LEN 1024

extern int
__wt_vsnprintf_len_incr(char *buf, size_t size, size_t *retsizep, const char *fmt, va_list ap)
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));

/*
 * __wt_snprintf_len_set --
 *     snprintf convenience function, setting the returned size.
 */
static inline int
__wt_snprintf_len_set(char *buf, size_t size, size_t *retsizep, const char *fmt, ...)
  WT_GCC_FUNC_ATTRIBUTE((format(printf, 4, 5)))
{
    WT_DECL_RET;
    va_list ap;

    *retsizep = 0;

    va_start(ap, fmt);
    ret = __wt_vsnprintf_len_incr(buf, size, retsizep, fmt, ap);
    va_end(ap);
    return (ret);
}

/*
 * __wt_snprintf --
 *     snprintf convenience function, ignoring the returned size.
 */
static inline int
__wt_snprintf(char *buf, size_t size, const char *fmt, ...)
  WT_GCC_FUNC_ATTRIBUTE((format(printf, 3, 4)))
{
    WT_DECL_RET;
    size_t len;
    va_list ap;

    len = 0;

    va_start(ap, fmt);
    ret = __wt_vsnprintf_len_incr(buf, size, &len, fmt, ap);
    va_end(ap);
    WT_RET(ret);

    /* It's an error if the buffer couldn't hold everything. */
    return (len >= size ? ERANGE : 0);
}

/*
 * BUILD_ASSERTION_STRING --
 *  Append a common prefix to an assertion message and save into the provided buffer.
 */
#define BUILD_ASSERTION_STRING(session, buf, len, exp, ...)                                        \
    do {                                                                                           \
        size_t _offset;                                                                            \
        _offset = 0;                                                                               \
        WT_IGNORE_RET(                                                                             \
          __wt_snprintf_len_set(buf, len, &_offset, "WiredTiger assertion failed: '%s'. ", #exp)); \
        /* If we would overflow, finish with what we have. */                                      \
        if (_offset < len)                                                                         \
            WT_IGNORE_RET(__wt_snprintf(buf + _offset, len - _offset, __VA_ARGS__));               \
    } while (0)

/*
 * TRIGGER_ABORT --
 *  Abort the program.
 *
 * When unit testing assertions we don't want to call __wt_abort, but we do want to track that we
 * should have done so.
 */
#ifdef HAVE_UNITTEST_ASSERTS
#define TRIGGER_ABORT(session, exp, ...)                                                    \
    do {                                                                                    \
        if ((session) == NULL) {                                                            \
            __wt_errx(                                                                      \
              session, "A non-NULL session must be provided when unit testing assertions"); \
            __wt_abort(session);                                                            \
        }                                                                                   \
        BUILD_ASSERTION_STRING(                                                             \
          session, (session)->unittest_assert_msg, WT_ERR_MSG_BUF_LEN, exp, __VA_ARGS__);   \
        (session)->unittest_assert_hit = true;                                              \
    } while (0)
#else
#define TRIGGER_ABORT(session, exp, ...)                                             \
    do {                                                                             \
        char _buf[WT_ERR_MSG_BUF_LEN];                                               \
        BUILD_ASSERTION_STRING(session, _buf, WT_ERR_MSG_BUF_LEN, exp, __VA_ARGS__); \
        __wt_errx(session, "%s", _buf);                                              \
        __wt_abort(session);                                                         \
    } while (0)
#endif

/*
 * WT_ASSERT --
 *  Assert an expression and abort if it fails.
 *  Only enabled when compiled with HAVE_DIAGNOSTIC=1.
 */
#ifdef HAVE_DIAGNOSTIC
#define WT_ASSERT(session, exp)                                       \
    do {                                                              \
        if (UNLIKELY(!(exp)))                                         \
            TRIGGER_ABORT(session, exp, "Expression returned false"); \
    } while (0)
#else
#define WT_ASSERT(session, exp) WT_UNUSED(session)
#endif

#endif /* __WIREDTIGER_ERROR_MODULE_H_ */
