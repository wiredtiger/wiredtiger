/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* Check if a given verbosity level satisfies the verbosity level of a category. */
#define WT_VERBOSE_LEVEL_ISSET(session, category, level) (level <= S2C(session)->verbose[category])

/*
 * Given this verbosity check is without an explicit verbosity level, the macro checks whether the
 * given category satisfies the default verbosity level.
 */
#define WT_VERBOSE_ISSET(session, category) \
    WT_VERBOSE_LEVEL_ISSET(session, category, WT_VERBOSE_DEFAULT)

/*
 * __wt_verbose_level --
 *     Display a verbose message considering a category and a verbosity level.
 */
#define __wt_verbose_level(session, category, level, fmt, ...)                 \
    do {                                                                       \
        if (WT_VERBOSE_LEVEL_ISSET(session, category, level))                  \
            __wt_verbose_worker(session, "[" #category "] " fmt, __VA_ARGS__); \
    } while (0)

/*
 * __wt_verbose_error --
 *     Wrapper to __wt_verbose_level defaulting the verbosity level to WT_VERBOSE_ERROR.
 */
#define __wt_verbose_error(session, category, fmt, ...) \
    __wt_verbose_level(session, category, WT_VERBOSE_ERROR, fmt, __VA_ARGS__)

/*
 * __wt_verbose_warning --
 *     Wrapper to __wt_verbose_level defaulting the verbosity level to WT_VERBOSE_WARNING.
 */
#define __wt_verbose_warning(session, category, fmt, ...) \
    __wt_verbose_level(session, category, WT_VERBOSE_WARNING, fmt, __VA_ARGS__)

/*
 * __wt_verbose_info --
 *     Wrapper to __wt_verbose_level defaulting the verbosity level to WT_VERBOSE_INFO.
 */
#define __wt_verbose_info(session, category, fmt, ...) \
    __wt_verbose_level(session, category, WT_VERBOSE_INFO, fmt, __VA_ARGS__)

/*
 * __wt_verbose_debug --
 *     Wrapper to __wt_verbose_level using the default verbosity level.
 */
#define __wt_verbose_debug(session, category, fmt, ...) \
    __wt_verbose_level(session, category, WT_VERBOSE_DEBUG, fmt, __VA_ARGS__)

/*
 * __wt_verbose --
 *     Display a verbose message using the default verbosity level. Not an inlined function because
 *     you can't inline functions taking variadic arguments and we don't want to make a function
 *     call in production systems just to find out a verbose flag isn't set. The macro must take a
 *     format string and at least one additional argument, there's no portable way to remove the
 *     comma before an empty __VA_ARGS__ value.
 */
#define __wt_verbose(session, category, fmt, ...) \
    __wt_verbose_level(session, category, WT_VERBOSE_DEFAULT, fmt, __VA_ARGS__)

/*
 * __wt_verbose_level_multi --
 *     Display a verbose message, given a set of multiple verbose categories. A verbose message will
 *     be displayed if at least one category in the set satisfies the required verbosity level.
 */
#define __wt_verbose_level_multi(session, multi_category, level, fmt, ...)                    \
    do {                                                                                      \
        uint32_t __v_idx;                                                                     \
        for (__v_idx = 0; __v_idx < multi_category.cnt; __v_idx++) {                          \
            if (WT_VERBOSE_LEVEL_ISSET(session, multi_category.categories[__v_idx], level)) { \
                __wt_verbose_worker(session, "[" #multi_category "] " fmt, __VA_ARGS__);      \
                break;                                                                        \
            }                                                                                 \
        }                                                                                     \
    } while (0)

/*
 * __wt_verbose_multi --
 *     Display a verbose message, given a set of multiple verbose categories using the default
 *     verbosity level.
 */
#define __wt_verbose_multi(session, multi_category, fmt, ...) \
    __wt_verbose_level_multi(session, multi_category, WT_VERBOSE_DEFAULT, fmt, __VA_ARGS__)
