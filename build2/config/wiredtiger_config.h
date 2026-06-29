/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef __WIREDTIGER_CONFIG_H_
#define __WIREDTIGER_CONFIG_H_

/* Define to 1 to pause for debugger attach on failure. */
/* #undef HAVE_ATTACH */

/* LZ4 support automatically loaded. */
/* #undef HAVE_BUILTIN_EXTENSION_LZ4 */

/* Snappy support automatically loaded. */
/* #undef HAVE_BUILTIN_EXTENSION_SNAPPY */

/* ZLIB support automatically loaded. */
/* #undef HAVE_BUILTIN_EXTENSION_ZLIB */

/* ZSTD support automatically loaded. */
/* #undef HAVE_BUILTIN_EXTENSION_ZSTD */

/* IAA support automatically loaded. */
/* #undef HAVE_BUILTIN_EXTENSION_IAA */

/* libsodium support automatically loaded. */
/* #undef HAVE_BUILTIN_EXTENSION_SODIUM */

/* key provider extension automatically loaded. */
/* #undef HAVE_BUILTIN_EXTENSION_KEY_PROVIDER */

/* Define to 1 for call logging. */
/* #undef HAVE_CALL_LOG */

/* Define to 1 for diagnostic tests. */
#define HAVE_DIAGNOSTIC 1

/* Define to 1 to enable the error log. */
#define HAVE_ERROR_LOG 1

/* Define to 1 for ref tracking */
#define HAVE_REF_TRACK 1

/* Define to 1 for unit tests. */
/* #undef HAVE_UNITTEST */

/*
 * Define to 1 to when measuring code coverage to make measurements more accurate, by switching code where needed,
 * for example from macros to inline functions.
 *
 * An #ifdef guard for CODE_COVERAGE_MEASUREMENT macro can be used as a temporary measure while evaluating the
 * performance of inline functions vs the original macros.
 *
 * Once inline functions have been validated as providing similar (or better) performance than the original macros
 * the macro versions can be removed and the #ifdef guard using CODE_COVERAGE_MEASUREMENT removed to make the inline
 * functions the only version.
 */
/* #undef CODE_COVERAGE_MEASUREMENT */

/* define 1 to to switch code that exists in both inline function and macro versions to use the inline versions. */
/* #undef INLINE_FUNCTIONS_INSTEAD_OF_MACROS */

/*
 * Define to 1 for unit testing assertions.
 * This overrides normal abort logic and should be used *only* when unit testing assertions.
 */
/* #undef HAVE_UNITTEST_ASSERTS */

/* Define to 1 if the user has explicitly enabled memkind builds. */
/* #undef ENABLE_MEMKIND */

/* Define to 1 if the user has set enable antithesis. */
/* #undef ENABLE_ANTITHESIS */

/* Automatically set by the build system, turns on or off optional RCpc ARM instructions. */
/* #undef HAVE_RCPC */

/* Define to 1 to disable any crc32 hardware support. */
/* #undef HAVE_NO_CRC32_HARDWARE */

/*
 * Compile-time platform feature flags.
 */

/* POSIX.1-2001 functions available on every supported POSIX target. */
#if defined(__linux__) || defined(__APPLE__) || defined(__NetBSD__)
#define HAVE_CLOCK_GETTIME 1
#define HAVE_GETTIMEOFDAY 1
#define HAVE_POSIX_MADVISE 1
#define HAVE_POSIX_MEMALIGN 1
#define HAVE_SETRLIMIT 1
#endif

/* POSIX functions macOS does not implement. */
#if defined(__linux__) || defined(__NetBSD__)
#define HAVE_FDATASYNC 1
#define HAVE_POSIX_FADVISE 1
#define HAVE_POSIX_FALLOCATE 1
#define HAVE_PTHREAD_COND_MONOTONIC 1
#define HAVE_TIMER_CREATE 1
#endif

/* Linux-specific syscalls and extensions. */
#if defined(__linux__)
#define HAVE_FALLOCATE 1
#define HAVE_SYNC_FILE_RANGE 1
#endif

/* Architecture-specific intrinsic headers. */
#if defined(__x86_64__) || defined(_M_X64)
#define HAVE_X86INTRIN_H 1
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#define HAVE_ARM_NEON_INTRIN_H 1
#endif

/* Spinlock type from mutex.h. */
#define SPINLOCK_TYPE SPINLOCK_PTHREAD_MUTEX

/* Define to 1 if the target system is big endian */
/* #undef WORDS_BIGENDIAN */

/* Version number of package */
#define VERSION "12.0.0"

/* Define to 1 to support standalone build. */
#define WT_STANDALONE_BUILD 1

#ifndef _DARWIN_USE_64_BIT_INODE
# define _DARWIN_USE_64_BIT_INODE 1
#endif

#endif /* __WIREDTIGER_CONFIG_H_ */
