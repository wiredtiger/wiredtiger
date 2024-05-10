#pragma once
/*-
 * Copyright (c) 2024-present MongoDB, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Minimal portable Futex operations API.
 */

/*
 * Futex words are limited 32 bits (Linux).
 */
typedef uint32_t WT_FUTEX_WORD;

/*
 * Number of waiting threads to wake.
 */
typedef enum __wt_futex_wake {
    WT_FUTEX_WAKE_ONE, /* Wake a single waiting thread. */
    WT_FUTEX_WAKE_ALL  /* Wake all waiting threads. */
} WT_FUTEX_WAKE;
