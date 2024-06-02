#pragma once
/*-
 * Copyright (c) 2024-present MongoDB, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Linux limits the futex size to 32 bits irrespective architecture word size.
 */
typedef uint32_t WT_FUTEX_WORD;

/*
 * Number of waiting threads to wake.
 */
typedef enum __wt_futex_wake { WT_FUTEX_WAKE_ONE, WT_FUTEX_WAKE_ALL } WT_FUTEX_WAKE;
