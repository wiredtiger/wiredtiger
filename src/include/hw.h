/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#pragma once

/*
 * -- __wti_hw_proc_freq_hz
 *      Return the number of nanoseconds per cpu "tick". If the value is unknown
 *      the function returns 0.
 */
uint64_t __wti_hw_proc_freq_hz(void);
