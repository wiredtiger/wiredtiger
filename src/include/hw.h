/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#pragma once

/*
 * -- __wti_hw_nsec_per_tick
 *      Return the number of nanoseconds per cpu "tick". If the value is unknown
 *      the function returns 0.
 */
double __wti_hw_nsec_per_tick(void);
