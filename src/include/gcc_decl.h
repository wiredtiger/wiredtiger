/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Attribute are only permitted on function declarations, not definitions. This macro is a marker
 * for function definitions that is rewritten by dist/s_prototypes to create extern.h.
 */
#define WT_GCC_FUNC_ATTRIBUTE(x)
#define WT_GCC_FUNC_DECL_ATTRIBUTE(x) __attribute__(x)
