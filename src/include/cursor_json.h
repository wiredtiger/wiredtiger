/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_json and related definitions. */

struct __wt_cursor_json {
    char *key_buf;              /* JSON formatted string */
    char *value_buf;            /* JSON formatted string */
    WT_CONFIG_ITEM key_names;   /* Names of key columns */
    WT_CONFIG_ITEM value_names; /* Names of value columns */
};
