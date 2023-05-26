/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

static inline bool
__wt_conf_get_compiled(WT_CONNECTION_IMPL *conn, const char *config, WT_CONF_COMPILED **compiledp)
{
    ssize_t offset;

    offset = (config - conn->conf_compiled_dummy);
    if (offset < 0 || offset >= conn->conf_compiled_size)
        return (false);

    *compiledp = conn->conf_compiled_array[offset];
    return (true);
}

static inline bool
__wt_conf_is_compiled(WT_CONNECTION_IMPL *conn, const char *config)
{
    ssize_t offset;

    offset = (config - conn->conf_compiled_dummy);
    return (offset >= 0 && offset < conn->conf_compiled_size);
}
