/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __wt_conf_get_compiled --
 *     Return true if and only if the given string is a dummy compiled string, and if so, return the compiled
 *     structure.
 */
static inline bool
__wt_conf_get_compiled(WT_CONNECTION_IMPL *conn, const char *config, WT_CONF **confp)
{
    ssize_t offset;

    offset = (config - conn->conf_dummy);
    if (offset < 0 || offset >= conn->conf_size)
        return (false);

    *confp = conn->conf_array[offset];
    return (true);
}

/*
 * __wt_conf_is_compiled --
 *     Return true if and only if the given string is a dummy compiled string.
 */
static inline bool
__wt_conf_is_compiled(WT_CONNECTION_IMPL *conn, const char *config)
{
    ssize_t offset;

    offset = (config - conn->conf_dummy);
    return (offset >= 0 && offset < conn->conf_size);
}
