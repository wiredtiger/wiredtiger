/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_config_replace --
 *     Rebuild a configuration string, substituting value for key. Returns WT_NOTFOUND if key is not
 *     present in base.
 */
int
__wt_config_replace(WT_SESSION_IMPL *session, const char *base, const char *key,
  const WT_CONFIG_ITEM *value, char **config_ret)
{
    WT_CONFIG cparser;
    WT_CONFIG_ITEM k, v;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    bool saw_key;

    *config_ret = NULL;
    saw_key = false;

    WT_RET(__wt_scr_alloc(session, 1024, &tmp));

    __wt_config_init(session, &cparser, base);
    while ((ret = __wt_config_next(&cparser, &k, &v)) == 0) {
        if (k.type != WT_CONFIG_ITEM_STRING && k.type != WT_CONFIG_ITEM_ID)
            WT_ERR_MSG(
              session, EINVAL, "Invalid configuration key found: '%.*s'", (int)k.len, k.str);
        if (WT_CONFIG_MATCH(key, k)) {
            v = *value;
            saw_key = true;
        }
        /* Include the quotes around string keys/values. */
        if (k.type == WT_CONFIG_ITEM_STRING)
            WT_CONFIG_PRESERVE_QUOTES(session, &k);
        if (v.type == WT_CONFIG_ITEM_STRING)
            WT_CONFIG_PRESERVE_QUOTES(session, &v);
        WT_ERR(__wt_buf_catfmt(session, tmp, "%.*s=%.*s,", (int)k.len, k.str, (int)v.len, v.str));
    }
    WT_ERR_NOTFOUND_OK(ret, false);
    if (!saw_key)
        WT_ERR(WT_NOTFOUND);

    if (tmp->size != 0)
        --tmp->size;
    WT_ERR(__wt_strndup(session, tmp->data, tmp->size, config_ret));

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}
