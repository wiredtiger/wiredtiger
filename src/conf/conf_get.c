/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_conf_gets_def_func --
 *     Given a compiled structure of configuration strings, find the final value for a given key,
 *     represented as (up to 4) 16-bit key ids packed into a 64-bit key. If a default is given, it
 *     overrides any default found in the compiled structure.
 */
int
__wt_conf_gets_func(WT_SESSION_IMPL *session, const WT_CONF_LIST *cfg, uint64_t keys, int def,
  bool use_def, WT_CONFIG_ITEM *value)
{
    WT_CONFIG_ITEM_STATIC_INIT(false_value);
    WT_CONF_BIND_DESC *bind_desc;
    const WT_CONF_COMPILED *compiled;
    WT_CONF_SET_ITEM *si;
    uint32_t partkey, values_off;
    uint8_t set_item_index;

    compiled = cfg;
    WT_ASSERT(session, keys != 0);
    while (keys != 0) {
        partkey = keys & 0xffff;
        WT_ASSERT(session, partkey != 0);

        set_item_index = compiled->key_to_set_item[partkey];
        if (set_item_index == 0)
            return (WT_NOTFOUND);

        /* The value in key_to_set_item is one-based, account for that here. */
        --set_item_index;
        WT_ASSERT(session, set_item_index < compiled->set_item_count);
        si = &compiled->set_item[set_item_index];
        keys >>= 16;

        switch (si->set_type) {
        case CONF_SET_DEFAULT_ITEM:
            if (use_def) {
                *value = false_value;
                value->val = def;
                return (0);
            }

        /* FALLTHROUGH */
        case CONF_SET_NONDEFAULT_ITEM:
            if (keys != 0)
                return (WT_NOTFOUND);
            *value = si->u.item;
            return (0);

        case CONF_SET_BIND_DESC:
            if (keys != 0)
                return (WT_NOTFOUND);
            bind_desc = &si->u.bind_desc;
            values_off = bind_desc->offset + session->conf_bindings.bind_values;
            WT_ASSERT(session,
              bind_desc->offset < cfg->binding_count && values_off <= WT_CONF_BIND_VALUES_LEN);
            /*TODO: desc not set? fixed? */
            WT_ASSERT(session, session->conf_bindings.values[values_off].desc == bind_desc);
            *value = session->conf_bindings.values[values_off].item;
            return (0);

        case CONF_SET_SUB_INFO:
            compiled = si->u.sub;
            break;
        }
    }
    return (WT_NOTFOUND);
}

/*
 * __wt_conf_gets_none_func --
 *     Given a NULL-terminated list of configuration strings, find the final value for a given
 *     string key. Treat "none" as empty.
 */
int
__wt_conf_gets_none_func(
  WT_SESSION_IMPL *session, WT_CONF_LIST *cfg, uint64_t key, WT_CONFIG_ITEM *value)
{
    WT_RET(__wt_conf_gets_func(session, cfg, key, 0, false, value));
    if (WT_STRING_MATCH("none", value->str, value->len))
        value->len = 0;
    return (0);
}

/*
 * __wt_conf_getone --
 *     Get the value for a given key from a single config string.
 */
int
__wt_conf_getone(WT_SESSION_IMPL *session, const char *config, uint64_t key, WT_CONFIG_ITEM *value)
{
    WT_UNUSED(session);
    WT_UNUSED(config);
    WT_UNUSED(key);
    WT_UNUSED(value);

    /* TODO */
    return (ENOTSUP);
}

/*
 * __wt_conf_getones --
 *     Get the value for a given string key from a single config string.
 */
int
__wt_conf_getones(WT_SESSION_IMPL *session, const char *config, uint64_t key, WT_CONFIG_ITEM *value)
{
    WT_UNUSED(session);
    WT_UNUSED(config);
    WT_UNUSED(key);
    WT_UNUSED(value);

    /* TODO */
    return (ENOTSUP);
}

/*
 * __wt_conf_getones_none --
 *     Given a compiled structure of configuration strings, find the final value for a given key,
 *     represented as (up to 4) 16-bit key ids packed into a 64-bit key. Treat "none" as empty.
 */
int
__wt_conf_getones_none(
  WT_SESSION_IMPL *session, const char *config, uint64_t key, WT_CONFIG_ITEM *value)
{
    WT_RET(__wt_conf_getones(session, config, key, value));
    if (WT_STRING_MATCH("none", value->str, value->len))
        value->len = 0;
    return (0);
}
