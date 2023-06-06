/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __conf_string_to_type --
 *     Convert a type name string into an enum representing the type.
 */
static int
__conf_string_to_type(WT_SESSION_IMPL *session, const char *typename, WT_CONFIG_ITEM_TYPE *result)
{
    switch (*typename) {
    case 'b':
        if (WT_STREQ(typename, "boolean")) {
            *result = WT_CONFIG_ITEM_BOOL;
            return (0);
        }
        break;
    case 'c':
        if (WT_STREQ(typename, "category")) {
            *result = WT_CONFIG_ITEM_STRUCT;
            return (0);
        }
        break;
    case 'i':
        if (WT_STREQ(typename, "int")) {
            *result = WT_CONFIG_ITEM_NUM;
            return (0);
        }
        break;
    case 'l':
        if (WT_STREQ(typename, "list")) {
            *result = WT_CONFIG_ITEM_STRUCT;
            return (0);
        }
        break;
    case 's':
        if (WT_STREQ(typename, "string")) {
            *result = WT_CONFIG_ITEM_STRING;
            return (0);
        }
        break;
    }
    WT_RET_PANIC(session, EINVAL, "illegal type string found in configuration: %s", typename);
}

/*
 * __conf_compile_value --
 *     Compile a value into the compiled struct.
 */
static int
__conf_compile_value(WT_SESSION_IMPL *session, WT_CONF *top_conf, WT_CONFIG_ITEM_TYPE check_type,
  WT_CONF_KEY *conf_key, const WT_CONFIG_CHECK *check, WT_CONFIG_ITEM *value, bool is_default)
{
    uint32_t bind_offset;

    if (value->len > 0 && value->str[0] == '%') {
        if (value->str[1] == 'd') {
            if (check_type != WT_CONFIG_ITEM_NUM && check_type != WT_CONFIG_ITEM_BOOL)
                WT_RET_MSG(session, EINVAL, "Value '%.*s' is not compatible with %s type",
                  (int)value->len, value->str, check->type);
        } else if (value->str[1] == 's') {
            if (check_type != WT_CONFIG_ITEM_STRING && check_type != WT_CONFIG_ITEM_STRUCT)
                WT_RET_MSG(session, EINVAL, "Value '%.*s' is not compatible with %s type",
                  (int)value->len, value->str, check->type);
        } else
            WT_RET_MSG(session, EINVAL, "Value '%.*s' does not match %s for binding",
              (int)value->len, value->str, "%d or %s");

        bind_offset = top_conf->binding_count++;

        if (conf_key->type == CONF_KEY_BIND_DESC)
            WT_RET_MSG(session, EINVAL, "Value '%.*s' cannot be used on the same key twice",
              (int)value->len, value->str);

        conf_key->type = CONF_KEY_BIND_DESC;
        conf_key->u.bind_desc.type = check_type;
        conf_key->u.bind_desc.offset = bind_offset;
        WT_RET(__wt_realloc_def(session, &top_conf->binding_allocated, top_conf->binding_count,
          &top_conf->binding_descriptions));
        top_conf->binding_descriptions[bind_offset] = &conf_key->u.bind_desc;
    } else if (check_type == WT_CONFIG_ITEM_STRUCT) {
        if (value->type != WT_CONFIG_ITEM_STRUCT || value->str[0] != '[')
            WT_RET_MSG(session, EINVAL, "Value '%.*s' expected to be a category", (int)value->len,
              value->str);
        conf_key->type = CONF_KEY_SUB_INFO;
        abort(); /* TODO: more to do here, call into a variant of __wt_conf_compile */
    } else {
        switch (check_type) {
        case WT_CONFIG_ITEM_NUM:
            if (value->type != WT_CONFIG_ITEM_NUM)
                WT_RET_MSG(session, EINVAL, "Value '%.*s' expected to be an integer",
                  (int)value->len, value->str);
            break;
        case WT_CONFIG_ITEM_BOOL:
            if (value->type != WT_CONFIG_ITEM_BOOL &&
              (value->type != WT_CONFIG_ITEM_NUM || (value->val != 0 && value->val != 1)))
                WT_RET_MSG(session, EINVAL, "Value '%.*s' expected to be a boolean",
                  (int)value->len, value->str);
            break;
        case WT_CONFIG_ITEM_STRING:
            /* Any value passed in, whether it is "123", "true", etc. can be interpreted as a
             * string. */
            break;
        case WT_CONFIG_ITEM_ID:
        case WT_CONFIG_ITEM_STRUCT: /* actually handled previous to this call, case added for picky
                                       compilers */
            return (__wt_illegal_value(session, (int)check_type));
        }

        conf_key->type = is_default ? CONF_KEY_DEFAULT_ITEM : CONF_KEY_NONDEFAULT_ITEM;
        conf_key->u.item = *value;
    }
    return (0);
}

/*
 * __conf_check_compiler --
 *     Compare function used for binary search.
 */
static int
__conf_check_compare(const void *keyvoid, const void *check)
{
    const WT_CONFIG_ITEM *key;

    key = keyvoid;
    return (strncmp(key->str, ((WT_CONFIG_CHECK *)check)->name, key->len));
}

/*
 * __conf_compile --
 *     Compile a configuration string into the compiled struct.
 */
static int
__conf_compile(WT_SESSION_IMPL *session, const char *api, WT_CONF *top_conf, WT_CONF *conf,
  const WT_CONFIG_CHECK *checks, u_int check_count, const uint8_t *check_jump, const char *format,
  size_t format_len, bool is_default)
{
    WT_CONF *sub_conf;
    WT_CONFIG parser;
    const WT_CONFIG_CHECK *check;
    WT_CONFIG_ITEM key, value;
    WT_CONFIG_ITEM_TYPE check_type;
    WT_CONF_KEY *conf_key;
    WT_DECL_RET;
    uint32_t i, key_id, conf_key_pos, subconf_count, subconf_keys;
    bool existing;

    /*
     * Walk through the given configuration string, for each key, look it up. We should find it in
     * the configuration checks array, and the index in that array is both the bit position to flip
     * in the 'set' array, and the position in the values table where we will compile the value.
     */
    __wt_config_initn(session, &parser, format, format_len);
    while ((ret = __wt_config_next(&parser, &key, &value)) == 0) {
        if (key.len == 0 || (uint8_t)key.str[0] > 0x80)
            i = check_count;
        else {
            i = check_jump[(uint8_t)key.str[0]];
            check_count = check_jump[(uint8_t)key.str[0] + 1];
        }
        check = (const WT_CONFIG_CHECK *)bsearch(
          &key, &checks[i], check_count - i, sizeof(WT_CONFIG_CHECK), __conf_check_compare);
        if (check == NULL)
            WT_ERR_MSG(session, EINVAL, "Error compiling '%s', unknown key '%.*s' for method '%s'",
              format, (int)key.len, key.str, api);

        /* The key id is an offset into the key_map table. */
        key_id = check->key_id;
        WT_ASSERT(session, key_id < WT_CONF_ID_COUNT);
        existing = (conf->key_map[key_id] != 0);
        if (existing)
            /* The position stored is one-based. */
            conf_key_pos = conf->key_map[key_id] - 1;
        else {
            WT_ASSERT_ALWAYS(
              session, conf->conf_key_count < conf->conf_key_max, "conf: key count overflow");
            conf_key_pos = conf->conf_key_count++;
            /* The position inserted to the key_map is one based, and must fit
             * into a byte */
            WT_ASSERT(session, conf_key_pos + 1 <= 0xff);
            conf->key_map[key_id] = (uint8_t)(conf_key_pos + 1);
        }
        conf_key = &conf->conf_key[conf_key_pos];

        WT_ERR(__conf_string_to_type(session, check->type, &check_type));
        if (check_type == WT_CONFIG_ITEM_STRUCT) {
            if (value.type != WT_CONFIG_ITEM_STRUCT)
                WT_ERR_MSG(session, EINVAL, "Value '%.*s' expected to be a category",
                  (int)value.len, value.str);
            if (value.str[0] == '[') {
                if (value.str[value.len - 1] != ']')
                    WT_ERR_MSG(
                      session, EINVAL, "Value '%.*s' non-matching []", (int)value.len, value.str);
            } else if (value.str[0] == '(') {
                if (value.str[value.len - 1] != ')')
                    WT_ERR_MSG(
                      session, EINVAL, "Value '%.*s' non-matching ()", (int)value.len, value.str);
            } else
                WT_ERR_MSG(
                  session, EINVAL, "Value '%.*s' expected () or []", (int)value.len, value.str);

            /* Remove the first and last char, they were just checked */
            ++value.str;
            value.len -= 2;

            if (existing) {
                WT_ASSERT(session, conf_key->type == CONF_KEY_SUB_INFO);
                sub_conf = conf_key->u.sub;
                WT_ASSERT(session, sub_conf != NULL);
            } else {
                WT_ASSERT_ALWAYS(
                  session, conf->conf_count < conf->conf_max, "conf: sub-configuration overflow");

                conf_key->type = CONF_KEY_SUB_INFO;

                sub_conf = &conf[conf->conf_count];
                sub_conf->compile_time_entry = top_conf->compile_time_entry;
                sub_conf->conf_key_count = 0;
                sub_conf->conf_key = &conf->conf_key[conf->conf_key_count];
                sub_conf->conf_key_max = conf->conf_key_max - conf->conf_key_count;
                /*
                 * The sub-configuration count needs to count itself.
                 */
                sub_conf->conf_count = 1;
                sub_conf->conf_max = conf->conf_max - conf->conf_count;

                ++conf->conf_count;
                conf_key->u.sub = sub_conf;
            }

            /*
             * Before we compile the sub-configuration, take note of the current counts there, we'll
             * need to adjust our counts when it's done.
             */
            subconf_keys = sub_conf->conf_key_count;
            subconf_count = sub_conf->conf_count;

            /* Compile the sub-configuration and adjust our counts. */
            WT_ERR(__conf_compile(session, api, top_conf, sub_conf, check->subconfigs,
              check->subconfigs_entries, check->subconfigs_jump, value.str, value.len, is_default));
            conf->conf_key_count += (sub_conf->conf_key_count - subconf_keys);
            conf->conf_count += (sub_conf->conf_count - subconf_count);
        } else
            /*
             * TODO: if check->checks starts with "choices=[...]", we should make sure the value
             * matches one.
             */
            WT_ERR(__conf_compile_value(
              session, top_conf, check_type, conf_key, check, &value, is_default));
    }
    WT_ERR_NOTFOUND_OK(ret, false);
err:
    return (ret);
}

/*
 * __wt_conf_compile --
 *     Compile a configuration string in a way that can be used by API calls.
 */
int
__wt_conf_compile(
  WT_SESSION_IMPL *session, const char *api, const char *format, const char **resultp)
{
    WT_CONF *conf;
    const WT_CONFIG_ENTRY *centry;
    const WT_CONF_SIZING *sizing;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    size_t format_len;
    uint32_t compiled_entry;
    u_int config_num;
    char *format_copy;
    const char *cfgs[3] = {NULL, NULL, NULL};
    void *buf;

    conn = S2C(session);
    conf = NULL;
    format_len = strlen(format);
    *resultp = NULL;

    centry = __wt_conn_config_match(api);
    if (centry == NULL)
        WT_RET_MSG(session, EINVAL, "Error compiling configuration, unknown method '%s'", api);

    if (!centry->compilable)
        WT_RET_MSG(session, ENOTSUP,
          "Error compiling, method '%s' does not support compiled configurations", centry->method);

    /* Get sizing information for the API we are compiling. */
    WT_ERR(__wt_conn_config_entry_number(centry, &config_num));
    WT_ASSERT(session, config_num < WT_ELEMENTS(__wt_conf_sizing));
    sizing = &__wt_conf_sizing[config_num];

    /*
     * Keep a copy of the original configuration string, as the caller may reuse their own string,
     * and we will need to have valid pointers to values in the configuration when the precompiled
     * information is used.
     */
    WT_ERR(__wt_strndup(session, format, format_len, &format_copy));

    cfgs[0] = centry->base;
    cfgs[1] = format_copy;
    WT_ERR(__wt_calloc(session, sizing->total_size, 1, &buf));
    conf = buf;
    conf->compiled_type = CONF_COMPILED_EXPLICIT;
    conf->orig_config = format_copy;

    WT_ERR(__wt_conf_compile_config_strings(session, centry, sizing, cfgs, 1, conf));

    /*
     * The entry compiled. Now put it into the connection array if there's room.
     */
    compiled_entry = __wt_atomic_fetch_addv32(&conn->conf_size, 1);
    if (compiled_entry >= conn->conf_max)
        WT_ERR_MSG(session, EINVAL,
          "Error compiling '%s', overflowed maximum compile slots of %" PRIu32, format,
          conn->conf_max);
    conn->conf_array[compiled_entry] = conf;

    /*
     * Mark the entry so it won't be mistakenly freed when used for API calls.
     */

    *resultp = &conn->conf_dummy[compiled_entry];

err:
    if (ret != 0)
        __wt_conf_compile_free(session, conf);

    return (ret);
}

/*
 * __wt_conf_compile_api_call --
 *     Given an array of config strings, parse them, returning the compiled structure. This is
 *     called from an API call.
 */
int
__wt_conf_compile_api_call(WT_SESSION_IMPL *session, const WT_CONFIG_ENTRY *centry,
  u_int centry_index, const char **cfg, void *compile_buf, size_t compile_buf_size, WT_CONF **confp)
{
    WT_CONF *conf, *preconf;
    const WT_CONF_SIZING *sizing;
    WT_DECL_RET;
    u_int last;

    if (!centry->compilable)
        WT_RET_MSG(session, ENOTSUP,
          "Error compiling, method '%s' does not support compiled configurations", centry->method);

    conf = NULL;
    sizing = &__wt_conf_sizing[centry_index];

    /* Verify we have the needed size. */
    WT_ASSERT_ALWAYS(session, sizing->total_size == compile_buf_size,
      "conf: total size does not equal calculated size");

    /* Find the last entry. */
    for (last = 0; cfg[last] != NULL; ++last)
        ;
    --last;

    /*
     * If an entry is precompiled, it will be the last one. A precompiled entry already includes the
     * default values, so very little needs to be done in that case.
     */
    if (__wt_conf_get_compiled(S2C(session), cfg[last], &preconf)) {
        memcpy(compile_buf, preconf, compile_buf_size);
        conf = compile_buf;

        conf->compiled_type = CONF_COMPILED_IMPLICIT; /* Stack allocated, no frees needed. */
        *confp = conf;
        return (0);
    }
    memset(compile_buf, 0, compile_buf_size);
    conf = compile_buf;
    conf->compiled_type = CONF_COMPILED_IMPLICIT; /* Stack allocated, no frees needed. */

    WT_ERR(__wt_conf_compile_config_strings(session, centry, sizing, cfg, last, conf));
    *confp = conf;

err:
    if (ret != 0)
        __wt_conf_compile_free(session, conf);

    return (ret);
}

/*
 * __wt_conf_compile_config_strings --
 *     Given an array of config strings, parse them, returning the compiled structure.
 */
int
__wt_conf_compile_config_strings(WT_SESSION_IMPL *session, const WT_CONFIG_ENTRY *centry,
  const WT_CONF_SIZING *sizing, const char **cfg, u_int last, WT_CONF *conf)
{
    u_int i, nconf, nkey;

    nconf = sizing->conf_count;
    nkey = sizing->key_count;

    /*
     * The layout of the compiled conf starts with N conf structs, followed by M key structs.
     */
    conf->conf_key = (WT_CONF_KEY *)&conf[nconf];

    conf->compile_time_entry = centry;
    conf->conf_key_max = nkey;
    conf->conf_max = nconf;
    conf->conf_count = 1; /* The current conf is counted. */

    for (i = 0; cfg[i] != NULL; ++i)
        /* Every entry but the last is considered a "default" entry. */
        WT_RET(__conf_compile(session, centry->method, conf, conf, centry->checks,
          centry->checks_entries, centry->checks_jump, cfg[i], strlen(cfg[i]), i != last));

    WT_ASSERT_ALWAYS(session, conf->conf_key_count <= nkey, "conf: key count overflow");
    WT_ASSERT_ALWAYS(session, conf->conf_count <= nconf, "conf: sub-conf count overflow");

    return (0);
}

/*
 * __wt_conf_compile_init --
 *     Initialization for the configuration compilation system.
 */
int
__wt_conf_compile_init(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    size_t i, lastlen;
    char *cs;

    conn = S2C(session);
    WT_RET(__wt_config_gets(session, cfg, "compile_configuration_count", &cval));
    conn->conf_max = (uint32_t)cval.val;

    /*
     * Compiled strings will look like some number of '~'s, with offset number at every ten:
     *    "0~~~~~~~~~10~~~~~~~~20..."
     * By design, this will give a configuration error if mistakenly interpreted directly, and
     * makes it easier to debug.
     */
    WT_RET(__wt_calloc(session, conn->conf_max + 2, 1, &cs));
    memset(cs, '~', conn->conf_max + 1);
    lastlen = 1;
    for (i = 0; i < conn->conf_max - lastlen - 2; i += 10) {
        WT_RET(
          __wt_snprintf_len_set(&cs[i], conn->conf_max - i - lastlen - 2, &lastlen, "%d", (int)i));
        cs[i + lastlen] = '~';
    }
    conn->conf_dummy = cs;
    WT_RET(__wt_calloc_def(session, conn->conf_max, &conn->conf_array));
    conn->conf_size = 0;

    return (0);
}

/*
 * __wt_conf_compile_free --
 *     Free one compiled item.
 */
void
__wt_conf_compile_free(WT_SESSION_IMPL *session, WT_CONF *conf)
{
    /*
     * Don't mistakenly free a compiled entry that has already been handed back to a user or is one
     * of the initial compilations of base APIs.
     */
    if (conf != NULL && conf->compiled_type == CONF_COMPILED_EXPLICIT) {
        __wt_free(session, conf->orig_config);
        __wt_free(session, conf->binding_descriptions);
        __wt_free(session, conf);
    }
}

/*
 * __wt_conf_compile_discard --
 *     Discard compiled configuration info.
 */
void
__wt_conf_compile_discard(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    uint32_t i;

    conn = S2C(session);
    __wt_free(session, conn->conf_dummy);
    if (conn->conf_array != NULL) {
        for (i = 0; i < conn->conf_size; ++i)
            __wt_conf_compile_free(session, conn->conf_array[i]);
        __wt_free(session, conn->conf_array);
    }
}
