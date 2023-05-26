/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int config_check(WT_SESSION_IMPL *, const WT_CONFIG_CHECK *, u_int, const char *, size_t);

/*
 * __wt_config_check --
 *     Check the keys in an application-supplied config string match what is specified in an array
 *     of check strings.
 */
int
__wt_config_check(
  WT_SESSION_IMPL *session, const WT_CONFIG_ENTRY *entry, const char *config, size_t config_len)
{
    /*
     * Callers don't check, it's a fast call without a configuration or check array.
     */
    return (config == NULL || entry->checks == NULL ||
          (entry->compilable && __wt_conf_is_compiled(S2C(session), config)) ?
        0 :
        config_check(session, entry->checks, entry->checks_entries, config, config_len));
}

/*
 * config_check_search --
 *     Search a set of checks for a matching name.
 */
static inline int
config_check_search(WT_SESSION_IMPL *session, const WT_CONFIG_CHECK *checks, u_int entries,
  const char *str, size_t len, int *ip)
{
    u_int base, indx, limit;
    int cmp;

    /*
     * For standard sets of configuration information, we know how many entries and that they're
     * sorted, do a binary search. Else, do it the slow way.
     */
    if (entries == 0) {
        for (indx = 0; checks[indx].name != NULL; indx++)
            if (WT_STRING_MATCH(checks[indx].name, str, len)) {
                *ip = (int)indx;
                return (0);
            }
    } else
        for (base = 0, limit = entries; limit != 0; limit >>= 1) {
            indx = base + (limit >> 1);
            cmp = strncmp(checks[indx].name, str, len);
            if (cmp == 0 && checks[indx].name[len] == '\0') {
                *ip = (int)indx;
                return (0);
            }
            if (cmp < 0) {
                base = indx + 1;
                --limit;
            }
        }

    WT_RET_MSG(session, EINVAL, "unknown configuration key: '%.*s'", (int)len, str);
}

/*
 * config_check --
 *     Check the keys in an application-supplied config string match what is specified in an array
 *     of check strings.
 */
static int
config_check(WT_SESSION_IMPL *session, const WT_CONFIG_CHECK *checks, u_int checks_entries,
  const char *config, size_t config_len)
{
    WT_CONFIG parser, cparser, sparser;
    const WT_CONFIG_CHECK *check;
    WT_CONFIG_ITEM k, v, ck, cv, dummy;
    WT_DECL_RET;
    int i;
    bool badtype, found;

    /*
     * The config_len parameter is optional, and allows passing in strings that are not
     * nul-terminated.
     */
    if (config_len == 0)
        __wt_config_init(session, &parser, config);
    else
        __wt_config_initn(session, &parser, config, config_len);
    while ((ret = __wt_config_next(&parser, &k, &v)) == 0) {
        if (k.type != WT_CONFIG_ITEM_STRING && k.type != WT_CONFIG_ITEM_ID)
            WT_RET_MSG(
              session, EINVAL, "Invalid configuration key found: '%.*s'", (int)k.len, k.str);

        /* Search for a matching entry. */
        WT_RET(config_check_search(session, checks, checks_entries, k.str, k.len, &i));

        check = &checks[i];
        badtype = false;
        switch (check->compiled_type) {
        case WT_CONFIG_COMPILED_TYPE_BOOLEAN:
            badtype = v.type != WT_CONFIG_ITEM_BOOL &&
              (v.type != WT_CONFIG_ITEM_NUM || (v.val != 0 && v.val != 1));
            break;
        case WT_CONFIG_COMPILED_TYPE_CATEGORY:
            /* Deal with categories of the form: XXX=(XXX=blah). */
            ret = config_check(session, check->subconfigs, check->subconfigs_entries,
              k.str + strlen(check->name) + 1, v.len);
            badtype = ret == EINVAL;
            break;
        case WT_CONFIG_COMPILED_TYPE_FORMAT:
            break;
        case WT_CONFIG_COMPILED_TYPE_INT:
            badtype = v.type != WT_CONFIG_ITEM_NUM;
            break;
        case WT_CONFIG_COMPILED_TYPE_LIST:
            badtype = v.len > 0 && v.type != WT_CONFIG_ITEM_STRUCT;
            break;
        case WT_CONFIG_COMPILED_TYPE_STRING:
        case 's':
            break;
        default:
            WT_RET_MSG(session, EINVAL, "unknown configuration type: '%s'", check->type);
        }
        if (badtype)
            WT_RET_MSG(session, EINVAL, "Invalid value for key '%.*s': expected a %s", (int)k.len,
              k.str, check->type);

        if (check->checkf != NULL)
            WT_RET(check->checkf(session, &v));

        if (check->checks == NULL)
            continue;

#if 0
        /* This always skips the sub parsing for choices. */
        if (check->subconfigs != check) /* TODO!!! */
            continue;
#endif

        /* Setup an iterator for the check string. */
        __wt_config_init(session, &cparser, check->checks);
        while ((ret = __wt_config_next(&cparser, &ck, &cv)) == 0) {
            if (WT_STRING_MATCH("min", ck.str, ck.len)) {
                if (v.val < cv.val)
                    WT_RET_MSG(session, EINVAL,
                      "Value too small for key '%.*s' the minimum is %.*s", (int)k.len, k.str,
                      (int)cv.len, cv.str);
            } else if (WT_STRING_MATCH("max", ck.str, ck.len)) {
                if (v.val > cv.val)
                    WT_RET_MSG(session, EINVAL,
                      "Value too large for key '%.*s' the maximum is %.*s", (int)k.len, k.str,
                      (int)cv.len, cv.str);
            } else if (WT_STRING_MATCH("choices", ck.str, ck.len)) {
                if (v.len == 0)
                    WT_RET_MSG(session, EINVAL, "Key '%.*s' requires a value", (int)k.len, k.str);
                if (v.type == WT_CONFIG_ITEM_STRUCT) {
                    /*
                     * Handle the 'verbose' case of a list containing restricted choices.
                     */
                    __wt_config_subinit(session, &sparser, &v);
                    found = true;
                    while (found && (ret = __wt_config_next(&sparser, &v, &dummy)) == 0) {
                        ret = __wt_config_subgetraw(session, &cv, &v, &dummy);
                        found = ret == 0;
                    }
                } else {
                    ret = __wt_config_subgetraw(session, &cv, &v, &dummy);
                    found = ret == 0;
                }

                if (ret != 0 && ret != WT_NOTFOUND)
                    return (ret);
                if (!found)
                    WT_RET_MSG(session, EINVAL,
                      "Value '%.*s' not a permitted choice for key '%.*s'", (int)v.len, v.str,
                      (int)k.len, k.str);
            } else
                WT_RET_MSG(session, EINVAL, "unexpected configuration description keyword %.*s",
                  (int)ck.len, ck.str);
        }
    }

    if (ret == WT_NOTFOUND)
        ret = 0;

    return (ret);
}
