/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __config_check(
  WT_SESSION_IMPL *, const WT_CONFIG_CHECK *, u_int, const uint8_t *, const char *, size_t);

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
    return (config == NULL || entry->checks == NULL ?
        0 :
        __config_check(
          session, entry->checks, entry->checks_entries, entry->checks_jump, config, config_len));
}

/*
 * __config_check_compare --
 *     Compare function used for binary search.
 */
static int
__config_check_compare(const void *keyvoid, const void *checkvoid)
{
    const WT_CONFIG_ITEM *key;
    const WT_CONFIG_CHECK *check;
    int cmp;

    key = keyvoid;
    check = checkvoid;
    cmp = strncmp(key->str, check->name, key->len);
    if (cmp == 0) {
        if (check->name[key->len] == '\0')
            return (0);
        cmp = -1;
    }
    return (cmp);
}

/*
 * __config_check_search --
 *     Search a set of checks for a matching name.
 */
static inline int
__config_check_search(WT_SESSION_IMPL *session, const WT_CONFIG_CHECK *checks, u_int entries,
  const WT_CONFIG_ITEM *item, const uint8_t *check_jump, const WT_CONFIG_CHECK **resultp)
{
    u_int check_count, indx;
    int ch;

    /*
     * For standard sets of configuration information, we know how many entries and that they're
     * sorted, do a binary search. Else, do it the slow way.
     */
    if (entries == 0) {
        for (indx = 0; checks[indx].name != NULL; indx++)
            if (WT_STRING_MATCH(checks[indx].name, item->str, item->len)) {
                *resultp = &checks[indx];
                return (0);
            }
    } else {
        check_count = entries;
        if (item->len == 0 || ((ch = item->str[0]) + 1 >= 0x80))
            indx = check_count;
        else {
            indx = check_jump[ch];
            check_count = check_jump[ch + 1];
        }
        *resultp = (const WT_CONFIG_CHECK *)bsearch(
          item, &checks[indx], check_count - indx, sizeof(WT_CONFIG_CHECK), __config_check_compare);
        if (*resultp != NULL)
            return (0);
    }

    WT_RET_MSG(session, EINVAL, "unknown configuration key '%.*s'", (int)item->len, item->str);
}

/*
 * __config_get_choice --
 *     Walk through list of legal choices looking for an item.
 */
static bool
__config_get_choice(const char **choices, WT_CONFIG_ITEM *item)
{
    const char **choice;
    bool found;

    found = false;
    for (choice = choices; *choice != NULL; ++choice)
        if (WT_STRING_MATCH(*choice, item->str, item->len)) {
            found = true;
            break;
        }
    return (found);
}

/*
 * __config_check --
 *     Check the keys in an application-supplied config string match what is specified in an array
 *     of check strings.
 */
static int
__config_check(WT_SESSION_IMPL *session, const WT_CONFIG_CHECK *checks, u_int checks_entries,
  const uint8_t *check_jump, const char *config, size_t config_len)
{
    WT_CONFIG parser, sparser;
    const WT_CONFIG_CHECK *check;
    WT_CONFIG_ITEM k, v, dummy;
    WT_DECL_RET;
    const char **choices;
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
        WT_RET(__config_check_search(session, checks, checks_entries, &k, check_jump, &check));

        if (strcmp(check->type, "boolean") == 0) {
            badtype = v.type != WT_CONFIG_ITEM_BOOL &&
              (v.type != WT_CONFIG_ITEM_NUM || (v.val != 0 && v.val != 1));
        } else if (strcmp(check->type, "category") == 0) {
            /* Deal with categories of the form: XXX=(XXX=blah). */
            ret = __config_check(session, check->subconfigs, check->subconfigs_entries,
              check->subconfigs_jump, k.str + strlen(check->name) + 1, v.len);
            if (ret != EINVAL)
                badtype = false;
            else
                badtype = true;
        } else if (strcmp(check->type, "format") == 0) {
            badtype = false;
        } else if (strcmp(check->type, "int") == 0) {
            badtype = v.type != WT_CONFIG_ITEM_NUM;
        } else if (strcmp(check->type, "list") == 0) {
            badtype = v.len > 0 && v.type != WT_CONFIG_ITEM_STRUCT;
        } else if (strcmp(check->type, "string") == 0) {
            badtype = false;
        } else
            WT_RET_MSG(session, EINVAL, "unknown configuration type: '%s'", check->type);

        if (badtype)
            WT_RET_MSG(session, EINVAL, "Invalid value for key '%.*s': expected a %s", (int)k.len,
              k.str, check->type);

        if (check->checkf != NULL)
            WT_RET(check->checkf(session, &v));

        /* If the checks string is empty, there are no additional checks we need to make. */
        if (check->checks == NULL)
            continue;

        /* The checks string itself is not needed for checking. */
        if (v.val < check->min_value)
            WT_RET_MSG(session, EINVAL, "Value too small for key '%.*s' the minimum is %" PRIi64,
              (int)k.len, k.str, check->min_value);

        if (v.val > check->max_value)
            WT_RET_MSG(session, EINVAL, "Value too large for key '%.*s' the maximum is %" PRIi64,
              (int)k.len, k.str, check->max_value);

        if ((choices = check->choices) != NULL) {
            if (v.len == 0)
                WT_RET_MSG(session, EINVAL, "Key '%.*s' requires a value", (int)k.len, k.str);
            if (v.type == WT_CONFIG_ITEM_STRUCT) {
                /*
                 * Handle the 'verbose' case of a list containing restricted choices.
                 */
                __wt_config_subinit(session, &sparser, &v);
                found = true;
                while (found && (ret = __wt_config_next(&sparser, &v, &dummy)) == 0)
                    found = __config_get_choice(choices, &v);
            } else
                found = __config_get_choice(choices, &v);

            if (!found)
                WT_RET_MSG(session, EINVAL, "Value '%.*s' not a permitted choice for key '%.*s'",
                  (int)v.len, v.str, (int)k.len, k.str);
        }
    }

    if (ret == WT_NOTFOUND)
        ret = 0;

    return (ret);
}
