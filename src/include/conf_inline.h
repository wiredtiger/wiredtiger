/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __wt_conf_get_compiled --
 *     Return true if and only if the given string is a dummy compiled string, and if so, return the
 *     compiled structure.
 */
static inline bool
__wt_conf_get_compiled(WT_CONNECTION_IMPL *conn, const char *config, WT_CONF **confp)
{
    if (config < conn->conf_dummy || config >= conn->conf_dummy + conn->conf_size)
        return (false);

    *confp = conn->conf_array[(uint32_t)(config - conn->conf_dummy)];
    return (true);
}

/*
 * __wt_conf_is_compiled --
 *     Return true if and only if the given string is a dummy compiled string.
 */
static inline bool
__wt_conf_is_compiled(WT_CONNECTION_IMPL *conn, const char *config)
{
    return (config >= conn->conf_dummy && config < conn->conf_dummy + conn->conf_size);
}

/*
 * __wt_conf_compile_choice --
 *     Check the string value against a list of choices, if it is found, set up the value so it can
 *     be checked against a particular choice quickly.
 */
static inline int
__wt_conf_compile_choice(
  WT_SESSION_IMPL *session, const char **choices, const char *str, size_t len, const char **result)
{
    const char *choice;

    if (choices == NULL)
        return (0);

    /*
     * Find the choice, and set the string in the value to the entry in the choice table. It's the
     * same string, but the address is known by an external identifier (e.g.
     * __WT_CONFIG_CHOICE_force). That way it can be checked without a string compare call by using
     * the WT_CONF_STRING_MATCH macro.
     */
    for (; (choice = *choices) != NULL; ++choices)
        if (WT_FAST_STRING_MATCH(choice, str, len)) {
            *result = choice;
            break;
        }

    if (choice == NULL) {
        /*
         * We didn't find it in the list of choices. It's legal to specify a choice as blank, we
         * have a special value to indicate that. We check this last because this is a rare case,
         * especially when we are binding a parameter, which is the fast path we optimize for.
         */
        if (len == 0)
            *result = __WT_CONFIG_CHOICE_NULL;
        else
            WT_RET_MSG(session, EINVAL, "Value '%.*s' is not a valid choice", (int)len, str);
    }
    return (0);
}
