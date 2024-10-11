/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* cp_pair_pred_action.c: Definitions for pairs of predicates and actions. */
/* This file must be edited when a new control point predicate or action is created. */

/*
 * The names of pairs are derived from the configuration names of the predicate and the action, for
 * example from predicate "times" and action "Wait for trigger".
 *
 * Each pair has:
 * - Pair init function (Could be generated).
 */
#include "wt_internal.h"

/* See comment in cp_action.c:__wt_verbose levels used in WT_VERB_CONTROL_POINT logging. */

#ifdef HAVE_CONTROL_POINT
/* Pair init function. */

/* Pair "Always" and "Sleep". */
/*
 * __wt_control_point_pair_init_always_sleep --
 *     The pair initialization function for predicate "Always" and action "Sleep".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_always_sleep(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_sleep *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_sleep(session, init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Always" and "ERR". */
/*
 * __wt_control_point_pair_init_always_err --
 *     The pair initialization function for predicate "Always" and action "ERR".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_always_err(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_err *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_err(session, init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Always" and "RET". */
/*
 * __wt_control_point_pair_init_always_ret --
 *     The pair initialization function for predicate "Always" and action "RET".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_always_ret(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_ret *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_ret(session, init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Always" and "Wait for trigger". */
/*
 * __wt_control_point_pair_init_always_wait_for_trigger --
 *     The pair initialization function for predicate "Always" and action "Wait for trigger".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_always_wait_for_trigger(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_wait_for_trigger *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(session, init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(session, cp_config_name, init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Skip" and "Sleep". */
/*
 * __wt_control_point_pair_init_skip_sleep --
 *     The pair initialization function for predicate "Skip" and action "Sleep".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_skip_sleep(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_sleep *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_sleep(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_skip(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Skip" and "ERR". */
/*
 * __wt_control_point_pair_init_skip_err --
 *     The pair initialization function for predicate "Skip" and action "ERR".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_skip_err(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_err *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_err(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_skip(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Skip" and "RET". */
/*
 * __wt_control_point_pair_init_skip_ret --
 *     The pair initialization function for predicate "Skip" and action "RET".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_skip_ret(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_ret *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_ret(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_skip(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Skip" and "Wait for trigger". */
/*
 * __wt_control_point_pair_init_skip_wait_for_trigger --
 *     The pair initialization function for predicate "Skip" and action "Wait for trigger".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_skip_wait_for_trigger(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_wait_for_trigger *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_skip(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(session, cp_config_name, init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Times" and "Sleep". */
/*
 * __wt_control_point_pair_init_times_sleep --
 *     The pair initialization function for predicate "Times" and action "Sleep".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_times_sleep(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_sleep *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_sleep(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_times(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Times" and "ERR". */
/*
 * __wt_control_point_pair_init_times_err --
 *     The pair initialization function for predicate "Times" and action "ERR".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_times_err(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_err *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_err(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_times(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Times" and "RET". */
/*
 * __wt_control_point_pair_init_times_ret --
 *     The pair initialization function for predicate "Times" and action "RET".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_times_ret(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_ret *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_ret(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_times(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Times" and "Wait for trigger". */
/*
 * __wt_control_point_pair_init_times_wait_for_trigger --
 *     The pair initialization function for predicate "Times" and action "Wait for trigger".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_times_wait_for_trigger(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_wait_for_trigger *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_times(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(session, cp_config_name, init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Random param1" and "Sleep". */
/*
 * __wt_control_point_pair_init_random_param1_sleep --
 *     The pair initialization function for predicate "Random param1" and action "Sleep".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_random_param1_sleep(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_sleep *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_sleep(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_random_param1(
      session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Random param1" and "ERR". */
/*
 * __wt_control_point_pair_init_random_param1_err --
 *     The pair initialization function for predicate "Random param1" and action "ERR".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_random_param1_err(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_err *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_err(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_random_param1(
      session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Random param1" and "RET". */
/*
 * __wt_control_point_pair_init_random_param1_ret --
 *     The pair initialization function for predicate "Random param1" and action "RET".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_random_param1_ret(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_ret *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_ret(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_random_param1(
      session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Random param1" and "Wait for trigger". */
/*
 * __wt_control_point_pair_init_random_param1_wait_for_trigger --
 *     The pair initialization function for predicate "Random param1" and action "Wait for trigger".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_random_param1_wait_for_trigger(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_wait_for_trigger *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_random_param1(
      session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(session, cp_config_name, init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Random param2" and "Sleep". */
/*
 * __wt_control_point_pair_init_random_param2_sleep --
 *     The pair initialization function for predicate "Random param2" and action "Sleep".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_random_param2_sleep(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_sleep *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_sleep(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_random_param2(
      session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Random param2" and "ERR". */
/*
 * __wt_control_point_pair_init_random_param2_err --
 *     The pair initialization function for predicate "Random param2" and action "ERR".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_random_param2_err(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_err *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_err(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_random_param2(
      session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Random param2" and "RET". */
/*
 * __wt_control_point_pair_init_random_param2_ret --
 *     The pair initialization function for predicate "Random param2" and action "RET".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_random_param2_ret(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_ret *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_ret(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_random_param2(
      session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Random param2" and "Wait for trigger". */
/*
 * __wt_control_point_pair_init_random_param2_wait_for_trigger --
 *     The pair initialization function for predicate "Random param2" and action "Wait for trigger".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_random_param2_wait_for_trigger(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_wait_for_trigger *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(session, init_data, cfg));
    WT_ERR(__wt_control_point_config_pred_random_param2(
      session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(session, cp_config_name, init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Param match" and "Sleep". */
/*
 * __wt_control_point_pair_init_param_match_sleep --
 *     The pair initialization function for predicate "Param match" and action "Sleep".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_param_match_sleep(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_sleep *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_sleep(session, init_data, cfg));
    WT_ERR(
      __wt_control_point_config_pred_param_match(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Param match" and "ERR". */
/*
 * __wt_control_point_pair_init_param_match_err --
 *     The pair initialization function for predicate "Param match" and action "ERR".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_param_match_err(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_err *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_err(session, init_data, cfg));
    WT_ERR(
      __wt_control_point_config_pred_param_match(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Param match" and "RET". */
/*
 * __wt_control_point_pair_init_param_match_ret --
 *     The pair initialization function for predicate "Param match" and action "RET".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_param_match_ret(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_ret *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_ret(session, init_data, cfg));
    WT_ERR(
      __wt_control_point_config_pred_param_match(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair "Param match" and "Wait for trigger". */
/*
 * __wt_control_point_pair_init_param_match_wait_for_trigger --
 *     The pair initialization function for predicate "Param match" and action "Wait for trigger".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_param_match_wait_for_trigger(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg)
{
    struct __wt_control_point_pair_data_wait_for_trigger *init_data;
    WT_DECL_RET;
    WT_UNUSED(cp_config_name);

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(session, init_data, cfg));
    WT_ERR(
      __wt_control_point_config_pred_param_match(session, (WT_CONTROL_POINT_DATA *)init_data, cfg));

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(session, cp_config_name, init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT_DATA *)init_data);
}

#endif /* HAVE_CONTROL_POINT */
