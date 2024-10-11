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

static int
construct_configuration_control_point_string(WT_SESSION_IMPL *session, const char *cp_config_name,
  bool control_point_for_connection, char **buf)
{
    size_t len = strlen("per_connection_control_points") + strlen(cp_config_name) + 10;
    WT_RET(__wt_calloc_def(session, len, buf));
    WT_RET(__wt_snprintf(*buf, len, "%s.%s",
      control_point_for_connection ? "per_connection_control_points" : "per_session_control_points",
      cp_config_name));
    return (0);
}

/* Pair init function. */
/* Pair Predicate and "Sleep". */

/*
 * __wt_control_point_pair_init_always_sleep --
 *     The pair initialization function for predicate "Always" and action "Sleep".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */

WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_pred_sleep(WT_SESSION_IMPL *session, const char *cp_config_name,
  bool control_point_for_connection,
  wt_control_point_init_pred_t __F(init_pred), const char **cfg)
{
    WT_CONFIG_ITEM cval;
    struct __wt_control_point_pair_data_sleep *init_data;
    WT_DECL_RET;
    char *config_str = NULL;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(construct_configuration_control_point_string(
      session, cp_config_name, control_point_for_connection, &config_str));
    WT_ERR(__wt_config_gets(session, cfg, config_str, &cval));
    WT_ERR(__wt_control_point_config_action_sleep(session, init_data, &cval));
    if (init_pred != NULL)
        WT_ERR(init_pred(session, (WT_CONTROL_POINT_DATA *)init_data, &cval));

err:
    if (ret != 0)
        __wt_free(session, init_data);
    __wt_free(session, config_str);
    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair Predicate and "ERR". */
/*
 * __wt_control_point_pair_init_always_err --
 *     The pair initialization function for predicate "Always" and action "ERR".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_pred_err(WT_SESSION_IMPL *session, const char *cp_config_name,
  bool control_point_for_connection,
  wt_control_point_init_pred_t __F(init_pred), const char **cfg)
{
    WT_CONFIG_ITEM cval;
    struct __wt_control_point_pair_data_err *init_data;
    WT_DECL_RET;
    char *config_str = NULL;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(construct_configuration_control_point_string(
      session, cp_config_name, control_point_for_connection, &config_str));
    WT_ERR(__wt_config_gets(session, cfg, config_str, &cval));
    WT_ERR(__wt_control_point_config_action_err(session, init_data, &cval));
    if (init_pred != NULL)
        WT_ERR(init_pred(session, (WT_CONTROL_POINT_DATA *)init_data, &cval));

err:
    if (ret != 0)
        __wt_free(session, init_data);

    __wt_free(session, config_str);
    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair Predicate and "RET". */
/*
 * __wt_control_point_pair_init_always_ret --
 *     The pair initialization function for predicate function and action "RET".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_pred_ret(WT_SESSION_IMPL *session, const char *cp_config_name,
  bool control_point_for_connection,
  wt_control_point_init_pred_t __F(init_pred), const char **cfg)
{
    WT_CONFIG_ITEM cval;
    struct __wt_control_point_pair_data_ret *init_data;
    WT_DECL_RET;
    char *config_str = NULL;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(construct_configuration_control_point_string(
      session, cp_config_name, control_point_for_connection, &config_str));
    WT_ERR(__wt_config_gets(session, cfg, config_str, &cval));
    WT_ERR(__wt_control_point_config_action_ret(session, init_data, &cval));
    if (init_pred != NULL)
        WT_ERR(init_pred(session, (WT_CONTROL_POINT_DATA *)init_data, &cval));
err:
    if (ret != 0)
        __wt_free(session, init_data);
    __wt_free(session, config_str);
    return ((WT_CONTROL_POINT_DATA *)init_data);
}

/* Pair Predicate and "Wait for trigger". */
/*
 * __wt_control_point_pair_init_always_wait_for_trigger --
 *     The pair initialization function for predicate function and action "Wait for trigger".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cp_config_name The control point's configuration name. @param cfg
 *     Configuration string.
 */
WT_CONTROL_POINT_DATA *
__wt_control_point_pair_init_pred_wait_for_trigger(WT_SESSION_IMPL *session,
  const char *cp_config_name, bool control_point_for_connection,
  wt_control_point_init_pred_t __F(init_pred), const char **cfg)
{
    WT_CONFIG_ITEM cval;
    struct __wt_control_point_pair_data_wait_for_trigger *init_data;
    WT_DECL_RET;
    char *config_str = NULL;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(construct_configuration_control_point_string(
      session, cp_config_name, control_point_for_connection, &config_str));
    WT_ERR(__wt_config_gets(session, cfg, config_str, &cval));
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(session, init_data, &cval));
    if (init_pred != NULL)
        WT_ERR(init_pred(session, (WT_CONTROL_POINT_DATA *)init_data, &cval));

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(session, cp_config_name, init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);
    __wt_free(session, config_str);
    return ((WT_CONTROL_POINT_DATA *)init_data);
}
#endif /* HAVE_CONTROL_POINT */
