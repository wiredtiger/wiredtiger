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
 * example from predicate "times" and action "Trigger".
 *
 * Each pair has:
 * - Pair init function (Could be generated).
 */
#include "wt_internal.h"

/* See comment in cp_action.c:__wt_verbose levels used in WT_VERB_CONTROL_POINT logging. */

#ifdef HAVE_CONTROL_POINT
/*
 * __construct_configuration_control_point_string --
 *     Construct the configuration string for the control point.
 *
 * @param session The session. @param cp_config_name The control point's configuration name. @param
 *     cfg Configuration string.
 */
static int
__construct_configuration_control_point_string(
  WT_SESSION_IMPL *session, const char *cp_config_name, char **buf)
{
    size_t len;

    len = strlen("per_connection_control_points") + strlen(cp_config_name) + 10;

    WT_RET(__wt_calloc_def(session, len, buf));
    WT_RET(__wt_snprintf(*buf, len, "per_connection_control_points.%s", cp_config_name));
    return (0);
}

/* Pair predicate function and "Sleep". */
/*
 * __wt_control_point_pair_init_pred_sleep --
 *     The pair initialization function for predicate function and action "Sleep".
 *
 * @param session The session. @param cp_config_name The control point's configuration name. @param
 *     init_pred Predicate initialization function. @param cfg Configuration string.
 */
int
__wt_control_point_pair_init_pred_sleep(WT_SESSION_IMPL *session, const char *cp_config_name,
  wt_control_point_init_pred_t __F(init_pred), const char **cfg, WT_CONTROL_POINT_DATA **cp_datap)
{
    struct __wt_control_point_pair_data_sleep *init_data;
    WT_CONFIG_ITEM cval;
    WT_DECL_RET;
    char *config_str;

    WT_RET(__wt_calloc_one(session, &init_data));

    /* Initialize configuration parameters. */
    WT_ERR(__construct_configuration_control_point_string(session, cp_config_name, &config_str));
    WT_ERR(__wt_config_gets(session, cfg, config_str, &cval));
    WT_ERR(__wt_control_point_config_action_sleep(session, init_data, &cval));
    if (init_pred != NULL)
        WT_ERR(init_pred(session, (WT_CONTROL_POINT_DATA *)init_data, &cval));
    *cp_datap = (WT_CONTROL_POINT_DATA *)init_data;

    if (0) {
err:
        __wt_free(session, init_data);
    }
    __wt_free(session, config_str);
    return (ret);
}

/* Pair predicate function and "Trigger". */
/*
 * __wt_control_point_pair_init_pred_trigger --
 *     The pair initialization function for predicate function and action "Wait for trigger".
 *
 * @param session The session. @param cp_config_name The control point's configuration name. @param
 *     init_pred Predicate initialization function. @param cfg Configuration string.
 */
int
__wt_control_point_pair_init_pred_trigger(WT_SESSION_IMPL *session, const char *cp_config_name,
  wt_control_point_init_pred_t __F(init_pred), const char **cfg, WT_CONTROL_POINT_DATA **cp_datap)
{
    struct __wt_control_point_pair_data_trigger *init_data;
    WT_CONFIG_ITEM cval;
    WT_DECL_RET;
    char *config_str;

    WT_ERR(__wt_calloc_one(session, &init_data));

    /* Initialize configuration parameters. */
    WT_ERR(__construct_configuration_control_point_string(session, cp_config_name, &config_str));
    WT_ERR(__wt_config_gets(session, cfg, config_str, &cval));
    WT_ERR(__wt_control_point_config_action_trigger(session, init_data, &cval));
    if (init_pred != NULL)
        WT_ERR(init_pred(session, (WT_CONTROL_POINT_DATA *)init_data, &cval));

    /* Extra initialization required for action "Trigger". */
    __wt_control_point_action_init_trigger(session, cp_config_name, init_data);
    *cp_datap = (WT_CONTROL_POINT_DATA *)init_data;

    if (0) {
err:
        __wt_free(session, init_data);
    }
    __wt_free(session, config_str);
    return (ret);
}
#endif /* HAVE_CONTROL_POINT */
