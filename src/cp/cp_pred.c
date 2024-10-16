/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* cp_pred.c: Definitions for control point predicates. */
/* This file must be edited when a new control point predicate is created. */

/*
 * The names below are for a predicate named "Example predicate".
 *
 * Each predicate has:
 * - Predicate function (Must be manual): __wt_control_point_pred_example_predicate.
 * - Predicate config parsing function (Must be manual):
 * __wt_control_point_config_pred_example_predicate.
 * - An assignment of configuration parameters to values in WT_CONTROL_POINT_DATA (Must be manual).
 */

#include "wt_internal.h"

#ifdef HAVE_CONTROL_POINT
/*
 * Predicate: Skip: Skip the first skip-count control point.
 */
/* Predicate function. */
/*
 * __wt_control_point_pred_skip --
 *     Control point predicate function for "Skip: Skip the first skip-count control point
 *     crossings".
 *
 * @param session The session. @param cp_registry The control point registry. @param data The
 *     control point's predicate data is in here.
 */
bool
__wt_control_point_pred_skip(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, WT_CONTROL_POINT_DATA *data)
{
    WT_UNUSED(session);
    /* skip_count is assigned to WT_CONTROL_PARAM.param1.value64. */
    /* Note: crossing_count is incremented before calling this function. */
    return (cp_registry->crossing_count > data->param1.value64);
}

/* Predicate config parsing function. */
/*
 * __wt_control_point_config_pred_skip --
 *     Configuration parsing for control point predicate "Skip: Skip the first skip-count control
 *     point crossings".
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_pred_skip(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_DATA *data, WT_CONFIG_ITEM *item)
{
    WT_CONFIG_ITEM cval;
    WT_RET(__wt_config_subgets(session, item, "skip_count", &cval));
    data->param1.value64 = (uint64_t)cval.val;
    return (0);
}

/*
 * Predicate: Times: Enable only the first enable-count control point crossings.
 */
/* Predicate function. */
/*
 * __wt_control_point_pred_times --
 *     Control point predicate function for "Times: Enable only the first enable-count control point
 *     crossings".
 *
 * @param session The session. @param cp_registry The control point registry. @param data The
 *     control point's predicate data is in here.
 */
bool
__wt_control_point_pred_times(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, WT_CONTROL_POINT_DATA *data)
{
    /* TODO. Replace these hard wired values with control point predicate configuration parsing. */
    /* TODO. When the hard wire is removed, delete this function from func_ok() in dist/s_void. */
    WT_UNUSED(session);
    /* enable_count is assigned to WT_CONTROL_PARAM.param2.value64. */
    /* Note: trigger_count is incremented after calling this function. */
    return (cp_registry->trigger_count < data->param2.value64);
}

/* Predicate config parsing function. */
/*
 * __wt_control_point_config_pred_times --
 *     Configuration parsing for control point predicate "Times: Enable only the first enable-count
 *     control point crossings".
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_pred_times(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_DATA *data, WT_CONFIG_ITEM *item)
{
    WT_CONFIG_ITEM cval;
    /* enable_count is assigned to WT_CONTROL_PARAM.param2.value64. */
    WT_RET(__wt_config_subgets(session, item, "enable_count", &cval));
    data->param2.value64 = (uint64_t)cval.val;
    return (0);
}

/*
 * Predicate: Random_param1: Trigger with probability. Probability
 *     is assigned to param1.value16aa.
 */
/* Predicate function. */
/*
 * __wt_control_point_pred_random_param1 --
 *     Control point predicate function for "Random_param1: Trigger with probability". Probability
 *     is assigned to param1.value16aa.
 *
 * @param session The session. @param cp_registry The control point registry. @param data The
 *     control point's predicate data is in here.
 */
bool
__wt_control_point_pred_random_param1(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, WT_CONTROL_POINT_DATA *data)
{
    WT_UNUSED(cp_registry);
    /* probability is assigned to WT_CONTROL_PARAM.param1.value16aa. */
    if (__wt_random(&session->rnd) % 100 <= data->param1.value16aa)
        return (true);
    return (false);
}

/* Predicate config parsing function. */
/*
 * __wt_control_point_config_pred_random_param1 --
 *     Configuration parsing for control point predicate "Random_param1: Trigger with probability".
 *     Probability is assigned to param1.value16aa.
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_pred_random_param1(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_DATA *data, WT_CONFIG_ITEM *item)
{
    /* probability is assigned to WT_CONTROL_PARAM.param1.value16aa. */
    WT_RET(__wt_config_subgets(session, item, "probability", item));
    data->param1.value16aa = (uint16_t)item->val;
    return (0);
}

/*
 * Predicate: Random_param2: Trigger with probability. Probability
 *     is assigned to param2.value16aa.
 */
/* Predicate function. */
/*
 * __wt_control_point_pred_random_param2 --
 *     Control point predicate function for "Random_param2: Trigger with probability". Probability
 *     is assigned to param2.value16aa.
 *
 * @param session The session. @param cp_registry The control point registry. @param data The
 *     control point's predicate data is in here.
 */
bool
__wt_control_point_pred_random_param2(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, WT_CONTROL_POINT_DATA *data)
{
    WT_UNUSED(cp_registry);
    /* probability is assigned to WT_CONTROL_PARAM.param2.value16aa. */
    if (__wt_random(&session->rnd) % 100 <= data->param2.value16aa)
        return (true);
    return (false);
}

/* Predicate config parsing function. */
/*
 * __wt_control_point_config_pred_random_param2 --
 *     Configuration parsing for control point predicate "Random_param2: Trigger with probability".
 *     Probability is assigned to param2.value16aa.
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_pred_random_param2(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_DATA *data, WT_CONFIG_ITEM *item)
{
    WT_CONFIG_ITEM cval;

    /* probability is assigned to WT_CONTROL_PARAM.param2.value16aa. */
    WT_RET(__wt_config_subgets(session, item, "probability", &cval));
    data->param2.value16aa = (uint16_t)cval.val;

    return (0);
}

/*
 * Predicate: Param_match: Trigger if pointer matches. The match value is assigned to
 * param1.pointer.
 */
/* Predicate function. */
/*
 * __wt_control_point_pred_param_match --
 *     Control point predicate function for "Param match: Trigger if WT_CONTROL_DATA.param1 matches
 *     WT_CONTROL_DATA.param2". The match value is assigned to param1.value64 or .pointer. It should
 *     be set by the call site. The test value is assigned to param2.value64 or .pointer. It should
 *     be set by the trigger site.
 *
 * @param session The session. @param cp_registry The control point registry. @param data The
 *     control point's predicate data is in here.
 */
bool
__wt_control_point_pred_param_match(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, WT_CONTROL_POINT_DATA *data)
{
    WT_UNUSED(session);
    WT_UNUSED(cp_registry);
    /* The match value from the call site is assigned to WT_CONTROL_PARAM.param1.pointer. */
    /* The test value from the trigger site is assigned to WT_CONTROL_PARAM.param2.pointer. */
    return (data->param1.pointer == data->param2.pointer);
}

/* Predicate config parsing function. */
/*
 * __wt_control_point_config_pred_param_match --
 *     Configuration parsing for control point predicate "Param match: Trigger if param1 matches
 *     param2". The match value is assigned to param1.value64 or .pointer. It should be set by the
 *     call site. The test value is assigned to param2.value64 or .pointer. It should be set by the
 *     trigger site.
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_pred_param_match(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_DATA *data, WT_CONFIG_ITEM *item)
{
    WT_CONFIG_ITEM cval;

    /* probability is assigned to WT_CONTROL_PARAM.param2.value16aa. */
    WT_RET(__wt_config_subgets(session, item, "pointer", &cval));
    data->param1.pointer = (void *)cval.val;
    return (0);
}
#endif /* HAVE_CONTROL_POINT */
