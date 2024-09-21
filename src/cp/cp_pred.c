/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* cp_pred.c: Definitions for control point predicates. */
/* This file must be edited when a new control point predicate is created. */

#include "wt_internal.h"

#ifdef HAVE_CONTROL_POINTS
/*!
 * Control point predicate function for "Skip the first skip-count control point crossings".
 */
bool
__wt_control_point_pred_skip(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data)
{
    /* data->param1.value 64 is skip-count. */
    if (data->param1.value 64 > 0) {
        --(data->param1.value 64);
        return (false);
    }
    return (true);
}

/*!
 * Configuration parsing for control point predicate "Skip the first skip-count control point
 * crossings".
 */
int
__wt_control_point_config_pred_skip(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **configuration)
{
    /* TODO. */
}

/*!
 * Control point predicate function for "Enable only the first enable-count control point
 * crossings".
 */
bool
__wt_control_point_pred_times(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data)
{
    /* data->param2.value64 is enable-count. */
    if (data->param2.value 64 > 0) {
        --(data->param2.value 64);
        return (true);
    }
    return (false);
}

/*!
 * Configuration parsing for control point predicate "Enable only the first enable-count control
 * point crossings".
 */
int control_point_config_pred_times)(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **configuration)
{
    /* TODO. */
}

/*!
 * Control point predicate function for "Trigger with probability".
 * Probability is assigned to param1.value16aa.
 */
bool
__wt_control_point_pred_probability_param1(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data)
{
    /* data->param1.value16aa is probability. */
    if (__wt_random(&session->rnd) % 100 <= data->param1.value16aa)
        return (true);
    return (false);
}

/*!
 * Configuration parsing for control point predicate "Trigger with probability".
 * Random is assigned to param1.value16aa.
 */
int
__wt_control_point_config_probability_param1(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **configuration)
{
    /* TODO. */
}

/*!
 * Control point predicate function for "Trigger with probability".
 * Probability is assigned to param2.value16aa.
 */
bool
control_point_pred_probability_param2(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data)
{
    /* data->param2.value16aa is probability. */
    if (__wt_random(&session->rnd) % 100 <= data->param2.value16aa)
        return (true);
    return (false);
}

/*!
 * Configuration parsing for control point predicate "Trigger with probability".
 * Probability is assigned to param2.value16aa.
 */
int
__wt_control_point_config_probability_param2(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **configuration)
{
    /* TODO. */
}

#endif /* HAVE_CONTROL_POINTS */
