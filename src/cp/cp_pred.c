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
/*
 * __wt_control_point_pred_skip --
 *     Control point predicate function for "Skip: Skip the first skip-count control point
 *     crossings".
 *
 * @param session The session. @param data The control point's predicate data is in here.
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
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **cfg)
{
    /* TODO. */
    return (-1); /* For compiler */
}

/*
 * __wt_control_point_pred_times --
 *     Control point predicate function for "Times: Enable only the first enable-count control point
 *     crossings".
 *
 * @param session The session. @param data The control point's predicate data is in here.
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

/*
 * __wt_control_point_config_pred_times --
 *     Configuration parsing for control point predicate "Times: Enable only the first enable-count
 *     control point crossings".
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int __wt_control_point_config_pred_times)(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data,
    const char **cfg)
{
    /* TODO. */
    return (-1); /* For compiler */
}

/*
 * __wt_control_point_pred_random_param1 --
 *     Control point predicate function for "Random_param1: Trigger with probability". Probability
 *     is assigned to param1.value16aa.
 *
 * @param session The session. @param data The control point's predicate data is in here.
 */
bool
__wt_control_point_pred_random_param1(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data)
{
    /* data->param1.value16aa is probability. */
    if (__wt_random(&session->rnd) % 100 <= data->param1.value16aa)
        return (true);
    return (false);
}

/*
 * __wt_control_point_config_pred_random_param1 --
 *     Configuration parsing for control point predicate "Random_param1: Trigger with probability".
 *     Random is assigned to param1.value16aa.
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_pred_random_param1(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **cfg)
{
    /* TODO. */
    return (-1); /* For compiler */
}

/*
 * __wt_control_point_pred_random_param2 --
 *     Control point predicate function for "Random_param2: Trigger with probability". Probability
 *     is assigned to param2.value16aa.
 *
 * @param session The session. @param data The control point's predicate data is in here.
 */
bool
__wt_control_point_pred_random_param2(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data)
{
    /* data->param2.value16aa is probability. */
    if (__wt_random(&session->rnd) % 100 <= data->param2.value16aa)
        return (true);
    return (false);
}

/*
 * __wt_control_point_config_random_param2 --
 *     Configuration parsing for control point predicate "Random_param2: Trigger with probability".
 *     Probability is assigned to param2.value16aa.
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_random_param2(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **cfg)
{
    /* TODO. */
    return (-1); /* For compiler */
}

#endif /* HAVE_CONTROL_POINTS */
