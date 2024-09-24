/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* cp_pred.h: Declarations for control point predicates. */

#ifdef HAVE_CONTROL_POINTS
bool __wt_control_point_pred_skip(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data);
int __wt_control_point_config_pred_skip(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **cfg);

bool __wt_control_point_pred_times(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data);
int
__wt_control_point_config_pred_times(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data,
                                     const char **cfg);
bool __wt_control_point_pred_random_param1(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data);
int __wt_control_point_config_pred_random_param1(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **cfg);

bool __wt_control_point_pred_random_param2(WT_SESSION_IMPL *session, WT_CONTROL_POINT *data);
int __wt_control_point_config_random_param2(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **cfg);
#endif /* HAVE_CONTROL_POINTS */
