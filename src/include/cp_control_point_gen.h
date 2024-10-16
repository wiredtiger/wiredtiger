/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* cp_control_point_gen.h: Generated declarations for control points. */
/* In the future this file will be generated. Until then this file must be edited when a new control
 * point is created. */

#ifdef HAVE_CONTROL_POINT

/*
 * The name below is for a per connection control point named "Example control point".
 *
 * Each per connection control point has:
 * - Per connection control point ID (Could be generated):
 * WT_CONN_CONTROL_POINT_ID_EXAMPLE_CONTROL_POINT.
 * - A choice of action (Must be manual).
 * - A choice of predicate (Must be manual).
 * - An entry in __wt_conn_control_point_init_all (Could be generated).
 *
 * Each per connection control point that is enabled at startup has:
 * - An entry in __wt_conn_control_point_enable_all_in_start (Could be generated).
 *
 * The name below is for a per session control point named "Example control point2".
 *
 * Each per session control point has:
 * - Per session control point ID (Could be generated):
 * WT_CONN_CONTROL_POINT_ID_EXAMPLE_CONTROL_POINT2.
 * - A choice of action (Must be manual).
 * - A choice of predicate (Must be manual).
 * - An entry in __wt_session_control_point_init_all (Could be generated).
 *
 * Each per session control point that is enabled at startup has:
 * - An entry in __wt_session_control_point_enable_all_in_open (Could be generated).
 */

#include "control_points.h"

/*
 * Per connection control point IDs.
 */
#define WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING ((wt_control_point_id_t)0)
#define WT_CONN_CONTROL_POINT_ID_THREAD_0 ((wt_control_point_id_t)1)
#define WT_CONN_CONTROL_POINT_ID_THREAD_1 ((wt_control_point_id_t)2)
#define WT_CONN_CONTROL_POINT_ID_THREAD_2 ((wt_control_point_id_t)3)
#define WT_CONN_CONTROL_POINT_ID_THREAD_3 ((wt_control_point_id_t)4)
#define WT_CONN_CONTROL_POINT_ID_THREAD_4 ((wt_control_point_id_t)5)

/* The number of per connection control points (Could be generated). */
#define CONNECTION_CONTROL_POINTS_SIZE 6

/*
 * Per session control point IDs.
 */
#define WT_SESSION_CONTROL_POINT_ID_THREAD_0 ((wt_control_point_id_t)0)

/* The number of per session control points (Could be generated). */
#define SESSION_CONTROL_POINTS_SIZE 1

#endif /* HAVE_CONTROL_POINT */
