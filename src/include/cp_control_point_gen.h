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

#ifdef HAVE_CONTROL_POINTS

/*
 * The name below is for a per connection control point named "Example control point".
 *
 * Each per connection control point has:
 * - Per connection control point ID (Could be generated):
 * WT_CONN_CONTROL_POINT_ID_EXAMPLE_CONTROL_POINT.
 *
 * The name below is for a per session control point named "Example control point2".
 *
 * Each per session control point has:
 * - Per session control point ID (Could be generated):
 * WT_CONN_CONTROL_POINT_ID_EXAMPLE_CONTROL_POINT2.
 */

#include "control_points.h"

/*
 * Per connection control point IDs.
 */
#define WT_CONN_CONTROL_POINT_ID_MainStartPrinting ((WT_CONTROL_POINT_ID)0)
#define WT_CONN_CONTROL_POINT_ID_THREAD0 ((WT_CONTROL_POINT_ID)1)
#define WT_CONN_CONTROL_POINT_ID_THREAD1 ((WT_CONTROL_POINT_ID)2)
#define WT_CONN_CONTROL_POINT_ID_THREAD2 ((WT_CONTROL_POINT_ID)3)
#define WT_CONN_CONTROL_POINT_ID_THREAD3 ((WT_CONTROL_POINT_ID)4)
#define WT_CONN_CONTROL_POINT_ID_THREAD4 ((WT_CONTROL_POINT_ID)5)
#define WT_CONN_CONTROL_POINT_ID_THREAD5 ((WT_CONTROL_POINT_ID)6)
#define WT_CONN_CONTROL_POINT_ID_THREAD6 ((WT_CONTROL_POINT_ID)7)
#define WT_CONN_CONTROL_POINT_ID_THREAD7 ((WT_CONTROL_POINT_ID)8)
#define WT_CONN_CONTROL_POINT_ID_THREAD8 ((WT_CONTROL_POINT_ID)9)
#define WT_CONN_CONTROL_POINT_ID_THREAD9 ((WT_CONTROL_POINT_ID)10)

/* The number of per connection control points (Could be generated). */
#define CONNECTION_CONTROL_POINTS_SIZE 11

/*
 * Per session control point IDs.
 */
#if 0 /* For example */
#define WT_SESSION_CONTROL_POINT_ID_EXAMPLE2 ((WT_CONTROL_POINT_ID)0)
#endif

/* The number of per session control points (Could be generated). */
#define SESSION_CONTROL_POINTS_SIZE 0

#endif /* HAVE_CONTROL_POINTS */
