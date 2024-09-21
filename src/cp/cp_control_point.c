/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* cp_control_point.c: Definitions for control points. */
/* This file must be edited when a new control point is created. */

#include "wt_internal.h"

#ifdef HAVE_CONTROL_POINTS

/*!
 * Test whether a per connection control point is triggered and do common trigger processing.
 * If disabled or not triggered, return NULL.
 * If triggered, return the control point data.
 * When done with the data it must be released.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_test_and_trigger(WT_SESSION_IMPL *session, WT_CONTROL_POINT_ID id)
{
    WT_CONNECTION *conn;
    bool triggered;
    WT_CONTROL_POINT *data;

    if (WT_UNLIKELY(id >= CONNECTION_CONTROL_POINTS_SIZE))
        return (NULL);
    conn = S2C(session);
    if (WT_UNLIKELY(conn->control_points == NULL))
        return (NULL);

    data = __wt_control_point_get_data(session, cp_registry, false);
    if (data == NULL)
        /* Disabled. */
        return (NULL);
    triggered = cp_registry->pred ? cp_registry->pred(session, data) : true;
    ++(cp_registry->crossing_count);
    if (triggered)
        ++(cp_registry->trigger_count);
    else {
        __wt_control_point_release_data(session, cp_registry, false);
        /* Not triggered. */
        data = NULL;
    }
    return (data);
}

/*!
 * Test whether a per session control point is triggered and do common trigger processing.
 * If disabled or not triggered, return NULL.
 * If triggered, return the control point data.
 * The data does not need to be released.
 */
WT_CONTROL_POINT *
__wt_session_control_point_test_and_trigger(WT_SESSION_IMPL *session, WT_CONTROL_POINT_ID id)
{
    bool triggered;
    WT_CONTROL_POINT *data;
   if (WT_UNLIKELY((id >= SESSION_CONTROL_POINTS_SIZE) ||
                   (session->control_points == NULL))
      return (NULL);

    data = cp_registry->data;
    if (data == NULL)
       /* Disabled. */
       return (NULL);

    triggered = cp_registry->pred ? cp_registry->pred(session, data) : true;
    ++(cp_registry->crossing_count);
    if (triggered)
        ++(cp_registry->trigger_count);
    else
        /* Not triggered. */
        data = NULL;
    return (data);
}

/*!
 * Initialize per connection control points.
 */
void
__wt_conn_control_points_init_all(WT_CONNECTION *conn)
{
    WT_SESSION_IMPL *session = C2S(conn);
    WT_CONTROL_POINT_REGISTRY *control_points;
    if (CONNECTION_CONTROL_POINTS_SIZE == 0)
        return;
    __wt_calloc_def(session, CONNECTION_CONTROL_POINTS_SIZE, &control_points);

    /* This part must be edited. Repeat this for every per connection control point. */
#if 0 /* For example */
  control_points[WT_CONN_CONTROL_POINT_ID_EXAMPLE1].init = __wt_control_point_conn_init_example1;
  control_points[WT_CONN_CONTROL_POINT_ID_EXAMPLE1].pred = __wt_control_point_conn_pred_example1;
  __wt_spin_init(session, &(control_points[WT_CONN_CONTROL_POINT_ID_EXAMPLE1].lock));
#endif

    /* After all repeats finish with this. */
    conn->control_points = control_points;
}

/*!
 * Initialize per session control points.
 */
void
__wt_session_control_points_init(WT_SESSION_IMPL *session)
{
    WT_CONTROL_POINT_REGISTRY *control_points;
    if (SESSION_CONTROL_POINTS_SIZE == 0)
        return;
    __wt_calloc_def(session, SESSION_CONTROL_POINTS_SIZE, &control_points);

    /* This part must be edited. Repeat this for every per session control point. */
#if 0 /* For example */
  control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2].init = __wt_control_point_session_init_example2;
  control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2].pred = __wt_control_point_session_pred_examples;
  __wt_spin_init(session, &(control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2].lock));
#endif

    /* After all repeats finish with this. */
    session->control_points = control_points;
}

/*!
 * Enable per connection control points that start enabled.
 */
void
__wt_conn_control_points_enable_all(WT_CONNECTION *conn)
{
    WT_SESSION_IMPL *session;
    WT_CONTROL_POINT_REGISTRY *control_points;
    if (CONNECTION_CONTROL_POINTS_SIZE == 0)
        return;
    session = C2S(conn);
    control_points = conn->control_points;

    /* This part must be edited. Repeat this for every per connection control point that starts
     * enabled. */
#if 0 /* For example */
  __wti_control_point_enable(session, &(control_points[WT_CONN_CONTROL_POINT_ID_EXAMPLE1]));
#endif
}

/*!
 * Enable per session control points that start enabled.
 */
void
__wt_session_control_points_enable_all(WT_SESSION_IMPL *session)
{
    WT_CONTROL_POINT_REGISTRY *control_points;
    if (SESSION_CONTROL_POINTS_SIZE == 0)
        return;
    control_points = session->control_points;

    /* This part must be edited. Repeat this for every per session control point that starts
     * enabled. */
#if 0 /* For example */
  __wti_control_point_enable(session, &(control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2]));
#endif
}

#endif /* HAVE_CONTROL_POINTS */
