/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* cp_api.c: Definitions for the control point API. */

#include "wt_internal.h"

#ifdef HAVE_CONTROL_POINTS
/*!
 * __wt_control_point_get_data --
 * Get cp_registry->data safe from frees.
 *
 * @param session The session.
 * @param cp_registry The control point registry.
 * @param locked True if cp_registry->lock is left locked for additional processing along with
 * incrementing the ref_count.
 */
WT_CONTROL_POINT *
__wt_control_point_get_data(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, bool locked)
{
    volatile WT_CONTROL_POINT *saved_data;
    __wt_spin_lock(&cp_registry->lock);
    saved_data = cp_registry->data;
    if (saved_data != NULL)
        __wt_atomic_addu32(&saved_data->ref_count, 1);
    if (!locked)
        __wt_spin_unlock(&cp_registry->lock);
    return (saved_data);
}

/*!
 * Unlock after additional processing.
 * This is called after finishing the additional processing started with
 * __wt_control_point_get_data() with locked=true.
 *
 * @param cp_registry The control point registry.
 */
void
__wt_control_point_unlock(WT_CONTROL_POINT_REGISTRY *cp_registry)
{
    __wt_spin_unlock(&cp_registry->lock);
}

/*!
 * __wt_control_point_release_data --
 * Call when done using WT_CONTROL_POINT_REGISTRY->data that was returned by
 * __wt_control_point_get_data.
 *
 * @param session The session.
 * @param cp_registry The control point registry.
 * @param locked True if the control point data is already locked.
 */
void
__wt_control_point_release_data(WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry,
  WT_CONTROL_POINT *data, bool locked)
{
    if (WT_UNLIKELY(data == NULL))
        return;
    if (!locked)
        __wt_spin_lock(&cp_registry->lock);
    uint32_t new_ref = __wt_atomic_subu32(&cp_registry->data->ref_count, 1);
    if ((new_ref == 0) && (cp_registry->data != data))
        __wt_free(session, data);
    __wt_spin_unlock(&cp_registry->lock);
}

/*!
 * Disable a per connection control point.
 */
int
__wt_conn_control_point_disable(WT_SESSION_IMPL *session, WT_CONTROL_POINT_ID id)
{
    WT_CONNECTION *conn;
    WT_CONTROL_POINT *saved_data;
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;

    if (WT_UNLIKELY(id >= CONNECTION_CONTROL_POINTS_SIZE))
        return (EINVAL);
    conn = S2C(session);
   if (WT_UNLIKELY(F_ISSET(conn, WT_CONN_SHUTTING_DOWN))
      return (WT_ERROR);

   cp_registry = &(conn->control_points[id]);
    __wt_spin_lock(&cp_registry->lock);
    saved_data = cp_registry->data;
    if (WT_UNLIKELY(saved_data == NULL))
       /* Already disabled. */
       WT_ERR(WT_ENOTFOUND);
    cp_registry->data = NULL;
    if (__wt_atomic_loadu32(&saved_data->ref_count) == 0)
       __wt_free(session, saved_data);
#if 0 /* TODO. */
    else
       /* TODO: Add saved_data to a queue of control point data waiting to be freed. */
#endif
err:
   __wt_spin_unlock(&cp_registry->lock);
   return (ret);
}

/*!
 * Disable a per session control point.
 */
int
__wt_session_control_point_disable(WT_SESSION_IMPL *session, WT_CONTROL_POINT_ID id)
{
    WT_CONTROL_POINT_REGISTRY *cp_registry

      if (WT_UNLIKELY(id >= SESSION_CONTROL_POINTS_SIZE)) return (EINVAL);
    if (WT_UNLIKELY(F_ISSET(session, WT_SESSION_SHUTTING_DOWN))
       return (WT_ERROR);

    cp_registry = &(session->control_points[id]);
    if (WT_UNLIKELY(cp_registry->data == NULL))
       /* Already disabled. */
       WT_RET(WT_ENOTFOUND);
    __wt_free(session, cp_registry->data);
    return (0);
}

/*!
 * Enable a per connection control point.
 */
static int
__wt_conn_control_point_enable(WT_SESSION_IMPL *session, WT_CONTROL_POINT_ID id)
{
    WT_CONNECTION *conn;
    WT_CONTROL_POINT *data;
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;

    if (WT_UNLIKELY(id >= CONNECTION_CONTROL_POINTS_SIZE))
        return (EINVAL);
    conn = S2C(session);
   if (WT_UNLIKELY(F_ISSET(conn, WT_CONN_SHUTTING_DOWN))
      return (WT_ERROR);

    cp_registry = &(conn->control_points[id]);
    __wt_spin_lock(&cp_registry->lock);
    data = cp_registry->data;
    if (WT_UNLIKELY(data != NULL))
        /* Already enabled. */
        WT_ERR(EEXIST);
    data = cp_registry->init(session, cp_registry);
    if (WT_UNLIKELY(data == NULL))
       WT_ERR(WT_ERROR);
    cp_registry->data = data;
err:
    __wt_spin_unlock(&cp_registry->lock);
    return (ret);
}

/*!
 * Enable a per session control point.
 */
int
__wt_session_control_point_enable(WT_SESSION_IMPL *session, WT_CONTROL_POINT_ID id)
{
    WT_CONTROL_POINT *data;
    WT_CONTROL_POINT_REGISTRY *cp_registry;

    if (WT_UNLIKELY(id >= SESSION_CONTROL_POINTS_SIZE))
        return (EINVAL);
   if (WT_UNLIKELY(F_ISSET(session, WT_SESSION_SHUTTING_DOWN))
      return (WT_ERROR);

    cp_registry = &(session->control_points[id]);
    data = cp_registry->data;
    if (WT_UNLIKELY(data != NULL))
        /* Already enabled. */
        WT_ERR(EEXIST);
    data = cp_registry->init(session, cp_registry);
    if (WT_UNLIKELY(data == NULL))
       WT_RET(WT_ERROR);
    cp_registry->data = data;
    return (0);
}

void
__wt_conn_control_points_shutdown(WT_CONNECTION *conn)
{
    /* Stop new per connection control point operations. */
    WT_CONTROL_POINT_REGISTRY *control_points = conn->control_points;
    WT_SESSION_IMPL *session;
    if (control_points == NULL)
        return;
    F_SET(conn, WT_CONN_SHUTTING_DOWN);

    session = C2S(conn);
    for (int idx = 0; idx < CONNECTION_CONTROL_POINTS_SIZE; ++idx) {
        if (control_points[idx].data != NULL)
            __control_point_disable(session, &(control_points[idx]));
    }
    /* TODO: Wait for all disable operations to finish. */
}

void
__wt_session_control_points_shutdown(WT_SESSION_IMPL *session)
{
    /* Stop new per session control point operations. */
    WT_CONTROL_POINT_REGISTRY *control_points = session->control_points;
    if (control_points == NULL)
        return;
    F_SET(section, WT_SESSION_SHUTTING_DOWN);

    for (int idx = 0; idx < SESSION_CONTROL_POINTS_SIZE; ++idx) {
        if (control_points[idx].data != NULL)
            __control_point_disable(session, &(control_points[idx]));
    }
    /* TODO: Wait for all disable operations to finish. */
}

#endif /* HAVE_CONTROL_POINTS */
