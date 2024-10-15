/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* cp_api.c: Definitions for the control point API. */

#include "wt_internal.h"

#ifdef HAVE_CONTROL_POINT
/*
 * Lock/unlock functions used by per-connection control points.
 */
/*
 * __wti_control_point_get_data --
 *     Get cp_registry->cp_data safe from frees.
 *
 * @param session The session. @param cp_registry The control point registry. @param locked True if
 *     cp_registry->lock is left locked for additional processing along with incrementing the
 *     ref_count.
 */
WT_CONTROL_POINT_DATA *
__wti_control_point_get_data(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, bool locked)
{
    WT_CONTROL_POINT_DATA *saved_cp_data;

    __wt_spin_lock(session, &cp_registry->lock);
    saved_cp_data = cp_registry->cp_data;
    if (saved_cp_data != NULL)
        __wt_atomic_add32(&saved_cp_data->ref_count, 1);
    if (!locked)
        __wt_spin_unlock(session, &cp_registry->lock);
    return (saved_cp_data);
}

/*
 * __wt_control_point_unlock --
 *     Unlock after additional processing.
 *
 * This is called after finishing the additional processing started with
 *     __wti_control_point_get_data() with locked=true.
 *
 * @param session The session. @param cp_registry The control point registry.
 */
void
__wt_control_point_unlock(WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry)
{
    __wt_spin_unlock(session, &cp_registry->lock);
}

/*
 * __wt_control_point_release_data --
 *     Call when done using WT_CONTROL_POINT_REGISTRY->cp_data that was returned by
 *     __wti_control_point_get_data.
 *
 * @param session The session. @param cp_registry The control point registry. @param locked True if
 *     the control point data is already locked.
 */
void
__wt_control_point_release_data(WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry,
  WT_CONTROL_POINT_DATA *cp_data, bool locked)
{
    uint32_t new_ref;

    if (WT_UNLIKELY(cp_data == NULL))
        return;
    if (!locked)
        __wt_spin_lock(session, &cp_registry->lock);
    new_ref = __wt_atomic_sub32(&cp_registry->cp_data->ref_count, 1);
    if ((new_ref == 0) && (cp_registry->cp_data != cp_data))
        __wt_free(session, cp_data);
    __wt_spin_unlock(session, &cp_registry->lock);
}

/*
 * Get functions used to implement the API.
 */
/*
 * __wti_conn_control_point_get_registry --
 *     Get the control point registry of a per-connection control point.
 */
int
__wti_conn_control_point_get_registry(
  WT_CONNECTION_IMPL *conn, wt_control_point_id_t id, WT_CONTROL_POINT_REGISTRY **cp_registryp)
{
    WT_DECL_RET;
#if CONNECTION_CONTROL_POINTS_SIZE == 0
    WT_ERR(EINVAL);
    WT_UNUSED(conn);
    WT_UNUSED(id);
    WT_UNUSED(cp_registryp);
#else
    if (WT_UNLIKELY(id >= CONNECTION_CONTROL_POINTS_SIZE))
        WT_ERR(EINVAL);
    if (WT_UNLIKELY(F_ISSET(conn, WT_CONN_SHUTTING_DOWN)))
        WT_ERR(WT_SHUTTING_DOWN);
    if (conn->control_points == NULL)
        WT_ERR(WT_CP_DISABLED);
    *cp_registryp = &(conn->control_points[id]);
#endif
err:
    return (ret);
}

/*
 * __wti_session_control_point_get_registry --
 *     Get the control point registry of a per-session control point.
 */
int
__wti_session_control_point_get_registry(
  WT_SESSION_IMPL *session, wt_control_point_id_t id, WT_CONTROL_POINT_REGISTRY **cp_registryp)
{
    WT_DECL_RET;
#if SESSION_CONTROL_POINTS_SIZE == 0
    WT_ERR(EINVAL);
    WT_UNUSED(session);
    WT_UNUSED(id);
    WT_UNUSED(cp_registryp);
#else
    if (WT_UNLIKELY(id >= SESSION_CONTROL_POINTS_SIZE))
        WT_ERR(EINVAL);
    if (WT_UNLIKELY(F_ISSET(session, WT_SESSION_SHUTTING_DOWN)))
        WT_ERR(WT_SHUTTING_DOWN);

    /* Lazy initialization. */
    if (session->control_points == NULL) {
        /* Initialize and optionally enable per session control points */
        WT_ERR(__wt_session_control_point_init_all(session));
        WT_ERR(__wt_session_control_point_enable_all_in_open(session));
    }

    *cp_registryp = &(session->control_points[id]);
#endif
err:
    return (ret);
}

/*
 * __conn_control_point_get_data --
 *     Get the control point data of a per-connection control point.
 */
static int
__conn_control_point_get_data(
  WT_CONNECTION_IMPL *conn, wt_control_point_id_t id, WT_CONTROL_POINT_DATA **cp_datap)
{
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;

    WT_ERR(__wti_conn_control_point_get_registry(conn, id, &cp_registry));
    *cp_datap = cp_registry->cp_data;
err:
    return (ret);
}

/*
 * __session_control_point_get_data --
 *     Get the control point registry of a per-session control point.
 */
static int
__session_control_point_get_data(
  WT_SESSION_IMPL *session, wt_control_point_id_t id, WT_CONTROL_POINT_DATA **cp_datap)
{
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;

    WT_ERR(__wti_session_control_point_get_registry(session, id, &cp_registry));
    *cp_datap = cp_registry->cp_data;
err:
    return (ret);
}

/*
 * API: Get from WT_CONTROL_POINT_REGISTRY.
 */
/*
 * __wt_conn_control_point_get_crossing_count --
 *     Get the crossing count of a per-connection control point.
 */
int
__wt_conn_control_point_get_crossing_count(
  WT_CONNECTION *wt_conn, wt_control_point_id_t id, size_t *crossing_countp)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;

    conn = (WT_CONNECTION_IMPL *)wt_conn;
    WT_ERR(__wti_conn_control_point_get_registry(conn, id, &cp_registry));
    *crossing_countp = cp_registry->crossing_count;
err:
    return (ret);
}

/*
 * __wt_session_control_point_get_crossing_count --
 *     Get the crossing count of a per-session control point.
 */
int
__wt_session_control_point_get_crossing_count(
  WT_SESSION *wt_session, wt_control_point_id_t id, size_t *crossing_countp)
{
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    WT_ERR(__wti_session_control_point_get_registry(session, id, &cp_registry));
    *crossing_countp = cp_registry->crossing_count;
err:
    return (ret);
}

/*
 * __wt_conn_control_point_get_trigger_count --
 *     Get the trigger count of a per-connection control point.
 */
int
__wt_conn_control_point_get_trigger_count(
  WT_CONNECTION *wt_conn, wt_control_point_id_t id, size_t *trigger_countp)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;

    conn = (WT_CONNECTION_IMPL *)wt_conn;
    WT_ERR(__wti_conn_control_point_get_registry(conn, id, &cp_registry));
    *trigger_countp = cp_registry->trigger_count;
err:
    return (ret);
}

/*
 * __wt_session_control_point_get_trigger_count --
 *     Get the trigger count of a per-session control point.
 */
int
__wt_session_control_point_get_trigger_count(
  WT_SESSION *wt_session, wt_control_point_id_t id, size_t *trigger_countp)
{
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    WT_ERR(__wti_session_control_point_get_registry(session, id, &cp_registry));
    *trigger_countp = cp_registry->trigger_count;
err:
    return (ret);
}

/*
 * API: Get from WT_CONTROL_POINT_DATA and set in WT_CONTROL_POINT_DATA.
 */
/*
 * __wt_conn_control_point_is_enabled --
 *     Get whether a per-connection control point is enabled.
 */
int
__wt_conn_control_point_is_enabled(
  WT_CONNECTION *wt_conn, wt_control_point_id_t id, bool *is_enabledp)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;

    conn = (WT_CONNECTION_IMPL *)wt_conn;
    WT_ERR(__conn_control_point_get_data(conn, id, &cp_data));
    *is_enabledp = (cp_data != NULL);
err:
    return (ret);
}

/*
 * __wt_session_control_point_is_enabled --
 *     Get whether a per-session control point is enabled.
 */
int
__wt_session_control_point_is_enabled(
  WT_SESSION *wt_session, wt_control_point_id_t id, bool *is_enabledp)
{
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    WT_ERR(__session_control_point_get_data(session, id, &cp_data));
    *is_enabledp = (cp_data != NULL);
err:
    return (ret);
}

/*
 * __wt_conn_control_point_get_param1 --
 *     Get param1 of a per-connection control point with predicate "Param 64 match".
 */
int
__wt_conn_control_point_get_param1(
  WT_CONNECTION *wt_conn, wt_control_point_id_t id, WT_CONTROL_POINT_PARAM *param1p)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;

    conn = (WT_CONNECTION_IMPL *)wt_conn;
    WT_ERR(__conn_control_point_get_data(conn, id, &cp_data));
    *param1p = cp_data->param1;
err:
    return (ret);
}

/*
 * __wt_session_control_point_get_param1 --
 *     Get param1 of a per-session control point with predicate "Param 64 match".
 */
int
__wt_session_control_point_get_param1(
  WT_SESSION *wt_session, wt_control_point_id_t id, WT_CONTROL_POINT_PARAM *param1p)
{
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    WT_ERR(__session_control_point_get_data(session, id, &cp_data));
    *param1p = cp_data->param1;
err:
    return (ret);
}

/*
 * __wt_conn_control_point_get_param2 --
 *     Get param2 of a per-connection control point with predicate "Param 64 match".
 */
int
__wt_conn_control_point_get_param2(
  WT_CONNECTION *wt_conn, wt_control_point_id_t id, WT_CONTROL_POINT_PARAM *param2p)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;

    conn = (WT_CONNECTION_IMPL *)wt_conn;
    WT_ERR(__conn_control_point_get_data(conn, id, &cp_data));
    *param2p = cp_data->param2;
err:
    return (ret);
}

/*
 * __wt_session_control_point_get_param2 --
 *     Get param2 of a per-session control point with predicate "Param 64 match".
 */
int
__wt_session_control_point_get_param2(
  WT_SESSION *wt_session, wt_control_point_id_t id, WT_CONTROL_POINT_PARAM *param2p)
{
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    WT_ERR(__session_control_point_get_data(session, id, &cp_data));
    *param2p = cp_data->param2;
err:
    return (ret);
}

/*
 * __wt_conn_control_point_set_param1 --
 *     Set param1 of a per-connection control point with predicate "Param 64 match".
 *
 * Note, this is only for use with predicate "Param 64 match". The configuration strings are not
 *     changed. If WT_CONNECTION.disable_control_point() and WT_CONNECTION.enable_control_point()
 *     are called the change is lost.
 */
int
__wt_conn_control_point_set_param1(
  WT_CONNECTION *wt_conn, wt_control_point_id_t id, WT_CONTROL_POINT_PARAM param1)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;

    conn = (WT_CONNECTION_IMPL *)wt_conn;
    WT_ERR(__conn_control_point_get_data(conn, id, &cp_data));
    cp_data->param1 = param1;
err:
    return (ret);
}

/*
 * __wt_session_control_point_set_param1 --
 *     Set param1 of a per-session control point with predicate "Param 64 match".
 *
 * Note, this is only for use with predicate "Param 64 match". The configuration strings are not
 *     changed. If WT_SESSION.disable_control_point() and WT_SESSION.enable_control_point() are
 *     called the change is lost.
 */
int
__wt_session_control_point_set_param1(
  WT_SESSION *wt_session, wt_control_point_id_t id, WT_CONTROL_POINT_PARAM param1)
{
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    WT_ERR(__session_control_point_get_data(session, id, &cp_data));
    cp_data->param1 = param1;
err:
    return (ret);
}

/*
 * __wt_conn_control_point_set_param2 --
 *     Set param2 of a per-connection control point with predicate "Param 64 match".
 *
 * Note, this is only for use with predicate "Param 64 match". The configuration strings are not
 *     changed. If WT_CONNECTION.disable_control_point() and WT_CONNECTION.enable_control_point()
 *     are called the change is lost.
 */
int
__wt_conn_control_point_set_param2(
  WT_CONNECTION *wt_conn, wt_control_point_id_t id, WT_CONTROL_POINT_PARAM param2)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;

    conn = (WT_CONNECTION_IMPL *)wt_conn;
    WT_ERR(__conn_control_point_get_data(conn, id, &cp_data));
    cp_data->param2 = param2;
err:
    return (ret);
}

/*
 * __wt_session_control_point_set_param2 --
 *     Set param2 of a per-session control point with predicate "Param 64 match".
 *
 * Note, this is only for use with predicate "Param 64 match". The configuration strings are not
 *     changed. If WT_SESSION.disable_control_point() and WT_SESSION.enable_control_point() are
 *     called the change is lost.
 */
int
__wt_session_control_point_set_param2(
  WT_SESSION *wt_session, wt_control_point_id_t id, WT_CONTROL_POINT_PARAM param2)
{
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    WT_ERR(__session_control_point_get_data(session, id, &cp_data));
    cp_data->param2 = param2;
err:
    return (ret);
}

/*
 * API: Disable a per connection control point.
 */
/*
 * __conn_control_point_disable --
 *     Disable a per connection control point given a WT_CONTROL_POINT_REGISTRY.
 *
 * @param conn The connection. @param cp_registry The WT_CONTROL_POINT_REGISTRY of the per
 *     connection control point to disable.
 */
static int
__conn_control_point_disable(WT_CONNECTION_IMPL *conn, WT_CONTROL_POINT_REGISTRY *cp_registry)
{
    WT_CONTROL_POINT_DATA *saved_cp_data;
    WT_DECL_RET;
    WT_UNUSED(conn);

    __wt_spin_lock(NULL, &cp_registry->lock);
    saved_cp_data = cp_registry->cp_data;
    if (WT_UNLIKELY(saved_cp_data == NULL))
        /* Already disabled. */
        WT_ERR(WT_NOTFOUND);
    cp_registry->cp_data = NULL;
    if (__wt_atomic_loadv32(&saved_cp_data->ref_count) == 0)
        __wt_free(NULL, saved_cp_data);
#if 0 /* TODO. */
    else
        /* TODO: Add saved_cp_data to a queue of control point data waiting to be freed. */;
#endif
err:
    __wt_spin_unlock(NULL, &cp_registry->lock);
    return (ret);
}

/*
 * __wt_conn_control_point_disable --
 *     Disable a per connection control point.
 *
 * @param wt_conn The connection. @param id The ID of the per connection control point to disable.
 */
int
__wt_conn_control_point_disable(WT_CONNECTION *wt_conn, wt_control_point_id_t id)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;

    conn = (WT_CONNECTION_IMPL *)wt_conn;
    WT_ERR(__wti_conn_control_point_get_registry(conn, id, &cp_registry));
    ret = __conn_control_point_disable(conn, cp_registry);
err:
    return (ret);
}

/*
 * API: Disable a per session control point.
 */
/*
 * __session_control_point_disable --
 *     Disable a per session control point given a WT_CONTROL_POINT_REGISTRY.
 *
 * @param session The session. @param cp_registry The WT_CONTROL_POINT_REGISTRY of the per session
 *     control point to disable.
 */
static int
__session_control_point_disable(WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry)
{
    if (WT_UNLIKELY(cp_registry->cp_data == NULL))
        /* Already disabled. */
        WT_RET(WT_NOTFOUND);
    __wt_free(session, cp_registry->cp_data);
    return (0);
}

/*
 * __wt_session_control_point_disable --
 *     Disable a per session control point.
 *
 * @param wt_session The session. @param id The ID of the per session control point to disable.
 */
int
__wt_session_control_point_disable(WT_SESSION *wt_session, wt_control_point_id_t id)
{
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    WT_ERR(__wti_session_control_point_get_registry(session, id, &cp_registry));
    ret = __session_control_point_disable(session, cp_registry);
err:
    return (ret);
}

/*
 * API: Enable a per connection control point.
 */
/*
 * __wti_conn_control_point_enable --
 *     Enable a per connection control point given a WT_CONTROL_POINT_REGISTRY.
 *
 * @param conn The connection. @param cp_registry The registry of the per connection control point
 *     to enable. @param cfg The configuration strings.
 */
int
__wti_conn_control_point_enable(
  WT_CONNECTION_IMPL *conn, WT_CONTROL_POINT_REGISTRY *cp_registry, const char **cfg)
{
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;
    WT_UNUSED(conn);

    __wt_spin_lock(NULL, &cp_registry->lock);
    cp_data = cp_registry->cp_data;
    if (WT_UNLIKELY(cp_data != NULL))
        /* Already enabled. */
        WT_ERR(EEXIST);
    WT_ERR(cp_registry->init(NULL, cp_registry->config_name, cfg, &cp_data));
    cp_registry->cp_data = cp_data;
err:
    __wt_spin_unlock(NULL, &cp_registry->lock);
    return (ret);
}

/*
 * __wt_conn_control_point_enable --
 *     Enable a per connection control point.
 *
 * @param wt_conn The connection. @param id The ID of the per connection control point to enable.
 *     @param cfg The configuration string override.
 */
int
__wt_conn_control_point_enable(WT_CONNECTION *wt_conn, wt_control_point_id_t id, const char *cfg)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;
    const char *cfgs[3];

    conn = (WT_CONNECTION_IMPL *)wt_conn;
    WT_ERR(__wti_conn_control_point_get_registry(conn, id, &cp_registry));
    cfgs[0] = conn->cfg;
    cfgs[1] = cfg;
    cfgs[2] = NULL;
    ret = __wti_conn_control_point_enable(conn, cp_registry, cfgs);
err:
    return (ret);
}

/*
 * API: Enable a per session control point.
 */
/*
 * __wti_session_control_point_enable --
 *     Enable a per session control point given a WT_CONTROL_POINT_REGISTRY.
 *
 * @param session The session. @param cp_registry The registry of the per session control point to
 *     enable. @param cfg The configuration string override.
 */
int
__wti_session_control_point_enable(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, const char *cfg)
{
    WT_CONTROL_POINT_DATA *cp_data;
    WT_DECL_RET;
    const char *cfgs[2];

    cp_data = cp_registry->cp_data;
    if (WT_UNLIKELY(cp_data != NULL))
        /* Already enabled. */
        WT_ERR(EEXIST);
    cfgs[0] = cfg;
    cfgs[1] = NULL;
    WT_ERR(cp_registry->init(session, cp_registry->config_name, cfgs, &cp_data));
    cp_registry->cp_data = cp_data;
err:
    return (ret);
}

/*
 * __wt_session_control_point_enable --
 *     Enable a per session control point.
 *
 * @param wt_session The session. @param id The ID of the per session control point to enable.
 *     @param cfg The configuration string override.
 */
int
__wt_session_control_point_enable(WT_SESSION *wt_session, wt_control_point_id_t id, const char *cfg)
{
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)wt_session;
    WT_ERR(__wti_session_control_point_get_registry(session, id, &cp_registry));
    ret = __wti_session_control_point_enable(session, cp_registry, cfg);
err:
    return (ret);
}

/*
 * __wt_conn_control_point_shutdown --
 *     Shut down the per connection control points.
 *
 * @param conn The connection.
 */
int
__wt_conn_control_point_shutdown(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_REGISTRY *control_points;
    WT_DECL_RET;
    int one_ret;

    conn = S2C(session);
    control_points = conn->control_points;
    if (control_points == NULL)
        return (0);
    /* Stop new per connection control point operations. */
    F_SET(conn, WT_CONN_SHUTTING_DOWN);

    for (int idx = 0; idx < CONNECTION_CONTROL_POINTS_SIZE; ++idx) {
        if (control_points[idx].cp_data == NULL)
            continue;
        one_ret = __conn_control_point_disable(conn, &(control_points[idx]));
        if (one_ret != 0)
            ret = one_ret; /* Return the last error. */
    }
    /* TODO: Wait for all disable operations to finish. */
    return (ret);
}

/*
 * API: Per session control point shutdown.
 */
/*
 * __wt_session_control_point_shutdown --
 *     Shut down the per session control points.
 *
 * @param session The session.
 */
int
__wt_session_control_point_shutdown(WT_SESSION_IMPL *session)
{
    WT_CONTROL_POINT_REGISTRY *control_points;
    WT_DECL_RET;
    int one_ret;

    control_points = session->control_points;
    if (control_points == NULL)
        return (0);
    /* Stop new per session control point operations. */
    F_SET(session, WT_SESSION_SHUTTING_DOWN);

    for (int idx = 0; idx < SESSION_CONTROL_POINTS_SIZE; ++idx) {
        if (control_points[idx].cp_data == NULL)
            continue;
        one_ret = __session_control_point_disable(session, &(control_points[idx]));
        if (one_ret != 0)
            ret = one_ret; /* Return the last error. */
    }
    /* TODO: Wait for all disable operations to finish. */
    return (ret);
}

#endif /* HAVE_CONTROL_POINT */
