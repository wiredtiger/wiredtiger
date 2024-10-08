/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* cp_control_point.c: Definitions for control points. */
/* This file must be edited when a new control point is created. */

/*
 * Each per connection control point has:
 * - A choice of action (Must be manual).
 * - A choice of predicate (Must be manual).
 * - An entry in __wt_conn_control_point_init_all (Could be generated).
 *
 * Each per connection control point that is enabled at startup has:
 * - An entry in __wt_conn_control_point_enable_all (Could be generated).
 *
 * Each per session control point has:
 * - A choice of action (Must be manual).
 * - A choice of predicate (Must be manual).
 * - An entry in __wt_session_control_point_init_all (Could be generated).
 *
 * Each per session control point that is enabled at startup has:
 * - An entry in __wt_session_control_point_enable_all (Could be generated).
 */
#include "wt_internal.h"

/* See comment in cp_action.c:__wt_verbose levels used in WT_VERB_CONTROL_POINT logging. */

#ifdef HAVE_CONTROL_POINT
/*
 * Functions used at the trigger site.
 */
/*
 * __wt_conn_control_point_test_and_trigger --
 *     Test whether a per connection control point is triggered and do common trigger processing. If
 *     disabled or not triggered, return NULL. If triggered, return the control point data. When
 *     done with the data it must be released.
 *
 * @param session The session. @param id The per connection control point's ID.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_test_and_trigger(WT_SESSION_IMPL *session, wt_control_point_id_t id)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT *data;
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    size_t new_crossing_count;
    size_t new_trigger_count;
    bool triggered;

    __wt_verbose_debug4(session, WT_VERB_CONTROL_POINT, "Start: id=%" PRId32, id);
    if (WT_UNLIKELY(id >= CONNECTION_CONTROL_POINTS_SIZE)) {
        __wt_verbose_error(session, WT_VERB_CONTROL_POINT,
          "ERROR: id(%" PRId32 ") >= CONNECTION_CONTROL_POINTS_SIZE(%" PRId32 ")", id,
          CONNECTION_CONTROL_POINTS_SIZE);
        return (NULL);
    }
    conn = S2C(session);
    if (WT_UNLIKELY(conn->control_points == NULL)) {
        __wt_verbose_warning(
          session, WT_VERB_CONTROL_POINT, "control_points is NULL: id=%" PRId32, id);
        return (NULL);
    }
    cp_registry = &(conn->control_points[id]);

    data = __wti_control_point_get_data(session, cp_registry, false);
    if (data == NULL) {
        /* Disabled. */
        __wt_verbose_debug5(session, WT_VERB_CONTROL_POINT, "Is disabled: id=%" PRId32, id);
        return (NULL);
    }
    new_crossing_count = ++(cp_registry->crossing_count);
    triggered = cp_registry->pred ? cp_registry->pred(session, cp_registry, data) : true;
    if (triggered) {
        new_trigger_count = ++(cp_registry->trigger_count);
        __wt_verbose_debug1(session, WT_VERB_CONTROL_POINT,
          "Triggered: id=%" PRId32 ", crossing_count=%" PRIu64 ", trigger_count=%" PRIu64, id,
          (uint64_t)new_crossing_count, (uint64_t)new_trigger_count);
    } else {
        __wt_verbose_debug3(session, WT_VERB_CONTROL_POINT,
          "Not Triggered: id=%" PRId32 ", crossing_count=%" PRIu64 ", trigger_count=%" PRIu64, id,
          (uint64_t)new_crossing_count, (uint64_t)cp_registry->trigger_count);
        __wt_control_point_release_data(session, cp_registry, data, false);
        /* Not triggered. */
        data = NULL;
    }
    return (data);
}

/*
 * __wt_session_control_point_test_and_trigger --
 *     Test whether a per session control point is triggered and do common trigger processing. If
 *     disabled or not triggered, return NULL. If triggered, return the control point data. The data
 *     does not need to be released.
 *
 * @param session The session. @param id The per connection control point's ID.
 */
WT_CONTROL_POINT *
__wt_session_control_point_test_and_trigger(WT_SESSION_IMPL *session, wt_control_point_id_t id)
{
    WT_CONTROL_POINT *data;
    WT_CONTROL_POINT_REGISTRY *cp_registry;
    size_t new_crossing_count;
    size_t new_trigger_count;
    bool triggered;

    if (WT_UNLIKELY(id >= SESSION_CONTROL_POINTS_SIZE)) {
        __wt_verbose_error(session, WT_VERB_CONTROL_POINT,
          "ERROR: id(%" PRId32 ") >= SESSION_CONTROL_POINTS_SIZE(%" PRId32 ")", id,
          SESSION_CONTROL_POINTS_SIZE);
        return (NULL);
    }
    if (WT_UNLIKELY(session->control_points == NULL)) {
        __wt_verbose_warning(
          session, WT_VERB_CONTROL_POINT, "control_points is NULL: id=%" PRId32, id);
        return (NULL);
    }
    cp_registry = &(session->control_points[id]);

    data = cp_registry->data;
    if (data == NULL) {
        /* Disabled. */
        __wt_verbose_debug5(session, WT_VERB_CONTROL_POINT, "Is disabled: id=%" PRId32, id);
        return (NULL);
    }

    triggered = cp_registry->pred ? cp_registry->pred(session, cp_registry, data) : true;
    new_crossing_count = ++(cp_registry->crossing_count);
    if (triggered) {
        new_trigger_count = ++(cp_registry->trigger_count);
        __wt_verbose_debug1(session, WT_VERB_CONTROL_POINT,
          "Triggered: id=%" PRId32 ", crossing_count=%" PRIu64 ", trigger_count=%" PRIu64, id,
          (uint64_t)new_crossing_count, (uint64_t)new_trigger_count);
    } else {
        __wt_verbose_debug3(session, WT_VERB_CONTROL_POINT,
          "Not Triggered: id=%" PRId32 ", crossing_count=%" PRIu64 ", trigger_count=%" PRIu64, id,
          (uint64_t)new_crossing_count, (uint64_t)cp_registry->trigger_count);
        /* Not triggered. */
        data = NULL;
    }
    return (data);
}

/*
 * Control point startup functions: Initialization.
 */
/*
 * __wt_conn_control_point_init_all --
 *     Initialize all per connection control points. Note, one part of this function must be edited
 *     for each per connection control point.
 *
 * @param session The session.
 */
int
__wt_conn_control_point_init_all(WT_SESSION_IMPL *session)
{
    WT_CONTROL_POINT_REGISTRY *control_points;
    WT_DECL_RET;

    if (CONNECTION_CONTROL_POINTS_SIZE == 0)
        return (0);
    WT_RET(__wt_calloc_def(session, CONNECTION_CONTROL_POINTS_SIZE, &control_points));

    /*
     * This part must be edited. Repeat this for every per connection control point.
     */
    /* From examples/ex_control_points.c */
    control_points[WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING].pred = NULL; /* Always */
    control_points[WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING].config_name =
      "main_start_printing";
    WT_ERR(__wt_spin_init(session,
      &(control_points[WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING].lock), "Main Start Printing"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_0].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_0].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_0].lock), "Thread 0"));
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_0].config_name = "thread_0";
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_0].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_1].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_1].pred = NULL; /* Always */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_1].config_name = "thread_1";
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_1].lock), "Thread 1"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_1].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_2].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_2].pred = NULL; /* Always */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_2].config_name = "thread_2";
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_2].lock), "Thread 2"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_2].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_3].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_3].pred = NULL; /* Always */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_3].config_name = "thread_3";
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_3].lock), "Thread 3"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_3].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_4].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_4].pred = NULL; /* Always */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_4].config_name = "thread_4";
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_4].lock), "Thread 4"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_4].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_5].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_5].pred = NULL; /* Always */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_5].config_name = "thread_5";
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_5].lock), "Thread 5"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_5].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_6].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_6].pred = NULL; /* Always */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_6].config_name = "thread_6";
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_6].lock), "Thread 6"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_6].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_7].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_7].pred = NULL; /* Always */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_7].config_name = "thread_7";
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_7].lock), "Thread 7"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_7].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_8].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_8].pred = NULL; /* Always */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_8].config_name = "thread_8";
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_8].lock), "Thread 8"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_8].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_9].init =
      __wt_control_point_pair_init_always_wait_for_trigger;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_9].pred = NULL; /* Always */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_9].config_name = "thread_9";
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_9].lock), "Thread 9"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_9].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    /* After all repeats finish with this. */
    S2C(session)->control_points = control_points;

err:
    if (ret != 0)
        __wt_free(session, control_points);
    return (ret);
}

/*
 * __wt_session_control_point_init_all --
 *     Initialize all per session control points. Note, one part of this function must be edited for
 *     each per session control point.
 *
 * @param session The session.
 */
int
__wt_session_control_point_init_all(WT_SESSION_IMPL *session)
{
#if SESSION_CONTROL_POINTS_SIZE == 0
    WT_UNUSED(session);
    return (0);
#else
    WT_DECL_RET;
    WT_CONTROL_POINT_REGISTRY *control_points;

    if (SESSION_CONTROL_POINTS_SIZE == 0)
        return (0);
    WT_RET(__wt_calloc_def(session, SESSION_CONTROL_POINTS_SIZE, &control_points));

    /*
     * This part must be edited. Repeat this for every per session control point.
     */
#if 0 /* For example */
    control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2].init =
        __wt_control_point_pair_init_skip_wait_for_trigger;
    control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2].pred =
        __wt_control_point_pred_skip;
    control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2].config_name = "example2";
  WT_ERR(__wt_spin_init(session, &(control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2].lock)));
#endif

    /* After all repeats finish with this. */
    session->control_points = control_points;

err:
    if (ret != 0)
        __wt_free(session, control_points);
    return (ret);
#endif
}

/*
 * Control point startup functions: Enable at startup.
 */
/*
 * __wt_conn_control_point_enable_all --
 *     Enable per connection control points that start enabled. Note, one part of this function must
 *     be edited for each per connection control point that starts enabled.
 *
 * @param conn The connection. @param cfg The configuration strings.
 */
int
__wt_conn_control_point_enable_all(WT_CONNECTION_IMPL *conn, const char **cfg)
{
#if 1 /* If no per connection control points are enabled at the start. */
    WT_UNUSED(conn);
    WT_UNUSED(cfg);
#else
    WT_CONTROL_POINT_REGISTRY *control_points;

    if (CONNECTION_CONTROL_POINTS_SIZE == 0)
        return (0);
    control_points = conn->control_points;

    /*
     * This part must be edited. Repeat this for every per connection control point that starts
     * enabled.
     */
#if 0 /* For example. */
    WT_RET(__wti_conn_control_point_enable(
      conn, &(control_points[WT_CONN_CONTROL_POINT_ID_EXAMPLE1]), cfg));
#endif
#endif
    return (0);
}

/*
 * __wt_session_control_point_enable_all --
 *     Enable per session control points that start enabled. Note, one part of this function must be
 *     edited for each per session control point that starts enabled.
 *
 * @param session The session.
 */
int
__wt_session_control_point_enable_all(WT_SESSION_IMPL *session)
{
#if 1 /* If no per session control points are enabled at the start. */
    WT_UNUSED(session);
#else
    WT_CONTROL_POINT_REGISTRY *control_points;
    if (SESSION_CONTROL_POINTS_SIZE == 0)
        return (0);

    /* Lazy initialization. */
    control_points = session->control_points;
    if (control_points == NULL) {
        WT_RET(__wt_session_control_point_init_all(session));
        control_points = session->control_points;
    }

    /*
     * This part must be edited. Repeat this for every per session control point that starts
     * enabled.
     */
#if 0 /* For example. */
    WT_RET(__wti_session_control_point_enable(session,
        &(control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2]), ""));
#endif
#endif
    return (0);
}
#endif /* HAVE_CONTROL_POINT */
