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
 * The names below are for a per connection control point named "Example control point".
 *
 * Each per connection control point has:
 * - A choice of action (Must be manual).
 * - A choice of predicate (Must be manual).
 * - Per connection control point data type (Could be generated):
 * __wt_conn_control_point_data_example_control_point.
 * - Per connection control point init function (Could be generated):
 * __wt_conn_control_point_init_example_control_point.
 * - An entry in __wt_conn_control_point_init_all (Could be generated).
 *
 * Each per connection control point that is enabled at startup has:
 * - An entry in __wt_conn_control_point_enable_all (Could be generated).
 *
 * The names below are for a per session control point named "Example control point2".
 *
 * Each per session control point has:
 * - A choice of action (Must be manual).
 * - A choice of predicate (Must be manual).
 * - Per session control point data type (Could be generated):
 * __wt_session_control_point_data_example_control_point2.
 * - Per session control point init function (Could be generated):
 * __wt_session_control_point_init_example_control_point2.
 * - An entry in __wt_session_control_point_init_all (Could be generated).
 *
 * Each per session control point that is enabled at startup has:
 * - An entry in __wt_session_control_point_enable_all (Could be generated).
 */

#include "wt_internal.h"

#if 0
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

    __wt_verbose_notice(session, WT_VERB_TEMPORARY, "Start: id=%" PRId32, id);
    if (WT_UNLIKELY(id >= CONNECTION_CONTROL_POINTS_SIZE)) {
        __wt_verbose_error(session, WT_VERB_TEMPORARY,
          "ERROR: id(%" PRId32 ") >= CONNECTION_CONTROL_POINTS_SIZE(%" PRId32 ")", id,
          CONNECTION_CONTROL_POINTS_SIZE);
        return (NULL);
    }
    conn = S2C(session);
    if (WT_UNLIKELY(conn->control_points == NULL)) {
        __wt_verbose_notice(session, WT_VERB_TEMPORARY, "control_points is NULL: id=%" PRId32, id);
        return (NULL);
    }
    cp_registry = &(conn->control_points[id]);

    data = __wti_control_point_get_data(session, cp_registry, false);
    if (data == NULL) {
        /* Disabled. */
        __wt_verbose_notice(session, WT_VERB_TEMPORARY, "Is disabled: id=%" PRId32, id);
        return (NULL);
    }
    new_crossing_count = ++(cp_registry->crossing_count);
    triggered = cp_registry->pred ? cp_registry->pred(session, cp_registry, data) : true;
    if (triggered) {
        new_trigger_count = ++(cp_registry->trigger_count);
        __wt_verbose_notice(session, WT_VERB_TEMPORARY,
          "Triggered: id=%" PRId32 ", crossing_count=%" PRIu64 ", trigger_count=%" PRIu64, id,
          (uint64_t)new_crossing_count, (uint64_t)new_trigger_count);
    } else {
        __wt_verbose_notice(session, WT_VERB_TEMPORARY,
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
        __wt_verbose_error(session, WT_VERB_TEMPORARY,
          "ERROR: id(%" PRId32 ") >= SESSION_CONTROL_POINTS_SIZE(%" PRId32 ")", id,
          SESSION_CONTROL_POINTS_SIZE);
        return (NULL);
    }
    if (WT_UNLIKELY(session->control_points == NULL)) {
        __wt_verbose_notice(session, WT_VERB_TEMPORARY, "control_points is NULL: id=%" PRId32, id);
        return (NULL);
    }
    cp_registry = &(session->control_points[id]);

    data = cp_registry->data;
    if (data == NULL) {
        /* Disabled. */
        __wt_verbose_notice(session, WT_VERB_TEMPORARY, "Is disabled: id=%" PRId32, id);
        return (NULL);
    }

    triggered = cp_registry->pred ? cp_registry->pred(session, cp_registry, data) : true;
    new_crossing_count = ++(cp_registry->crossing_count);
    if (triggered) {
        new_trigger_count = ++(cp_registry->trigger_count);
        __wt_verbose_notice(session, WT_VERB_TEMPORARY,
          "Triggered: id=%" PRId32 ", crossing_count=%" PRIu64 ", trigger_count=%" PRIu64, id,
          (uint64_t)new_crossing_count, (uint64_t)new_trigger_count);
    } else {
        __wt_verbose_notice(session, WT_VERB_TEMPORARY,
          "Not Triggered: id=%" PRId32 ", crossing_count=%" PRIu64 ", trigger_count=%" PRIu64, id,
          (uint64_t)new_crossing_count, (uint64_t)cp_registry->trigger_count);
        /* Not triggered. */
        data = NULL;
    }
    return (data);
}

/*
 * Per connection control point initialization.
 */
/* From examples/ex_control_points.c */
#if 0  /* XXX TEMPORARY - Try without any control points. */
/*
 * Per connection control point "Main Start Printing".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_main_start_printing {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_main_start_printing --
 *     The per connection control point initialization function for control point "Main Start
 *     Printing".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_main_start_printing(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_main_start_printing *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Main Start Printing", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}

/*
 * Per connection control point "Thread 0".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_thread_0 {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_thread_0 --
 *     The per connection control point initialization function for control point "Thread 0".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_thread_0(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_thread_0 *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Thread 0", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}

/*
 * Per connection control point "Thread 1".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_thread_1 {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_thread_1 --
 *     The per connection control point initialization function for control point "Thread 1".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_thread_1(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_thread_1 *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Thread 1", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}

/*
 * Per connection control point "Thread 2".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_thread_2 {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_thread_2 --
 *     The per connection control point initialization function for control point "Thread 2".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_thread_2(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_thread_2 *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Thread 2", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}

/*
 * Per connection control point "Thread 3".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_thread_3 {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_thread_3 --
 *     The per connection control point initialization function for control point "Thread 3".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_thread_3(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_thread_3 *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Thread 3", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}

/*
 * Per connection control point "Thread 4".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_thread_4 {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_thread_4 --
 *     The per connection control point initialization function for control point "Thread 4".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_thread_4(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_thread_4 *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Thread 4", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}

/*
 * Per connection control point "Thread 5".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_thread_5 {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_thread_5 --
 *     The per connection control point initialization function for control point "Thread 5".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_thread_5(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_thread_5 *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Thread 5", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}

/*
 * Per connection control point "Thread 6".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_thread_6 {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_thread_6 --
 *     The per connection control point initialization function for control point "Thread 6".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_thread_6(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_thread_6 *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Thread 6", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}

/*
 * Per connection control point "Thread 7".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_thread_7 {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_thread_7 --
 *     The per connection control point initialization function for control point "Thread 7".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_thread_7(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_thread_7 *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Thread 7", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}

/*
 * Per connection control point "Thread 8".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_thread_8 {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_thread_8 --
 *     The per connection control point initialization function for control point "Thread 8".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_thread_8(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_thread_8 *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Thread 8", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}

/*
 * Per connection control point "Thread 9".
 */
/* Per connection control point data type. */
struct __wt_conn_control_point_data_thread_9 {
    WT_CONTROL_POINT iface;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER action_data;
};

/* Per connection control point init function. */
/*
 * __wt_conn_control_point_init_thread_9 --
 *     The per connection control point initialization function for control point "Thread 8".
 *
 * @param session The session. @param cp_registry The per connection control point's control point
 *     registry. @param cfg Configuration strings.
 */
WT_CONTROL_POINT *
__wt_conn_control_point_init_thread_9(WT_SESSION_IMPL *session, const char **cfg)
{
    struct __wt_conn_control_point_data_thread_9 *init_data;
    WT_DECL_RET;

    ret = __wt_calloc_one(session, &init_data);
    if (WT_UNLIKELY(ret != 0))
        return (NULL);

    /* Initialize configuration parameters. */
    WT_ERR(__wt_control_point_config_action_wait_for_trigger(
      session, (WT_CONTROL_POINT *)init_data, cfg));
    /* The predicate is "Always" therefore no predicate configuration parameters to initialize. */

    /* Extra initialization required for action "Wait for trigger". */
    __wt_control_point_action_init_wait_for_trigger(
      session, "Thread 9", (WT_CONTROL_POINT *)init_data);

err:
    if (ret != 0)
        __wt_free(session, init_data);

    return ((WT_CONTROL_POINT *)init_data);
}
#endif /* XXX TEMPORARY - Try without any control points. */

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
#if CONNECTION_CONTROL_POINTS_SIZE == 0
    WT_UNUSED(session);
    return (0);
#else
    WT_CONTROL_POINT_REGISTRY *control_points;
    WT_DECL_RET;

    if (CONNECTION_CONTROL_POINTS_SIZE == 0)
        return (0);
    WT_RET(__wt_calloc_def(session, CONNECTION_CONTROL_POINTS_SIZE, &control_points));

    /*
     * This part must be edited. Repeat this for every per connection control point.
     */
#if 0  /* XXX TEMPORARY - Try without any control points. */
    /* From examples/ex_control_points.c */
    control_points[WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING].init =
      __wt_conn_control_point_init_main_start_printing;
    control_points[WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(session,
      &(control_points[WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING].lock), "Main Start Printing"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_0].init = __wt_conn_control_point_init_thread_0;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_0].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_0].lock), "Thread 0"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_0].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_1].init = __wt_conn_control_point_init_thread_1;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_1].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_1].lock), "Thread 1"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_1].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_2].init = __wt_conn_control_point_init_thread_2;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_2].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_2].lock), "Thread 2"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_2].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_3].init = __wt_conn_control_point_init_thread_3;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_3].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_3].lock), "Thread 3"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_3].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_4].init = __wt_conn_control_point_init_thread_4;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_4].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_4].lock), "Thread 4"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_4].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_5].init = __wt_conn_control_point_init_thread_5;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_5].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_5].lock), "Thread 5"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_5].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_6].init = __wt_conn_control_point_init_thread_6;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_6].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_6].lock), "Thread 6"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_6].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_7].init = __wt_conn_control_point_init_thread_7;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_7].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_7].lock), "Thread 7"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_7].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_8].init = __wt_conn_control_point_init_thread_8;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_8].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_8].lock), "Thread 8"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_8].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;

    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_9].init = __wt_conn_control_point_init_thread_9;
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_9].pred = NULL; /* Always */
    WT_ERR(__wt_spin_init(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_THREAD_9].lock), "Thread 9"));
    /* Extra initialization required for action "Wait for trigger". */
    control_points[WT_CONN_CONTROL_POINT_ID_THREAD_9].action_supported =
      WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER;
#endif /* XXX TEMPORARY - Try without any control points. */

    /* After all repeats finish with this. */
    S2C(session)->control_points = control_points;

err:
    if (ret != 0)
        __wt_free(session, control_points);
    return (ret);
#endif /* CONNECTION_CONTROL_POINTS_SIZE == 0 */
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
      __wt_control_point_session_init_example2;
  control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2].pred =
      __wt_control_point_session_pred_examples;
  WT_ERR(__wt_spin_init(session, &(control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2].lock)));
#endif

    /* After all repeats finish with this. */
    session->control_points = control_points;

err:
    if (ret != 0)
        __wt_free(session, control_points);
    return (ret);
#endif /* SESSION_CONTROL_POINTS_SIZE == 0 */
}

/*
 * Control point startup functions: Enable at startup.
 */
/*
 * __wt_conn_control_point_enable_all --
 *     Enable per connection control points that start enabled. Note, one part of this function must
 *     be edited for each per connection control point that starts enabled.
 *
 * @param conn The connection.
 */
int
__wt_conn_control_point_enable_all(WT_SESSION_IMPL *session, const char **cfg)
{
    /* XXX TEMPORARY - Try without any control points. */
#if 1 /* If no per connection control points are enabled at the start. */
    WT_UNUSED(session);
    WT_UNUSED(cfg);
#else
    WT_CONNECTION_IMPL *conn;
    WT_CONTROL_POINT_REGISTRY *control_points;

    if (CONNECTION_CONTROL_POINTS_SIZE == 0)
        return (0);
    conn = S2C(session);
    control_points = conn->control_points;

    /*
     * This part must be edited. Repeat this for every per connection control point that starts
     * enabled.
     */
    /* From examples/ex_control_points.c */
    WT_RET(__wti_conn_control_point_enable(
      session, &(control_points[WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING]), cfg));
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
        &(control_points[WT_SESSION_CONTROL_POINT_ID_EXAMPLE2])));
#endif
#endif
    return (0);
}
#endif /* HAVE_CONTROL_POINT */
