/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* cp_action.h: Declarations for control point actions. */
/* This file must be edited when a new control point action is created. */

/*
 * The names below are for an action named "Example action".
 *
 * Each action has:
 * - Action data type (Must be manual): WT_CONTROL_POINT_ACTION_EXAMPLE_ACTION.
 * - Pair data type (Could be generated): WT_CONTROL_POINT_PAIR_DATA_EXAMPLE_ACTION.
 *   Note, the pair data type is dependent upon only the action, not the predicate.
 * - Macro to define a per-connection control point with this action (Must be manual):
 * CONNECTION_CONTROL_POINT_DEFINE_EXAMPLE_ACTION.
 * - Macro to define a per-session control point with this action (Must be manual):
 * SESSION_CONTROL_POINT_DEFINE_EXAMPLE_ACTION.
 *
 * An action that is called from the call site can only be used by a per-connection control point.
 * Such an action also has:
 * - Macro used at the call site (Must be manual): CONNECTION_CONTROL_POINT_EXAMPLE_ACTION.
 * - A non-zero control point action ID (Could be generated):
 * WT_CONTROL_POINT_ACTION_ID_EXAMPLE_ACTION.
 */

#ifdef HAVE_CONTROL_POINT
/*
 * Define a per-connection control point.
 */
/*!
 * The first part of a per-connection control point definition macro.
 * Sets _cp_data = WT_CONTROL_POINT_REGISTRY.cp_data if triggered, and
 * NULL if not triggered.
 * Sets _conn, _session, _cp_registry, _cp_data, and _cp_id.
 */
#define CONNECTION_CONTROL_POINT_DEFINE_START(SESSION, CONTROL_POINT_ID, ASSIGN, VALUE64) \
    do {                                                                                  \
        WT_SESSION_IMPL *const _session = (SESSION);                                      \
        WT_CONNECTION_IMPL *const _conn = S2C(_session);                                  \
                                                                                          \
        const wt_control_point_id_t _cp_id = (CONTROL_POINT_ID);                          \
        WT_CONTROL_POINT_REGISTRY *_cp_registry;                                          \
        WT_CONTROL_POINT_DATA *_cp_data;                                                  \
        WT_ASSERT(_session, _cp_id < CONNECTION_CONTROL_POINTS_SIZE);                     \
        _cp_registry = &(_conn->control_points[_cp_id]);                                  \
        _cp_data = _cp_registry->cp_data;                                                 \
        if (_cp_data != NULL)                                                             \
            _cp_data =                                                                    \
              __wt_conn_control_point_test_and_trigger(_session, _cp_id, (ASSIGN), (VALUE64));

/*!
 * The last part of a per-connection control point definition macro.
 */
#define CONNECTION_CONTROL_POINT_DEFINE_END(LOCKED)                                  \
    if (_cp_data != NULL)                                                            \
        __wt_control_point_release_data(_session, _cp_registry, _cp_data, (LOCKED)); \
    }                                                                                \
    while (0)

/*
 * Define a per-session control point.
 */
/*!
 * The first part of a per-session control point definition macro.
 * Sets _cp_data = WT_CONTROL_POINT_REGISTRY.cp_data if triggered, and
 * NULL if not triggered.
 * Sets _session, _cp_registry, _cp_data, and _cp_id.
 */
#define SESSION_CONTROL_POINT_DEFINE_START(SESSION, CONTROL_POINT_ID) \
    do {                                                              \
        WT_SESSION_IMPL *const _session = (SESSION);                  \
        const wt_control_point_id_t _cp_id = (CONTROL_POINT_ID);      \
        WT_CONTROL_POINT_REGISTRY *_cp_registry;                      \
        WT_CONTROL_POINT_DATA *_cp_data;                              \
        WT_ASSERT(_session, _cp_id < SESSION_CONTROL_POINTS_SIZE);    \
        _cp_registry = &(_session->control_points[_cp_id]);           \
        _cp_data = _cp_registry->cp_data;                             \
        if (_cp_data != NULL)                                         \
            _cp_data = __wt_session_control_point_test_and_trigger(_session, _cp_id);

/*!
 * The last part of a per-session control point definition macro.
 */
#define SESSION_CONTROL_POINT_DEFINE_END \
    }                                    \
    while (0)
#endif /* HAVE_CONTROL_POINT */

/*
 * Action: Sleep: Delay at a specific code location during an execution via __wt_sleep
 *
 * # Action configuration parameters
 * Config('seconds', '1', r'''
 *     the number of seconds to sleep''',
 *     min='0', max=ControlPoint.int64_max),
 * Config('microseconds', '1', r'''
 *     the number of microseconds to sleep''',
 *     min='0', max=ControlPoint.int64_max),
 */
#ifdef HAVE_CONTROL_POINT
/* Action data type */
struct __wt_control_point_action_sleep {
    /* Action Configuration parameter(s) */
    uint64_t seconds;
    uint64_t microseconds;
};

/* Pair data type */
struct __wt_control_point_pair_data_sleep {
    WT_CONTROL_POINT_DATA iface;
    WT_CONTROL_POINT_ACTION_SLEEP action_data;
};

/* Control point action ID. */
#define WT_CONTROL_POINT_ACTION_ID_SLEEP 0
#endif /* HAVE_CONTROL_POINT */

/* Macro to define a per-connection control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define CONNECTION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID)           \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID), false, 0) \
    if (_cp_data != NULL) {                                                        \
        WT_CONTROL_POINT_PAIR_DATA_SLEEP *_pair_data =                             \
          (WT_CONTROL_POINT_PAIR_DATA_SLEEP *)_cp_data;                            \
        WT_CONTROL_POINT_ACTION_SLEEP *action_data = &_pair_data->action_data;     \
        uint64_t _seconds = action_data->sleep;                                    \
        uint64_t _microseconds = action_data->microseconds;                        \
        /* _cp_data not needed during action. */                                   \
        __wt_control_point_release_data(_session, _cp_registry, _cp_data, false);  \
        _cp_data = NULL;                                                           \
        /* The action. */                                                          \
        __wt_sleep(_seconds, _microseconds);                                       \
    }                                                                              \
    CONNECTION_CONTROL_POINT_DEFINE_END(false)
#else
#define CONNECTION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/* Macro to define a per-session control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define SESSION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID)          \
    SESSION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))          \
    if (_cp_data != NULL) {                                                    \
        WT_CONTROL_POINT_PAIR_DATA_SLEEP *_pair_data =                         \
          (WT_CONTROL_POINT_PAIR_DATA_SLEEP *)_cp_data;                        \
        WT_CONTROL_POINT_ACTION_SLEEP *action_data = &_pair_data->action_data; \
        uint64_t _seconds = action_data->seconds;                              \
        uint64_t _microseconds = action_data->microseconds;                    \
        /* The action. */                                                      \
        __wt_sleep(_seconds, _microseconds);                                   \
    }                                                                          \
    SESSION_CONTROL_POINT_DEFINE_END
#else
#define SESSION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/*
 * Action: ERR: Change the control flow to trigger an error condition via WT_ERR
 *
 * # Action configuration parameter
 * Config('err', '1', r'''
 *     the error to return''',
 *     min='0', max=ControlPoint.int64_max),
 */
#ifdef HAVE_CONTROL_POINT
/* Action data type */
struct __wt_control_point_action_err {
    /* Action Configuration parameter(s) */
    int err;
};

/* Pair data type */
struct __wt_control_point_pair_data_err {
    WT_CONTROL_POINT_DATA iface;
    WT_CONTROL_POINT_ACTION_ERR action_data;
};

/* Control point action ID. */
#define WT_CONTROL_POINT_ACTION_ID_ERR 0
#endif /* HAVE_CONTROL_POINT */

/* Macro to define a per-connection control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define CONNECTION_CONTROL_POINT_DEFINE_ERR(SESSION, CONTROL_POINT_ID)            \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))          \
    if (_cp_data != NULL) {                                                       \
        int _err = ((WT_CONTROL_POINT_PAIR_DATA_ERR *)_cp_data)->action_data.err; \
        /* _cp_data not needed during action. */                                  \
        __wt_control_point_release_data(_session, _cp_registry, _cp_data, false); \
        _cp_data = NULL;                                                          \
        /* The action. */                                                         \
        WT_ERR(_err);                                                             \
    }                                                                             \
    CONNECTION_CONTROL_POINT_DEFINE_END(false)
#else
#define CONNECTION_CONTROL_POINT_DEFINE_ERR(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/* Macro to define a per-session control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define SESSION_CONTROL_POINT_DEFINE_ERR(SESSION, CONTROL_POINT_ID)   \
    SESSION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID)) \
    if (_cp_data != NULL) {                                           \
        int _err = ((WT_CONTROL_POINT_PAIR_DATA_ERR *)_cp_data)->err; \
        /* The action. */                                             \
        WT_ERR(_err);                                                 \
    }                                                                 \
    SESSION_CONTROL_POINT_DEFINE_END
#else
#define SESSION_CONTROL_POINT_DEFINE_ERR(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/*
 * Action: RET: Return an error via WT_RET
 *
 * # Action configuration parameter
 * Config('ret_value', '1', r'''
 *     the value to return''',
 *     min='0', max=ControlPoint.int64_max),
 */
#ifdef HAVE_CONTROL_POINT
/* Action data type */
struct __wt_control_point_action_ret {
    /* Action Configuration parameter(s) */
    int ret_value;
};

/* Pair data type */
struct __wt_control_point_pair_data_ret {
    WT_CONTROL_POINT_DATA iface;
    WT_CONTROL_POINT_ACTION_RET action_data;
};

/* Control point action ID. */
#define WT_CONTROL_POINT_ACTION_ID_RET 0
#endif /* HAVE_CONTROL_POINT */

/* Macro to define a per-connection control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define CONNECTION_CONTROL_POINT_DEFINE_RET(SESSION, CONTROL_POINT_ID)             \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID), false, 0) \
    if (_cp_data != NULL) {                                                        \
        int _ret_value = ((WT_CONTROL_POINT_PAIR_DATA_RET *)_cp_data)->ret_value;  \
        /* _cp_data not needed during action. */                                   \
        __wt_control_point_release_data(_session, _cp_register, _cp_data, false);  \
        _cp_data = NULL; /* The action. */                                         \
        WT_RET(_ret_value);                                                        \
    }                                                                              \
    CONNECTION_CONTROL_POINT_DEFINE_END(false)
#else
#define CONNECTION_CONTROL_POINT_DEFINE_RET(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/* Macro to define a per-session control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define SESSION_CONTROL_POINT_DEFINE_RET(SESSION, CONTROL_POINT_ID)               \
    SESSION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))             \
    if (_cp_data != NULL) {                                                       \
        int _ret_value = ((WT_CONTROL_POINT_PAIR_DATA_RET *)_cp_data)->ret_value; \
        /* The action. */                                                         \
        WT_RET(_ret_value);                                                       \
    }                                                                             \
    SESSION_CONTROL_POINT_DEFINE_END
#else
#define SESSION_CONTROL_POINT_DEFINE_RET(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/*
 * Action: Trigger: Block the testing thread until a control point is triggered.
 *
 * # Action configuration parameter
 * Config('wait_count', '1', r'''
 *     the number of triggers for which to wait''',
 *     min='0', max=ControlPoint.int64_max),
 */
#ifdef HAVE_CONTROL_POINT
/* Action data type */
struct __wt_control_point_action_trigger {
    /* Action Configuration parameter */
    uint64_t wait_count;
    /* Action state variables */
    uint64_t desired_trigger_count;
    WT_CONDVAR *condvar;
};

/* Pair data type */
struct __wt_control_point_pair_data_trigger {
    WT_CONTROL_POINT_DATA iface;
    WT_CONTROL_POINT_ACTION_TRIGGER action_data;
};

/* Control point action ID. */
#define WT_CONTROL_POINT_ACTION_ID_TRIGGER 1
#endif /* HAVE_CONTROL_POINT */

/* Macros used at the call site. */
#ifdef HAVE_CONTROL_POINT
/* Wait for the control point to be triggered. */
/*!
 * Wait for a per-connection control point with action "Trigger: Block the testing
 * thread until a control point is triggered" to be triggered.
 */
#define CONNECTION_CONTROL_POINT_WAIT(SESSION, CONTROL_POINT_ID)      \
    do {                                                              \
        WT_SESSION_IMPL *const _session = (SESSION);                  \
        WT_CONNECTION_IMPL *const _conn = S2C(_session);              \
        const wt_control_point_id_t _cp_id = (CONTROL_POINT_ID);      \
        WT_CONTROL_POINT_REGISTRY *_cp_registry;                      \
        WT_CONTROL_POINT_DATA *_cp_data;                              \
        WT_ASSERT(_session, _cp_id < CONNECTION_CONTROL_POINTS_SIZE); \
        _cp_registry = &(_conn->control_points[_cp_id]);              \
        _cp_data = _cp_registry->cp_data;                             \
        if (_cp_data != NULL)                                         \
            __wt_control_point_wait(_session, _cp_registry, _cp_id);  \
    } while (0)
#else
#define CONNECTION_CONTROL_POINT_WAIT(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

#ifdef HAVE_CONTROL_POINT
/* Set the "match value". */
/*!
 * Set the "match value" at the call site of a per-connection control point with predicate "Param 64
 * match".
 */
#define CONNECTION_CONTROL_POINT_SET_MATCH_VALUE_FOR_PARAM_64_MATCH( \
  CONNECTION, CONTROL_POINT_ID, VALUE64)                             \
    __wt_conn_control_point_set_param1((CONNECTION), (CONTROL_POINT_ID), (VALUE64))
#else
#define CONNECTION_CONTROL_POINT_SET_MATCH_VALUE_FOR_PARAM_64_MATCH( \
  CONNECTION, CONTROL_POINT_ID, VALUE64)                             \
    (0) /* NOP */
#endif

#ifdef HAVE_CONTROL_POINT
/* Set the "match value" and wait for the control point to be triggered. */
/*!
 * Set the "match value" and wait for a per-connection control point with action "Trigger:
 * Block the testing thread until a control point is triggered" to be triggered.
 */
#define CONNECTION_CONTROL_POINT_SET_MATCH_VALUE_AND_WAIT(SESSION, CONTROL_POINT_ID, VALUE64) \
    do {                                                                                      \
        WT_SESSION_IMPL *const _session = (SESSION);                                          \
        WT_CONNECTION_IMPL *const _conn = S2C(_session);                                      \
        const wt_control_point_id_t _cp_id = (CONTROL_POINT_ID);                              \
        WT_CONTROL_POINT_REGISTRY *_cp_registry;                                              \
        WT_CONTROL_POINT_DATA *_cp_data;                                                      \
        WT_ASSERT(_session, _cp_id < CONNECTION_CONTROL_POINTS_SIZE);                         \
        _cp_registry = &(_conn->control_points[_cp_id]);                                      \
        _cp_data = _cp_registry->cp_data;                                                     \
        if (_cp_data != NULL) {                                                               \
            _cp_data->param1.value64 = (VALUE64);                                             \
            __wt_control_point_wait(_session, _cp_registry, _cp_id);                          \
        }                                                                                     \
    } while (0)
#else
#define CONNECTION_CONTROL_POINT_SET_MATCH_VALUE_AND_WAIT( \
  SESSION, CONTROL_POINT_ID, VALUE64) /* NOP */
#endif

/* Macros used at the trigger site. */
#ifdef HAVE_CONTROL_POINT
/* Define a per-connection control point with this action. */
/*!
 * Define a per-connection control point with action "Trigger: Block the testing thread
 * until a control point is triggered". When triggered signal any waiting threads.
 */
#define CONNECTION_CONTROL_POINT_DEFINE_TRIGGER(SESSION, CONTROL_POINT_ID)         \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID), false, 0) \
    if (_cp_data != NULL) {                                                        \
        WT_CONTROL_POINT_PAIR_DATA_TRIGGER *_pair_data =                           \
          (WT_CONTROL_POINT_PAIR_DATA_TRIGGER *)_cp_data;                          \
        WT_CONTROL_POINT_ACTION_TRIGGER *action_data = &_pair_data->action_data;   \
        __wt_control_point_unlock(_session, _cp_registry);                         \
        /* The action. */                                                          \
        __wt_cond_signal(_session, action_data->condvar);                          \
    }                                                                              \
    CONNECTION_CONTROL_POINT_DEFINE_END(false)
#else
#define CONNECTION_CONTROL_POINT_DEFINE_TRIGGER(CONNECTION, CONTROL_POINT_ID) /* NOP */
#endif

#ifdef HAVE_CONTROL_POINT
/* Set the "test value". */
/*!
 * Set the "test value" at the trigger site of a per-connection control point with predicate "Param
 * 64 match".
 */
#define CONNECTION_CONTROL_POINT_SET_TEST_VALUE_FOR_PARAM_64_MATCH( \
  CONNECTION, CONTROL_POINT_ID, VALUE64)                            \
    __wt_conn_control_point_set_param2((CONNECTION), (CONTROL_POINT_ID), (VALUE64))
#else
#define CONNECTION_CONTROL_POINT_SET_TEST_VALUE_FOR_PARAM_64_MATCH( \
  CONNECTION, CONTROL_POINT_ID, VALUE64)                            \
    (0) /* NOP */
#endif

#ifdef HAVE_CONTROL_POINT
/* Set the "test value" and define a per-connection control point with this action. */
/*!
 * Set the test value and define a per-connection control point with action "Trigger:
 * Block the testing thread until a control point is triggered". When triggered signal any
 * waiting threads.
 */
#define CONNECTION_CONTROL_POINT_SET_TEST_VALUE_AND_DEFINE_TRIGGER(                       \
  SESSION, CONTROL_POINT_ID, VALUE64)                                                     \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID), true, (VALUE64)) \
    if (_cp_data != NULL) {                                                               \
        WT_CONTROL_POINT_PAIR_DATA_TRIGGER *_pair_data =                                  \
          (WT_CONTROL_POINT_PAIR_DATA_TRIGGER *)_cp_data;                                 \
        WT_CONTROL_POINT_ACTION_TRIGGER *action_data = &_pair_data->action_data;          \
        __wt_control_point_unlock(_session, _cp_registry);                                \
        /* The action. */                                                                 \
        __wt_cond_signal(_session, action_data->condvar);                                 \
    }                                                                                     \
    CONNECTION_CONTROL_POINT_DEFINE_END(false)
#else
#define CONNECTION_CONTROL_POINT_SET_TEST_VALUE_AND_DEFINE_TRIGGER( \
  SESSION, CONTROL_POINT_ID, VALUE64) /* NOP */
#endif
