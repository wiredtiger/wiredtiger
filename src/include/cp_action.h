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
 * - Macro to define a per connection control point with this action (Must be manual):
 * CONNECTION_CONTROL_POINT_DEFINE_EXAMPLE_ACTION.
 * - Macro to define a per session control point with this action (Must be manual):
 * SESSION_CONTROL_POINT_DEFINE_EXAMPLE_ACTION.
 *
 * An action that is called from the call site can only be used by a per connection control point.
 * Such an action also has:
 * - Macro used at the call site (Must be manual): CONNECTION_CONTROL_POINT_EXAMPLE_ACTION.
 * - A non-zero control point action ID (Could be generated):
 * WT_CONTROL_POINT_ACTION_ID_EXAMPLE_ACTION.
 */

#ifdef HAVE_CONTROL_POINT
/*
 * Define a per connection control point.
 */
/*!
 * The first part of a per connection control point definition macro.
 * Sets _data = WT_CONTROL_POINT_REGISTRY.data if triggered, and
 * NULL if not triggered.
 * Sets _conn, _session, _cp_registry, and _cp_id.
 */
#define CONNECTION_CONTROL_POINT_DEFINE_START(SESSION, CONTROL_POINT_ID) \
    do {                                                                 \
        WT_SESSION_IMPL *const _session = (SESSION);                     \
        WT_CONNECTION_IMPL *const _conn = S2C(_session);                 \
                                                                         \
        const wt_control_point_id_t _cp_id = (CONTROL_POINT_ID);         \
        WT_CONTROL_POINT_REGISTRY *_cp_registry;                         \
        WT_CONTROL_POINT *_data;                                         \
        WT_ASSERT(_session, _cp_id < CONNECTION_CONTROL_POINTS_SIZE);    \
        _cp_registry = &(_conn->control_points[_cp_id]);                 \
        _data = _cp_registry->data;                                      \
        if (_data != NULL)                                               \
            _data = __wt_conn_control_point_test_and_trigger(_session, _cp_id);

/*!
 * The last part of a per connection control point definition macro.
 */
#define CONNECTION_CONTROL_POINT_DEFINE_END(LOCKED)                               \
    if (_data != NULL)                                                            \
        __wt_control_point_release_data(_session, _cp_registry, _data, (LOCKED)); \
    }                                                                             \
    while (0)

/*
 * Define a per session control point.
 */
/*!
 * The first part of a per session control point definition macro.
 * Sets _data = WT_CONTROL_POINT_REGISTRY.data if triggered, and
 * NULL if not triggered.
 * Sets _conn, _session, _cp_registry, and _cp_id.
 */
#define SESSION_CONTROL_POINT_DEFINE_START(SESSION, CONTROL_POINT_ID) \
    do {                                                              \
        WT_SESSION_IMPL *const _session = (SESSION);                  \
        const wt_control_point_id_t _cp_id = (CONTROL_POINT_ID);      \
        WT_CONTROL_POINT_REGISTRY *_cp_registry;                      \
        WT_CONTROL_POINT *_data;                                      \
        WT_ASSERT(_session, _cp_id < SESSION_CONTROL_POINTS_SIZE);    \
        _cp_registry = &(_session->control_points[_cp_id]);           \
        _data = _cp_registry->data;                                   \
        if (_data != NULL)                                            \
            _data = __wt_session_control_point_test_and_trigger(_session, _cp_id);

/*!
 * The last part of a per session control point definition macro.
 */
#define SESSION_CONTROL_POINT_DEFINE_END \
    }                                    \
    while (0)
#endif /* HAVE_CONTROL_POINT */

/*
 * Action: Sleep: Delay at a specific code location during an execution via __wt_sleep
 */
#ifdef HAVE_CONTROL_POINT
/* Action data type */
struct __wt_control_point_action_sleep {
    /* Action Configuration parameter(s) */
    uint64_t seconds;
    uint64_t microseconds;
};

/* Control point action ID. */
#define WT_CONTROL_POINT_ACTION_ID_SLEEP 0
#endif /* HAVE_CONTROL_POINT */

/* Macro to define a per connection control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define CONNECTION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID)                           \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))                           \
    if (_data != NULL) {                                                                           \
        WT_CONTROL_POINT_ACTION_SLEEP *action_data = (WT_CONTROL_POINT_ACTION_SLEEP *)(_data + 1); \
        uint64_t _seconds = action_data->sleep;                                                    \
        uint64_t _microseconds = action_data->microseconds;                                        \
        /* _data not needed during action. */                                                      \
        __wt_control_point_release_data(_session, _cp_registry, _data, false);                     \
        _data = NULL;                                                                              \
        /* The action. */                                                                          \
        __wt_sleep(_seconds, _microseconds);                                                       \
    }                                                                                              \
    CONNECTION_CONTROL_POINT_DEFINE_END(false)
#else
#define CONNECTION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/* Macro to define a per session control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define SESSION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID)                              \
    SESSION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))                              \
    if (_data != NULL) {                                                                           \
        WT_CONTROL_POINT_ACTION_SLEEP *action_data = (WT_CONTROL_POINT_ACTION_SLEEP *)(_data + 1); \
        uint64_t _seconds = action_data->sleep;                                                    \
        uint64_t _microseconds = action_data->microseconds;                                        \
        /* The action. */                                                                          \
        __wt_sleep(_seconds, _microseconds);                                                       \
    }                                                                                              \
    SESSION_CONTROL_POINT_DEFINE_END
#else
#define SESSION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/*
 * Action: ERR: Change the control flow to trigger an error condition via WT_ERR
 */
/* Action data type */
#ifdef HAVE_CONTROL_POINT
struct __wt_control_point_action_err {
    /* Action Configuration parameter(s) */
    int err;
};

/* Control point action ID. */
#define WT_CONTROL_POINT_ACTION_ID_ERR 0
#endif /* HAVE_CONTROL_POINT */

/* Macro to define a per connection control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define CONNECTION_CONTROL_POINT_DEFINE_ERR(CONNECTION, SESSION, CONTROL_POINT_ID)     \
    CONNECTION_CONTROL_POINT_DEFINE_START((CONNECTION), (SESSION), (CONTROL_POINT_ID)) \
    if (_data != NULL) {                                                               \
        int _err = ((WT_CONTROL_POINT_ACTION_ERR *)(_data + 1))->err;                  \
        /* _data not needed during action. */                                          \
        __wt_control_point_release_data(_session, _cp_registry, _data, false);         \
        _data = NULL;                                                                  \
        /* The action. */                                                              \
        WT_ERR(_err);                                                                  \
    }                                                                                  \
    CONNECTION_CONTROL_POINT_DEFINE_END(false)
#else
#define CONNECTION_CONTROL_POINT_DEFINE_ERR(CONNECTION, SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/* Macro to define a per session control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define SESSION_CONTROL_POINT_DEFINE_ERR(CONNECTION, SESSION, CONTROL_POINT_ID)     \
    SESSION_CONTROL_POINT_DEFINE_START((CONNECTION), (SESSION), (CONTROL_POINT_ID)) \
    if (_data != NULL) {                                                            \
        int _err = ((WT_CONTROL_POINT_ACTION_ERR *)(_data + 1))->err;               \
        /* The action. */                                                           \
        WT_ERR(_err);                                                               \
    }                                                                               \
    SESSION_CONTROL_POINT_DEFINE_END
#else
#define SESSION_CONTROL_POINT_DEFINE_ERR(CONNECTION, SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/*
 * Action: RET: Return an error via WT_RET
 */
/* Action data type */
#ifdef HAVE_CONTROL_POINT
struct __wt_control_point_action_ret {
    /* Action Configuration parameter(s) */
    int ret_value;
};

/* Control point action ID. */
#define WT_CONTROL_POINT_ACTION_ID_RET 0
#endif /* HAVE_CONTROL_POINT */

/* Macro to define a per connection control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define CONNECTION_CONTROL_POINT_DEFINE_RET(SESSION, CONTROL_POINT_ID)            \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))          \
    if (_data != NULL) {                                                          \
        int _ret_value = ((WT_CONTROL_POINT_ACTION_RET *)(_data + 1))->ret_value; \
        /* _data not needed during action. */                                     \
        __wt_control_point_release_data(_session, _cp_register, _data, false);    \
        _data = NULL; /* The action. */                                           \
        WT_RET(_ret_value);                                                       \
    }                                                                             \
    CONNECTION_CONTROL_POINT_DEFINE_END(false)
#else
#define CONNECTION_CONTROL_POINT_DEFINE_RET(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/* Macro to define a per session control point with this action. */
#ifdef HAVE_CONTROL_POINT
#define SESSION_CONTROL_POINT_DEFINE_RET(SESSION, CONTROL_POINT_ID)               \
    SESSION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))             \
    if (_data != NULL) {                                                          \
        int _ret_value = ((WT_CONTROL_POINT_ACTION_RET *)(_data + 1))->ret_value; \
        /* The action. */                                                         \
        WT_RET(_ret_value);                                                       \
    }                                                                             \
    SESSION_CONTROL_POINT_DEFINE_END
#else
#define SESSION_CONTROL_POINT_DEFINE_RET(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/*
 * Action: Wait for trigger: Blocking the testing thread until a control point is triggered
 */
/* Action data type */
#ifdef HAVE_CONTROL_POINT
struct __wt_control_point_action_wait_for_trigger {
    /* Action Configuration parameter */
    uint64_t wait_count;
    /* Action state variables */
    uint64_t desired_trigger_count;
    WT_CONDVAR *condvar;
};

/* Control point action ID. */
#define WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER 1

/* Macro used at the call site. */
/*!
 * The call site portion of control point action "Wait for Trigger: Blocking the testing thread
 * until a control point is triggered".
 */
#define CONNECTION_CONTROL_POINT_WAIT_FOR_TRIGGER(SESSION, CONTROL_POINT_ID, ENABLED)        \
    do {                                                                                     \
        WT_SESSION_IMPL *const _session = (SESSION);                                         \
        WT_CONNECTION_IMPL *const _conn = S2C(_session);                                     \
        const wt_control_point_id_t _cp_id = (CONTROL_POINT_ID);                             \
        WT_CONTROL_POINT_REGISTRY *_cp_registry;                                             \
        WT_CONTROL_POINT *_data;                                                             \
        WT_ASSERT(_session, _cp_id < CONNECTION_CONTROL_POINTS_SIZE);                        \
        _cp_registry = &(_conn->control_points[_cp_id]);                                     \
        _data = _cp_registry->data;                                                          \
        if (_data != NULL)                                                                   \
            (ENABLED) = __wt_control_point_wait_for_trigger(_session, _cp_registry, _cp_id); \
        else                                                                                 \
            (ENABLED) = false;                                                               \
    } while (0)
#endif

/* Macro to define a per connection control point with this action. */
#ifdef HAVE_CONTROL_POINT
/*!
 * The trigger site portion of control point action "Wait for Trigger: Blocking the testing thread
 * until a control point is triggered".
 */
#define CONNECTION_CONTROL_POINT_DEFINE_WAIT_FOR_TRIGGER(SESSION, CONTROL_POINT_ID) \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))            \
    if (_data != NULL) {                                                            \
        WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER *action_data =                     \
          (WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER *)(_data + 1);                  \
        __wt_control_point_unlock(_session, _cp_registry);                          \
        /* The action. */                                                           \
        __wt_cond_signal(_session, action_data->condvar);                           \
    }                                                                               \
    CONNECTION_CONTROL_POINT_DEFINE_END(false)
#else
#define CONNECTION_CONTROL_POINT_DEFINE_WAIT_FOR_TRIGGER(SESSION, CONTROL_POINT_ID) /* NOP */
#endif
