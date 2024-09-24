/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* cp_action.h: Declarations for control point actions. */

/*===========================================================================*/
#if defined(HAVE_CONTROL_POINTS) && (CONNECTION_CONTROL_POINTS_SIZE > 0)
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
        const WT_CONTROL_POINT_ID _cp_id = (CONTROL_POINT_ID);           \
        WT_CONTROL_POINT_REGISTRY *_cp_registry;                         \
        WT_CONTROL_POINT *_data;                                         \
        WT_ASSERT(_session, _cp_id < CONNECTION_CONTROL_POINTS_SIZE);    \
        _cp_registry = &(_conn->control_points[_cp_id]);                     \
        _data = _cp_registry->data;                                       \
        if (_data != NULL)                                               \
            _data = __wt_conn_control_point_test_and_trigger(_session, _cp_id);

#define CONNECTION_CONTROL_POINT_DEFINE_END \
    if (_data != NULL) {                    \
        __wt_release_data(_session, _data); \
    } while (0)
#endif

/*---------------------------------------------------------------------------*/
#if defined(HAVE_CONTROL_POINTS) && (SESSION_CONTROL_POINTS_SIZE > 0)
/*!
 * The first part of a per session control point definition macro.
 * Sets _data = WT_CONTROL_POINT_REGISTRY.data if triggered, and
 * NULL if not triggered.
 * Sets _conn, _session, _cp_registry, and _cp_id.
 */
#define SESSION_CONTROL_POINT_DEFINE_START(SESSION, CONTROL_POINT_ID) \
    do {                                                              \
        WT_SESSION_IMPL *const _session = (SESSION);                  \
        const WT_CONTROL_POINT_ID _cp_id = (CONTROL_POINT_ID);        \
        WT_CONTROL_POINT_REGISTRY *_cp_registry;                      \
        WT_CONTROL_POINT *_data;                                      \
        WT_ASSERT(_session, _cp_id < SESSION_CONTROL_POINTS_SIZE);    \
        _cp_registry = &(_session->control_points[_cp_id]);               \
        _data = _cp_registry->data;                                    \
        if (_data != NULL)                                            \
            _data = __wt_session_control_point_test_and_trigger(_session, _cp_id);

#define SESSION_CONTROL_POINT_DEFINE_END \
    } while (0)
#endif

/*===========================================================================*/
#ifdef HAVE_CONTROL_POINTS
struct __wt_control_point_action_sleep;
typedef struct __wt_control_point_action_sleep WT_CONTROL_POINT_ACTION_SLEEP;
struct __wt_control_point_action_sleep {
    /* Action Configuration parameter(s) */
    uint64_t seconds;
    uint64_t microseconds;
};
#endif

#if defined(HAVE_CONTROL_POINTS) && (CONNECTION_CONTROL_POINTS_SIZE > 0)
#define CONNECTION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID)                       \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))                       \
    if (_data != NULL) {                                                                       \
        WT_CONTROL_POINT_ACTION_SLEEP *action_data = (WT_CONTROL_POINT_ACTION_SLEEP *)(_data + 1); \
        uint64_t _seconds = action_data->sleep;                                                \
        uint64_t _microseconds = action_data->microseconds;                                    \
        /* _data not needed during action. */                                                  \
        __wt_release_data(_session, _data);                                                    \
        _data = NULL; /* The action. */                                                        \
        __wt_sleep(_seconds, _microseconds);                                                   \
    }                                                                                          \
    CONNECTION_CONTROL_POINT_DEFINE_END
#else
#define CONNECTION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/*---------------------------------------------------------------------------*/
#if defined(HAVE_CONTROL_POINTS) && (SESSION_CONTROL_POINTS_SIZE > 0)
#define SESSION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID)                          \
    SESSION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))                          \
    if (_data != NULL) {                                                                       \
        WT_CONTROL_POINT_ACTION_SLEEP *action_data = (WT_CONTROL_POINT_ACTION_SLEEP *)(_data + 1); \
        uint64_t _seconds = action_data->sleep;                                                \
        uint64_t _microseconds = action_data->microseconds;                                    \
        /* The action. */                                                                      \
        __wt_sleep(_seconds, _microseconds);                                                   \
    }                                                                                          \
    SESSION_CONTROL_POINT_DEFINE_END
#else
#define SESSION_CONTROL_POINT_DEFINE_SLEEP(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/*===========================================================================*/
#ifdef HAVE_CONTROL_POINTS
struct __wt_control_point_action_err;
typedef struct __wt_control_point_action_err WT_CONTROL_POINT_ACTION_ERR;
struct __wt_control_point_action_err {
    /* Action Configuration parameter(s) */
    int err;
};
#endif

#if defined(HAVE_CONTROL_POINTS) && (CONNECTION_CONTROL_POINTS_SIZE > 0)
#define CONNECTION_CONTROL_POINT_DEFINE_ERR(CONNECTION, SESSION, CONTROL_POINT_ID)     \
    CONNECTION_CONTROL_POINT_DEFINE_START((CONNECTION), (SESSION), (CONTROL_POINT_ID)) \
    if (_data != NULL) {                                                               \
        int _err = ((WT_CONTROL_POINT_ACTION_ERR *)(_data + 1))->err;                  \
        /* _data not needed during action. */                                          \
        __wt_release_data(_session, _data);                                            \
        _data = NULL; /* The action. */                                                \
        WT_ERR(_err);                                                                  \
    }                                                                                  \
    CONNECTION_CONTROL_POINT_DEFINE_END
#else
#define CONNECTION_CONTROL_POINT_DEFINE_ERR(CONNECTION, SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/*---------------------------------------------------------------------------*/
#if defined(HAVE_CONTROL_POINTS) && (SESSION_CONTROL_POINTS_SIZE > 0)
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

/*===========================================================================*/
#ifdef HAVE_CONTROL_POINTS
struct __wt_control_point_action_ret;
typedef struct __wt_control_point_action_ret WT_CONTROL_POINT_ACTION_RET;
struct __wt_control_point_action_ret {
    /* Action Configuration parameter(s) */
    int ret_value;
};
#endif

#if defined(HAVE_CONTROL_POINTS) && (CONNECTION_CONTROL_POINTS_SIZE > 0)
#define CONNECTION_CONTROL_POINT_DEFINE_RET(SESSION, CONTROL_POINT_ID)            \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))          \
    if (_data != NULL) {                                                          \
        int _ret_value = ((WT_CONTROL_POINT_ACTION_RET *)(_data + 1))->ret_value; \
        /* _data not needed during action. */                                     \
        __wt_release_data(_session, _data);                                       \
        _data = NULL; /* The action. */                                           \
        WT_RET(_ret_value);                                                       \
    }                                                                             \
    CONNECTION_CONTROL_POINT_DEFINE_END
#else
#define CONNECTION_CONTROL_POINT_DEFINE_RET(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

/*---------------------------------------------------------------------------*/
#if defined(HAVE_CONTROL_POINTS) && (SESSION_CONTROL_POINTS_SIZE > 0)
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

/*===========================================================================*/
#if defined(HAVE_CONTROL_POINTS) && (CONNECTION_CONTROL_POINTS_SIZE > 0)
/*!
 * The call site portion of control point action "Wait for Trigger: Blocking the testing thread until a control point
 * is triggered".
 */
#define CONNECTION_CONTROL_POINT_WAIT_FOR_TRIGGER(SESSION, CONTROL_POINT_ID, ENABLED) \
    do {                                                                              \
        WT_SESSION_IMPL *const _session = (SESSION);                                  \
        WT_CONNECTION_IMPL *const _conn = S2C(_session);                              \
        const WT_CONTROL_POINT_ID _cp_id = (CONTROL_POINT_ID);                        \
        WT_CONTROL_POINT_REGISTRY *_cp_registry;                                      \
        WT_CONTROL_POINT *_data;                                                      \
        WT_ASSERT(_session, _cp_id < CONNECTION_CONTROL_POINTS_SIZE);                 \
        _cp_registry = &(_conn->control_points[_cp_id]);                              \
        _data = _cp_registry->data;                                                   \
        if (_data != NULL)                                                            \
            (ENABLED) = __wt_control_point_wait_for_trigger(_session, _cp_registry);  \
    else                                                                              \
    {                                                                                 \
        (ENABLED) = false;                                                            \
    }                                                                                 \
    } while (0)
#endif

#if defined(HAVE_CONTROL_POINTS) && (CONNECTION_CONTROL_POINTS_SIZE > 0)
/*!
 * The trigger site portion of control point action "Wait for Trigger: Blocking the testing thread until a control
 * point is triggered".
 */
#define CONNECTION_CONTROL_POINT_DEFINE_WAIT_FOR_TRIGGER(SESSION, CONTROL_POINT_ID) \
    CONNECTION_CONTROL_POINT_DEFINE_START((SESSION), (CONTROL_POINT_ID))            \
    if (_data != NULL) {                                                            \
        __wt_control_point_unlock(_cp_registry);                                    \
        /* The action. */                                                           \
        __wt_cond_signal(_session, _data->condvar);                                 \
    }                                                                               \
    CONNECTION_CONTROL_POINT_DEFINE_END
#else
#define CONNECTION_CONTROL_POINT_DEFINE_WAIT_FOR_TRIGGER(SESSION, CONTROL_POINT_ID) /* NOP */
#endif

#if defined(HAVE_CONTROL_POINTS) && (CONNECTION_CONTROL_POINTS_SIZE > 0)
WT_CONTROL_POINT *
__wt_conn_control_point_test_and_trigger(WT_SESSION_IMPL *session, WT_CONTROL_POINT_ID id);

bool
__wt_control_point_wait_for_trigger(
        WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry);
#endif

#if defined(HAVE_CONTROL_POINTS) && (SESSION_CONTROL_POINTS_SIZE > 0)
WT_CONTROL_POINT * __wt_session_control_point_test_and_trigger(WT_SESSION_IMPL *session, WT_CONTROL_POINT_ID id);
#endif

#ifdef HAVE_CONTROL_POINTS
void __wt_control_point_unlock(WT_CONTROL_POINT_REGISTRY *cp_registry);
WT_CONTROL_POINT * __wt_control_point_get_data(WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, bool locked);
void __wt_control_point_release_data(WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, WT_CONTROL_POINT *data, bool locked);
#endif
