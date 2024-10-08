/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* control_points.h: Declarations for control points. */

/* These types are not conditional upon HAVE_CONTROL_POINT since they are used in extern.h. */

#if defined(DOXYGEN) || defined(SWIG)
#define __F(func) func
#else
/* NOLINTNEXTLINE(misc-macro-parentheses) */
#define __F(func) *func
#endif

/*!
 * Identifies a control point action.
 * Used to verify compatibility between the code at a
 * control point call site and at the control point trigger site.
 * Zero if the action does not have call site code.
 */
typedef uint32_t wt_control_point_action_id_t;

/*!
 * A function to initialize a control point's data.
 * If per-connection session = NULL.
 */
typedef WT_CONTROL_POINT *wt_control_point_init_t(
  WT_SESSION_IMPL *session, const char *cp_config_name, const char **cfg);

/*!
 * A function to test whether a control point should be triggered.
 */
typedef bool wt_control_point_pred_t(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, WT_CONTROL_POINT *data);

/*!
 * Registration data for one control point.
 */
struct __wt_control_point_registry {
    wt_control_point_init_t __F(init); /* Function to initialize the control point. */
    wt_control_point_pred_t __F(pred); /* Function to test whether to trigger. */
    size_t crossing_count;             /* Count of executions of the trigger site. */
    size_t trigger_count;              /* Count of triggers, i.e. pred returned true. */
    WT_SPINLOCK lock;                  /* Atomically access data and data->ref_count. */
    const char *config_name;           /* Control point config name */
    WT_CONTROL_POINT *data; /* Disabled if NULL. More data may follow WT_CONTROL_POINT. */
    wt_control_point_action_id_t action_supported; /* For compatibility checking. */
};

/*!
 * A reference count for a WT_CONTROL_POINT.
 * This is needed by only per connection control points, not per session control points.
 */
typedef uint32_t wt_control_point_ref_count_t;

/*!
 * A pred parameter in WT_CONTROL_POINT.
 * The usage and meaning depends upon the pred function.
 */
union __wt_control_point_param {
    void *pointer;
    uint64_t value64;
    struct {
        union {
            uint32_t value32a;
            struct {
                uint16_t value16aa;
                uint16_t value16ab;
            };
        };
        union {
            uint32_t value32b;
            struct {
                uint16_t value16ba;
                uint16_t value16bb;
            };
        };
    };
};

/*!
 * A control point interface that begins a control point specific data type.
 */
struct __wt_control_point {
    WT_CONTROL_POINT_PARAM param1;          /* First parameter for pred function. */
    WT_CONTROL_POINT_PARAM param2;          /* Second parameter for pred function. */
    wt_control_point_ref_count_t ref_count; /* Count of threads using this data. */
};
