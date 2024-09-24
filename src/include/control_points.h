/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* control_points.h: Declarations for control points. */

#ifdef HAVE_CONTROL_POINTS

#if defined(DOXYGEN) || defined(SWIG)
#define __F(func) func
#else
/* NOLINTNEXTLINE(misc-macro-parentheses) */
#define __F(func) (*func)
#endif

/*!
 * Identifies a per connection control point or a per session control point.
 *
 * A WT_CONTROL_POINT_ID does not specify whether a control point is
 * a per connection control point or a per session control point since both
 * start numbering at 0.
 *
 * The maximum per connection control point ID is CONNECTION_CONTROL_POINTS_SIZE - 1.
 * The maximum per session control point ID is SESSION_CONTROL_POINTS_SIZE - 1.
 */
typedef int32_t WT_CONTROL_POINT_ID;

/*!
 * Identifies a control point action.
 * Used to verify compatibility between the code at a
 * control point call site and at the control point trigger site.
 */
typedef uint32_t WT_CONTROL_POINT_ACTION_ID;

/*!
 * A function to initialize a control point's data.
 */
typedef void wt_control_point_init_t(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *registration, WT_CONTROL_POINT_ID id);

/*!
 * A function to test whether a control point should be triggered.
 */
typedef bool wt_control_point_pred_t(WT_SESSION_IMPL *session, WT_CONTROL_POINT *);

/*!
 * Registration data for one control point.
 */
struct __wt_control_point_registry;
typedef struct __wt_control_point_registry WT_CONTROL_POINT_REGISTRY;
struct __wt_control_point_registry {
    wt_control_point_init_t __F(init); /* Function to initialize the control point. */
    wt_control_point_pred_t __F(pred); /* Function to test whether to trigger. */
    WT_SPINLOCK lock;                  /* Atomically access data and data->ref_count. */
    WT_CONTROL_POINT *data; /* Disabled if NULL. More data may follow WT_CONTROL_POINT. */
    WT_CONTROL_POINT_ACTION_ID action_supported; /* For compatibility checking. */
};

/*!
 * A reference count for a WT_CONTROL_POINT.
 * This is needed by only per connection control points, not per session control points.
 */
typedef uint32_t WT_CONTROL_POINT_REF_COUNT;

/*!
 * A pred parameter in WT_CONTROL_POINT.
 * The usage and meaning depends upon the pred function.
 */
union __wt_control_point_param;
typedef union __wt_control_point_param WT_CONTROL_POINT_PARAM;
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
struct __wt_control_point;
typedef struct __wt_control_point WT_CONTROL_POINT;
struct __wt_control_point {
    WT_CONTROL_POINT_PARAM param1;        /* First parameter for pred function. */
    WT_CONTROL_POINT_PARAM param2;        /* Second parameter for pred function. */
    size_t crossing_count;                /* Count of executions of the trigger site. */
    size_t trigger_count;                 /* Count of triggers, i.e. pred returned true. */
    WT_CONTROL_POINT_REF_COUNT ref_count; /* Count of threads using this data. */
};

/* The control points API. */
int __wt_conn_control_point_disable(WT_SESSION *session, WT_CONTROL_POINT_ID id);
int __wt_session_control_point_disable(WT_SESSION *session, WT_CONTROL_POINT_ID id);
int __wt_conn_control_point_enable(WT_SESSION *session, WT_CONTROL_POINT_ID id);
int __wt_session_control_point_enable(WT_SESSION *session, WT_CONTROL_POINT_ID id);

#endif /* HAVE_CONTROL_POINTS */
