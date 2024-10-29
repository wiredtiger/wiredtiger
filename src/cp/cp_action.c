/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#ifdef HAVE_CONTROL_POINT

/* cp_action.c: Definitions for control point actions. */
/* This file must be edited when a new control point action is created. */

/*
 * The names below are for an action named "Example action".
 *
 * Each action has:
 * - Action config parsing function (Must be manual):
 * __wt_control_point_config_action_example_action().
 *
 * Some actions (only "Trigger" and "Thread Barrier" so far) have additional functions. These
 * functions are named __wt_control_point_*_example_action.
 */

/*
 * __wt_verbose levels used in WT_VERB_CONTROL_POINT logging.
 * -3 ERROR - Internal error in control point framework.
 * -2 WARNING - Recoverable internal error in control point framework.
 * -1 NOTICE: Not used.
 * 0 INFO - Not used.
 * 1 DEBUG_1 - When triggered.
 * 2 DEBUG_2 - Done waiting.
 * 3 DEBUG_3 - When not triggered.
 * 4 DEBUG_4 - Start waiting, Other details.
 * 5 DEBUG_5 - When disabled.
 */

/*
 * Action: Sleep: Delay at a specific code location during an execution via __wt_sleep.
 */
/* Action config parsing function. */
/*
 * __wt_control_point_config_action_sleep --
 *     Configuration parsing for control point action "Sleep: Delay at a specific code location
 *     during an execution".
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_action_sleep(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_PAIR_DATA_SLEEP *data, WT_CONFIG_ITEM *item)
{
    WT_CONFIG_ITEM cval;
    WT_CONTROL_POINT_ACTION_SLEEP *action_data;

    action_data = &data->action_data;
    WT_RET(__wt_config_subgets(session, item, "seconds", &cval));
    action_data->seconds = (uint64_t)cval.val;
    WT_RET(__wt_config_subgets(session, item, "microseconds", &cval));
    action_data->microseconds = (uint64_t)cval.val;
    return (0);
}

/*
 * Action: Trigger: Block the testing thread until a control point is triggered.
 */
#define WT_DELAY_UNTIL_TRIGGERED_USEC (10 * WT_THOUSAND) /* 10 milliseconds */

/* Action config parsing function. */
/*
 * __wt_control_point_config_action_trigger --
 *     Configuration parsing for control point action "Trigger: Block the testing thread until a
 *     control point is triggered".
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_action_trigger(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_PAIR_DATA_TRIGGER *data, WT_CONFIG_ITEM *item)
{
    WT_CONFIG_ITEM cval;
    WT_RET(__wt_config_subgets(session, item, "wait_count", &cval));
    data->action_data.wait_count = (uint64_t)cval.val;
    return (0);
}

/* Functions used at the call site. */
/*
 * __run_trigger --
 *     The run function for __wt_cond_wait_signal for the call site portion of control point action
 *     "Trigger: Block the testing thread until a control point is triggered".
 *
 * @param session The session.
 */
static bool
__run_trigger(WT_SESSION_IMPL *session)
{
    WT_CONTROL_POINT_ACTION_TRIGGER *action_data;
    WT_CONTROL_POINT_REGISTRY *cp_registry;

    cp_registry = session->cp_registry;
    action_data = &(((WT_CONTROL_POINT_PAIR_DATA_TRIGGER *)session->cp_data)->action_data);
    return (cp_registry->trigger_count >= action_data->desired_trigger_count);
}

/*
 * __wt_control_point_wait --
 *     The call site portion of control point action "Trigger: Block the testing thread until a
 *     control point is triggered" given a WT_CONTROL_POINT_REGISTRY. Return true if triggered.
 *
 * @param session The session. @param cp_registry The control point's registry.
 */
void
__wt_control_point_wait(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, wt_control_point_id_t id)
{
    WT_CONTROL_POINT_ACTION_TRIGGER *action_data;
    WT_CONTROL_POINT_DATA *cp_data;
    WT_CONTROL_POINT_PAIR_DATA_TRIGGER *pair_data;
    size_t crossing_count;
    size_t current_trigger_count;
    size_t desired_trigger_count;
    size_t start_trigger_count;
    size_t wait_count;
    bool signalled;

    WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
    cp_data = __wti_control_point_get_data(session, cp_registry, true);
    start_trigger_count = cp_registry->trigger_count;
    if (WT_UNLIKELY(cp_data == NULL)) {
        WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
        __wt_verbose_debug5(session, WT_VERB_CONTROL_POINT,
          "%s: False: Is disabled: trigger skipped: id=%" PRId32, __func__, id);
        return; /* Not enabled. */
    }
    WT_ASSERT(session, __wt_spin_owned(session, &cp_registry->lock));
    /* Does the call site and trigger site match in action? */
    if (WT_UNLIKELY(cp_registry->action_supported != WT_CONTROL_POINT_ACTION_ID_TRIGGER)) {
        __wt_control_point_release_data(session, cp_registry, cp_data, true);
        WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
        __wt_verbose_error(session, WT_VERB_CONTROL_POINT,
          "%s: False: Control point call site and trigger site have different actions: id=%" PRId32
          ": %d and %" PRIu32 ".",
          __func__, id, WT_CONTROL_POINT_ACTION_ID_TRIGGER, cp_registry->action_supported);
        return; /* Pretend not enabled. */
    }
    /* Is waiting necessary? */
    pair_data = (WT_CONTROL_POINT_PAIR_DATA_TRIGGER *)cp_data;
    action_data = &pair_data->action_data;
    wait_count = action_data->wait_count;
    desired_trigger_count = start_trigger_count + wait_count;
    current_trigger_count = cp_registry->trigger_count;
    crossing_count = cp_registry->crossing_count;
    if (current_trigger_count >= desired_trigger_count) { /* No */
        __wt_control_point_release_data(session, cp_registry, cp_data, true);
        WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
        __wt_verbose_debug2(session, WT_VERB_CONTROL_POINT,
          "%s: True: Wait not needed: id=%" PRId32 ", # to wait=%" PRIu64 ", # waited=%" PRIu64
          ", trigger_count=%" PRIu64 ", crossing_count=%" PRIu64,
          __func__, id, (uint64_t)wait_count,
          (uint64_t)(current_trigger_count - start_trigger_count), (uint64_t)current_trigger_count,
          (uint64_t)crossing_count);
        return; /* Enabled and wait fulfilled. */
    }
    /* Store data needed by run_func. */
    action_data->desired_trigger_count = desired_trigger_count;
    session->cp_registry = cp_registry;
    session->cp_data = &(pair_data->iface);
    __wt_control_point_unlock(session, cp_registry);
    WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
    __wt_verbose_debug4(session, WT_VERB_CONTROL_POINT,
      "%s: Waiting: id=%" PRId32 ", # left to wait=%" PRIu64 ", # waited=%" PRIu64
      ", trigger_count=%" PRIu64 ", crossing_count=%" PRIu64,
      __func__, id, (uint64_t)(desired_trigger_count - current_trigger_count),
      (uint64_t)(current_trigger_count - start_trigger_count), (uint64_t)current_trigger_count,
      (uint64_t)crossing_count);

    /* Wait for enough triggers. */
    for (;;) {
        /* Wait for cp_registry->trigger_count >= action_data->desired_trigger_count. */
        __wt_cond_wait_signal(
          session, action_data->condvar, WT_DELAY_UNTIL_TRIGGERED_USEC, __run_trigger, &signalled);

        current_trigger_count = cp_registry->trigger_count;
        if (current_trigger_count >= desired_trigger_count)
            /* Delay condition satisfied. */
            break;
    }
    crossing_count = cp_registry->crossing_count;
    __wt_control_point_release_data(session, cp_registry, cp_data, false);
    WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
    __wt_verbose_debug2(session, WT_VERB_CONTROL_POINT,
      "%s: True: Wait finished: id=%" PRId32 ", # to wait=%" PRIu64 ", # waited=%" PRIu64
      ", trigger_count=%" PRIu64 ", crossing_count=%" PRIu64,
      __func__, id, (uint64_t)wait_count, (uint64_t)(current_trigger_count - start_trigger_count),
      (uint64_t)current_trigger_count, (uint64_t)crossing_count);
    return; /* Enabled and wait finished. */
}

/* Extra initialization. */
/*
 * __wt_control_point_action_init_trigger --
 *     Extra initialization required for action "Trigger: Block the testing thread until a control
 *     point is triggered".
 *
 * @param session The session. @param control_point_name The name of the control point. @param data
 *     The control point's data.
 */
void
__wt_control_point_action_init_trigger(WT_SESSION_IMPL *session, const char *control_point_name,
  WT_CONTROL_POINT_PAIR_DATA_TRIGGER *data)
{
    WT_CONTROL_POINT_ACTION_TRIGGER *action_data;
    WT_DECL_RET;

    action_data = &data->action_data;
    ret = __wt_cond_alloc(session, control_point_name, &(action_data->condvar));
    WT_ASSERT(session, ret == 0);
}

/*
 * Action: Thread Barrier: Block the testing thread(s) and define thread until a control point is
 * triggered.
 */
#define WT_DELAY_UNTIL_BARRIER_USEC (10 * WT_THOUSAND) /* 10 milliseconds */

/* Action config parsing function. */
/*
 * __wt_control_point_config_action_thread_barrier --
 *     Configuration parsing for control point action "Thread Barrier: Block the testing thread(s)
 *     and define thread until a control point is triggered".
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_action_thread_barrier(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_PAIR_DATA_THREAD_BARRIER *data, WT_CONFIG_ITEM *item)
{
    WT_CONFIG_ITEM cval;
    WT_RET(__wt_config_subgets(session, item, "thread_count", &cval));
    data->action_data.thread_count = (uint64_t)cval.val;
    return (0);
}

/* Functions used at the call site. */
/*
 * __run_thread_barrier --
 *     The run function for __wt_cond_wait_signal for the call site portion of control point action
 *     "Thread Barrier: Block the testing thread(s) and define thread until a control point is
 *     triggered".
 *
 * @param session The session.
 */
static bool
__run_thread_barrier(WT_SESSION_IMPL *session)
{
    WT_CONTROL_POINT_ACTION_THREAD_BARRIER *action_data;

    action_data = &(((WT_CONTROL_POINT_PAIR_DATA_THREAD_BARRIER *)session->cp_data)->action_data);
    return (action_data->num_threads_waiting == action_data->thread_count);
}

/*
 * __wt_control_point_wait_thread_barrier --
 *     The call site portion of control point action "Thread Barrier: Block the testing thread(s)
 *     and define thread until a control point is triggered" given a WT_CONTROL_POINT_REGISTRY.
 *     Return true if triggered.
 *
 * If define, already locked and get_data done. Return locked.
 *
 * If not define, not locked, get_data not done. Call get_data. Return not get_data, and not locked.
 *
 * @param session The session. @param cp_registry The control point's registry.
 */
void
__wt_control_point_wait_thread_barrier(WT_SESSION_IMPL *session,
  WT_CONTROL_POINT_REGISTRY *cp_registry, wt_control_point_id_t id, bool define)
{
    WT_CONTROL_POINT_ACTION_THREAD_BARRIER *action_data;
    WT_CONTROL_POINT_DATA *cp_data;
    WT_CONTROL_POINT_PAIR_DATA_THREAD_BARRIER *pair_data;
    size_t crossing_count;
    size_t current_trigger_count;
    uint64_t num_threads_waiting;
    uint64_t num_threads_woke_up;
    uint64_t thread_count;
    bool signalled;

    if (define) {
        WT_ASSERT(session, __wt_spin_owned(session, &cp_registry->lock));
        cp_data = cp_registry->cp_data;
    } else {
        WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
        cp_data = __wti_control_point_get_data(session, cp_registry, true);
    }

    if (WT_UNLIKELY(cp_data == NULL)) {
        WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
        __wt_verbose_debug5(session, WT_VERB_CONTROL_POINT,
          "%s: False: Is disabled: trigger skipped: id=%" PRId32 ", %s.", __func__, id,
          define ? "define" : "call");
        return; /* Not enabled. */
    }

    WT_ASSERT(session, __wt_spin_owned(session, &cp_registry->lock));
    /* Does the call site and trigger site match in action? */
    if (WT_UNLIKELY(cp_registry->action_supported != WT_CONTROL_POINT_ACTION_ID_THREAD_BARRIER)) {
        if (!define) {
            __wt_control_point_release_data(session, cp_registry, cp_data, true);
            WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
        }
        __wt_verbose_error(session, WT_VERB_CONTROL_POINT,
          "%s: False: Control point call site and trigger site have different actions: id=%" PRId32
          ": %d and %" PRIu32 ", %s.",
          __func__, id, WT_CONTROL_POINT_ACTION_ID_THREAD_BARRIER, cp_registry->action_supported,
          define ? "define" : "call");
        return; /* Pretend not enabled. */
    }

    pair_data = (WT_CONTROL_POINT_PAIR_DATA_THREAD_BARRIER *)cp_data;
    action_data = &pair_data->action_data;
    thread_count = action_data->thread_count;
    num_threads_waiting = action_data->num_threads_waiting;
    action_data->num_threads_waiting = ++num_threads_waiting;
    WT_ASSERT(session, num_threads_waiting <= 2 * thread_count);
    num_threads_woke_up = action_data->num_threads_woke_up;
    current_trigger_count = cp_registry->trigger_count;
    crossing_count = cp_registry->crossing_count;

    /* Is waiting necessary? */
    if (num_threads_waiting == thread_count) { /* No */
        WT_ASSERT(session, num_threads_woke_up < thread_count);
        action_data->num_threads_woke_up = ++num_threads_woke_up;

        __wt_control_point_unlock(session, cp_registry);
        WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));

        __wt_verbose_debug2(session, WT_VERB_CONTROL_POINT,
          "%s: True: Wait not needed: id=%" PRId32 ", thread_count=%" PRIu64 ", # waiting=%" PRIu64
          ", # woke up=%" PRIu64 ", trigger_count=%" PRIu64 ", crossing_count=%" PRIu64 ", %s.",
          __func__, id, thread_count, num_threads_waiting, num_threads_woke_up,
          (uint64_t)current_trigger_count, (uint64_t)crossing_count, define ? "define" : "call");

        __wt_cond_signal(session, action_data->condvar);

        __wti_control_point_relock(session, cp_registry, &(pair_data->iface));
        WT_ASSERT(session, __wt_spin_owned(session, &cp_registry->lock));

        /* Enabled and all threads have arrived. */
        goto check_reset;
    }

    /* Store data needed by run_func. */
    session->cp_registry = cp_registry;
    session->cp_data = &(pair_data->iface);

    __wt_control_point_unlock(session, cp_registry);
    WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));

    __wt_verbose_debug4(session, WT_VERB_CONTROL_POINT,
      "%s: Waiting: id=%" PRId32 ", # left to wait=%" PRIu64 ", # waiting=%" PRIu64
      ", # woke up=%" PRIu64 ", trigger_count=%" PRIu64 ", crossing_count=%" PRIu64 ", %s.",
      __func__, id, (thread_count - num_threads_waiting), num_threads_waiting, num_threads_woke_up,
      (uint64_t)current_trigger_count, (uint64_t)crossing_count, define ? "define" : "call");

    /* Wait for all threads to wait. */
    for (;;) {
        WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
        /* Wait for action_data->num_threads_waiting == action_data->thread_count. */
        __wt_cond_wait_signal(session, action_data->condvar, WT_DELAY_UNTIL_BARRIER_USEC,
          __run_thread_barrier, &signalled);

        __wti_control_point_relock(session, cp_registry, &(pair_data->iface));
        WT_ASSERT(session, __wt_spin_owned(session, &cp_registry->lock));

        if (action_data->num_threads_waiting == thread_count) {
            /* Delay condition satisfied. */
            break;
        }

        __wt_control_point_unlock(session, cp_registry);
    }
    WT_ASSERT(session, __wt_spin_owned(session, &cp_registry->lock));

    num_threads_woke_up = ++(action_data->num_threads_woke_up);
    current_trigger_count = cp_registry->trigger_count;
    crossing_count = cp_registry->crossing_count;

    __wt_verbose_debug2(session, WT_VERB_CONTROL_POINT,
      "%s: True: Wait finished: id=%" PRId32 ", # to wait=%" PRIu64 ", # waiting=%" PRIu64
      ", # woke up=%" PRIu64 ", trigger_count=%" PRIu64 ", crossing_count=%" PRIu64 ", %s.",
      __func__, id, thread_count, num_threads_waiting, num_threads_woke_up,
      (uint64_t)current_trigger_count, (uint64_t)crossing_count, define ? "define" : "call");

check_reset:
    WT_ASSERT(session, __wt_spin_owned(session, &cp_registry->lock));

    if (num_threads_woke_up == thread_count) {
        /* All threads woke up. Reset for next use. */
        num_threads_waiting = action_data->num_threads_waiting;
        WT_ASSERT(session, num_threads_waiting >= thread_count);
        num_threads_waiting -= thread_count;
        action_data->num_threads_waiting = num_threads_waiting;

        num_threads_woke_up = action_data->num_threads_woke_up;
        WT_ASSERT(session, num_threads_woke_up >= thread_count);
        num_threads_woke_up -= thread_count;
        action_data->num_threads_woke_up = num_threads_woke_up;

        __wt_verbose_debug2(session, WT_VERB_CONTROL_POINT,
          "%s: Wake up finished, reset: id=%" PRId32 ", # waiting=%" PRIu64 ", # woke up=%" PRIu64
          ", %s.",
          __func__, id, num_threads_waiting, num_threads_woke_up, define ? "define" : "call");

        /* This thread was just waked up. It has not had a chance to wait again. Thus
         * num_threads_waiting < thread_count. */
        WT_ASSERT(session, num_threads_waiting < thread_count);
    }

    if (!define) {
        __wt_control_point_release_data(session, cp_registry, cp_data, true);
        WT_ASSERT(session, !__wt_spin_owned(session, &cp_registry->lock));
    }
    return; /* Enabled and wait finished. */
}

/* Extra initialization. */
/*
 * __wt_control_point_action_init_thread_barrier --
 *     Extra initialization required for action "Thread Barrier: Block the testing thread(s) and
 *     define thread until a control point is triggered".
 *
 * @param session The session. @param control_point_name The name of the control point. @param data
 *     The control point's data.
 */
void
__wt_control_point_action_init_thread_barrier(WT_SESSION_IMPL *session,
  const char *control_point_name, WT_CONTROL_POINT_PAIR_DATA_THREAD_BARRIER *data)
{
    WT_CONTROL_POINT_ACTION_THREAD_BARRIER *action_data;
    WT_DECL_RET;

    action_data = &data->action_data;
    ret = __wt_cond_alloc(session, control_point_name, &(action_data->condvar));
    WT_ASSERT(session, ret == 0);
}

#endif /* HAVE_CONTROL_POINT */
