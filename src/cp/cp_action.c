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
 * Some actions (only "Wait for trigger" so far) have additional functions. These functions are
 * named __wt_control_point_*_example_action.
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
 * Action: ERR: Change the control flow to trigger an error condition via WT_ERR.
 */
/* Action config parsing function. */
/*
 * __wt_control_point_config_action_err --
 *     Configuration parsing for control point action "ERR: Change the control flow to trigger an
 *     error condition".
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_action_err(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_PAIR_DATA_ERR *data, WT_CONFIG_ITEM *item)
{
    WT_CONFIG_ITEM cval;
    WT_CONTROL_POINT_ACTION_ERR *action_data;

    action_data = &data->action_data;
    WT_RET(__wt_config_subgets(session, item, "error", &cval));
    action_data->err = (int)cval.val;
    return (0);
}

/*
 * Action: RET: Return an error via WT_RET.
 */
/* Action config parsing function. */
/*
 * __wt_control_point_config_action_ret --
 *     Configuration parsing for control point action "RET: Return an error".
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_action_ret(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_PAIR_DATA_RET *data, WT_CONFIG_ITEM *item)
{
    WT_CONFIG_ITEM cval;
    WT_CONTROL_POINT_ACTION_RET *action_data;

    action_data = &data->action_data;
    WT_RET(__wt_config_subgets(session, item, "return", &cval));
    action_data->ret_value = (int)cval.val;
    return (0);
}

/*
 * Action: Wait for trigger: Blocking the testing thread until a control point is triggered.
 */
#define WT_DELAY_UNTIL_TRIGGERED_USEC (10 * WT_THOUSAND) /* 10 milliseconds */

/* Action config parsing function. */
/*
 * __wt_control_point_config_action_wait_for_trigger --
 *     Configuration parsing for control point action "Wait until trigger: Blocking the testing
 *     thread until a control point is triggered".
 *
 * @param session The session. @param data Return the parsed data in here. @param cfg The
 *     configuration strings.
 */
int
__wt_control_point_config_action_wait_for_trigger(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_PAIR_DATA_WAIT_FOR_TRIGGER *data, WT_CONFIG_ITEM *item)
{
    WT_CONFIG_ITEM cval;
    WT_RET(__wt_config_subgets(session, item, "wait_count", &cval));
    data->action_data.wait_count = (uint64_t)cval.val;
    return (0);
}

/* Functions used at the call site. */
/*
 * __run_wait_for_trigger --
 *     The run function for __wt_cond_wait_signal for the call site portion of control point action
 *     "Wait until trigger: Blocking the testing thread until a control point is triggered".
 *
 * @param session The session.
 */
static bool
__run_wait_for_trigger(WT_SESSION_IMPL *session)
{
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER *action_data;
    WT_CONTROL_POINT_REGISTRY *cp_registry;

    cp_registry = session->cp_registry;
    action_data = &(session->cp_data->action_data);
    return (cp_registry->trigger_count >= action_data->desired_trigger_count);
}

/*
 * __wt_control_point_wait_for_trigger --
 *     The call site portion of control point action "Wait until trigger: Blocking the testing
 *     thread until a control point is triggered" given a WT_CONTROL_POINT_REGISTRY. Return true if
 *     triggered.
 *
 * @param session The session. @param cp_registry The control point's registry.
 */
bool
__wt_control_point_wait_for_trigger(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry, wt_control_point_id_t id)
{
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER *action_data;
    WT_CONTROL_POINT_DATA *cp_data;
    WT_CONTROL_POINT_PAIR_DATA_WAIT_FOR_TRIGGER *pair_data;
    size_t crossing_count;
    size_t current_trigger_count;
    size_t desired_trigger_count;
    size_t start_trigger_count;
    size_t wait_count;
    bool signalled;

    cp_data = __wti_control_point_get_data(session, cp_registry, true);
    start_trigger_count = cp_registry->trigger_count;
    if (WT_UNLIKELY(cp_data == NULL)) {
        __wt_verbose_debug5(session, WT_VERB_CONTROL_POINT,
          "False: Is disabled: wait for trigger skipped: id=%" PRId32, id);
        return (false); /* Not enabled. */
    }
    /* Does the call site and trigger site match in action? */
    if (WT_UNLIKELY(cp_registry->action_supported != WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER)) {
        __wt_verbose_error(session, WT_VERB_CONTROL_POINT,
          "False: Control point call site and trigger site have different actions: id=%" PRId32
          ": %d and %" PRIu32 ".",
          id, WT_CONTROL_POINT_ACTION_ID_WAIT_FOR_TRIGGER, cp_registry->action_supported);
        return (false); /* Pretend not enabled. */
    }
    /* Is waiting necessary? */
    pair_data = (WT_CONTROL_POINT_PAIR_DATA_WAIT_FOR_TRIGGER *)cp_data;
    action_data = &pair_data->action_data;
    wait_count = action_data->wait_count;
    desired_trigger_count = start_trigger_count + wait_count;
    current_trigger_count = cp_registry->trigger_count;
    crossing_count = cp_registry->crossing_count;
    if (current_trigger_count >= desired_trigger_count) { /* No */
        __wt_control_point_release_data(session, cp_registry, cp_data, true);
        __wt_verbose_debug2(session, WT_VERB_CONTROL_POINT,
          "True: Wait not needed: id=%" PRId32 ", # to wait=%" PRIu64 ", # waited=%" PRIu64
          ", trigger_count=%" PRIu64 ", crossing_count=%" PRIu64,
          id, (uint64_t)wait_count, (uint64_t)(current_trigger_count - start_trigger_count),
          (uint64_t)current_trigger_count, (uint64_t)crossing_count);
        return (true); /* Enabled and wait fulfilled. */
    }
    /* Store data needed by run_func. */
    action_data->desired_trigger_count = desired_trigger_count;
    session->cp_registry = cp_registry;
    session->cp_data = pair_data;
    __wt_control_point_unlock(session, cp_registry);
    __wt_verbose_debug4(session, WT_VERB_CONTROL_POINT,
      "Waiting: id=%" PRId32 ", # left to wait=%" PRIu64 ", # waited=%" PRIu64
      ", trigger_count=%" PRIu64 ", crossing_count=%" PRIu64,
      id, (uint64_t)(desired_trigger_count - current_trigger_count),
      (uint64_t)(current_trigger_count - start_trigger_count), (uint64_t)current_trigger_count,
      (uint64_t)crossing_count);
    for (;;) {
        __wt_cond_wait_signal(session, action_data->condvar, WT_DELAY_UNTIL_TRIGGERED_USEC,
          __run_wait_for_trigger, &signalled);
        current_trigger_count = cp_registry->trigger_count;
        if (current_trigger_count >= desired_trigger_count)
            /* Delay condition satisfied. */
            break;
    }
    crossing_count = cp_registry->crossing_count;
    __wt_control_point_release_data(session, cp_registry, cp_data, false);
    __wt_verbose_debug2(session, WT_VERB_CONTROL_POINT,
      "True: Wait finished: id=%" PRId32 ", # to wait=%" PRIu64 ", # waited=%" PRIu64
      ", trigger_count=%" PRIu64 ", crossing_count=%" PRIu64,
      id, (uint64_t)wait_count, (uint64_t)(current_trigger_count - start_trigger_count),
      (uint64_t)current_trigger_count, (uint64_t)crossing_count);
    return (true);
}

/* Extra initialization. */
/*
 * __wt_control_point_action_init_wait_for_trigger --
 *     Extra initialization required for action "Wait until trigger: Blocking the testing thread
 *     until a control point is triggered".
 *
 * @param session The session. @param control_point_name The name of the control point. @param data
 *     The control point's data.
 */
void
__wt_control_point_action_init_wait_for_trigger(WT_SESSION_IMPL *session,
  const char *control_point_name, WT_CONTROL_POINT_PAIR_DATA_WAIT_FOR_TRIGGER *data)
{
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER *action_data;
    WT_DECL_RET;

    action_data = &data->action_data;
    ret = __wt_cond_alloc(session, control_point_name, &(action_data->condvar));
    WT_ASSERT(session, ret == 0);
}

#endif /* HAVE_CONTROL_POINT */
