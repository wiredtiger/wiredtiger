/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifdef HAVE_CONTROL_POINTS

/* cp_action.c: Definitions for control point actions. */
/* This file must be edited when a new control point action is created. */

/*!
 * Configuration parsing for control point action "Delay at a specific code location during an
 * execution".
 */
int
__wt_control_point_config_action_sleep(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **configuration)
{
    /* TODO. */
}

/*!
 * Configuration parsing for control point action "Change the control flow to trigger an error
 * condition".
 */
int
__wt_control_point_config_action_err(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **configuration)
{
    /* TODO. */
    return (-1); /* For compiler */
}

/*!
 * Configuration parsing for control point action "Return an error".
 */
int
__wt_control_point_config_action_ret(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **configuration)
{
    /* TODO. */
    return (-1); /* For compiler */
}

/*!
 * Configuration parsing for control point action "Blocking the testing thread until a control point
 * is triggered".
 */
int
__wt_control_point_config_action_wait_for_trigger(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT *data, const char **configuration)
{
    /* TODO. */
    return (-1); /* For compiler */
}

/*!
 * The run function for __wt_cond_wait_signal for the call site portion of control point action
 * "Blocking the testing thread until a control point is triggered".
 */
bool
__wt_control_point_run_wait_for_trigger(WT_SESSION_IMPL *session)
{
    WT_CONTROL_POINT_REGISTRY *cp_registry = session->cp_registry;
    WT_CONTROL_POINT_ACTION_WAIT_UNTIL_TRIGGERED *action_data =
      (WT_CONTROL_POINT_ACTION_WAIT_UNTIL_TRIGGERED *)(session->cp_data + 1);
    return (cp_registry->trigger_count >= action_data->desired_trigger_count);
}

/*!
 * The call site portion of control point action "Blocking the testing thread until a control point
 * is triggered" given a WT_CONTROL_POINT_REGISTRY. Return true if triggered.
 */
bool
__wt_control_point_wait_for_trigger(
  WT_SESSION_IMPL *session, WT_CONTROL_POINT_REGISTRY *cp_registry)
{
    size_t start_trigger_count = cp_registry->trigger_count;
    size_t desired_trigger_count;
    WT_CONTROL_POINT *data = __wt_control_point_get_data(session, cp_registry, true);
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER *action_data;
    if (WT_UNLIKELY(data == NULL))
        return (false); /* Not enabled. */
    /* Is waiting necessary? */
    action_data = (WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER *)(data + 1);
    desired_trigger_count = start_trigger_count + action_data->wait_count;
    if (cp_registry->trigger_count >= desired_trigger_count) { /* No */
        __wt_release_data(session, data, true);
        return (true); /* Enabled and wait fulfilled. */
    }
    /* Store data needed by run_func. */
    action_data->desired_trigger_count = desired_trigger_count;
    session->cp_registry = cp_registry;
    session->cp_data = data;
    __wt_control_point_unlock(cp_registry);
    for (;;) {
        __wt_cond_wait_signal(session, action_data->condvar, WT_DELAY_UNTIL_TRIGGERED_USEC,
          __wt_control_point_run_wait_for_trigger);
        if (cp_registry->trigger_count >= desired_trigger_count)
            /* Delay condition satisfied */
            break;
    }
    __wt_release_data(session, data, false);
    return (true);
}

void
__wt_control_point_action_init_wait_for_trigger(
  WT_SESSION_IMPL *session, const char *control_point_name, WT_CONTROL_POINT *data)
{
    WT_RET_DECL;
    WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER *action_data =
      (WT_CONTROL_POINT_ACTION_WAIT_FOR_TRIGGER *)(data + 1);
    ret = __wt_cond_alloc(session, control_point_name, &(action_data->condvar));
    return (ret);
}

#endif /* HAVE_CONTROL_POINTS */
