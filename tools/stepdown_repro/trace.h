/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "wt_internal.h"
#include "../../src/cursor/cur_layered_private.h"
#include "../../src/reconcile/reconcile_private.h"
#include "../../src/reconcile/reconcile_inline.h"

void repro_trace_open(const char *home, const char *mode);
void repro_trace_close(void);
void repro_checkpoint_gate(unsigned gate);
void repro_checkpoint_wait(void);
void repro_checkpoint_release(void);
void repro_failure_arm(void);
void repro_stage(const char *stage);
void repro_note(WT_SESSION_IMPL *session, const char *event, WT_REF *ref, uint64_t detail);
unsigned repro_failures(void);
