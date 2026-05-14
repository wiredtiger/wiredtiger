#pragma once

#include <fff.h>

#include "wt_internal.h"

extern "C" {
DECLARE_FAKE_VOID_FUNC(__wt_writelock, WT_SESSION_IMPL *, WT_RWLOCK *);
DECLARE_FAKE_VOID_FUNC(__wt_writeunlock, WT_SESSION_IMPL *, WT_RWLOCK *);
}

void reset_mtx_rw_fakes();
