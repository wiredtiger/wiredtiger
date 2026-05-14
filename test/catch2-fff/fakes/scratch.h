#pragma once

#include <fff.h>

#include "wt_internal.h"

extern "C" {
DECLARE_FAKE_VALUE_FUNC(int, __wt_buf_set, WT_SESSION_IMPL *, WT_ITEM *, const void *, size_t);
DECLARE_FAKE_VOID_FUNC(__wt_buf_free, WT_SESSION_IMPL *, WT_ITEM *);
}

void reset_scratch_fakes();
