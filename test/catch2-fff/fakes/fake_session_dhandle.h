#pragma once

#include <fff.h>

#include "wt_internal.h"

extern "C" {
DECLARE_FAKE_VALUE_FUNC(int, __wt_session_get_dhandle, WT_SESSION_IMPL *, const char *,
  const char *, const char **, uint32_t);

DECLARE_FAKE_VALUE_FUNC(int, __wt_session_release_dhandle, WT_SESSION_IMPL *);
}

void reset_session_dhandle_fakes();
