#pragma once

#include <fff.h>

#include "wt_internal.h"

extern "C" {
DECLARE_FAKE_VALUE_FUNC(int, __wt_calloc, WT_SESSION_IMPL *, size_t, size_t, void *);
}

void reset_os_alloc_fakes();
