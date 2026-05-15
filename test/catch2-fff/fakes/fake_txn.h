#pragma once

#include <fff.h>

#include "wt_internal.h"

extern "C" {
DECLARE_FAKE_VALUE_FUNC(int, __wt_txn_next_op, WT_SESSION_IMPL *, WT_TXN_OP **);
}

void reset_txn_fakes();
