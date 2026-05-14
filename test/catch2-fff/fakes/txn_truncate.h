#pragma once

#include <fff.h>

#include "wt_internal.h"

extern "C" {
DECLARE_FAKE_VALUE_FUNC(int, __wt_layered_table_truncate_detect_write_conflict, WT_SESSION_IMPL *,
  WT_LAYERED_TABLE *, const WT_ITEM *);
}

void reset_txn_truncate_fakes();
