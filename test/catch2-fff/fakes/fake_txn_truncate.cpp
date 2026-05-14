#include "fake_txn_truncate.h"

extern "C" {
DEFINE_FAKE_VALUE_FUNC(int, __wt_layered_table_truncate_detect_write_conflict, WT_SESSION_IMPL *,
  WT_LAYERED_TABLE *, const WT_ITEM *);
}

void
reset_txn_truncate_fakes()
{
    RESET_FAKE(__wt_layered_table_truncate_detect_write_conflict);
    FFF_RESET_HISTORY();
}
