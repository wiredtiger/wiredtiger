#include "fake_txn.h"

extern "C" {
DEFINE_FAKE_VALUE_FUNC(int, __wt_txn_next_op, WT_SESSION_IMPL *, WT_TXN_OP **);
}

void
reset_txn_fakes()
{
    RESET_FAKE(__wt_txn_next_op);
    FFF_RESET_HISTORY();
}
