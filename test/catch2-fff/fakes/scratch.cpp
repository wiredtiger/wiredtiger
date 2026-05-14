#include "scratch.h"

#include <fff.h>

extern "C" {
DEFINE_FAKE_VALUE_FUNC(int, __wt_buf_set, WT_SESSION_IMPL *, WT_ITEM *, const void *, size_t);
DEFINE_FAKE_VOID_FUNC(__wt_buf_free, WT_SESSION_IMPL *, WT_ITEM *);
}

void
reset_scratch_fakes()
{
    RESET_FAKE(__wt_buf_set);
    RESET_FAKE(__wt_buf_free);
    FFF_RESET_HISTORY();
}
