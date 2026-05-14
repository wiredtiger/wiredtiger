#include "mtx_rw.h"

#include <fff.h>

extern "C" {
DEFINE_FAKE_VOID_FUNC(__wt_writelock, WT_SESSION_IMPL *, WT_RWLOCK *);
DEFINE_FAKE_VOID_FUNC(__wt_writeunlock, WT_SESSION_IMPL *, WT_RWLOCK *);
}

void
reset_mtx_rw_fakes()
{
    RESET_FAKE(__wt_writelock);
    RESET_FAKE(__wt_writeunlock);
    FFF_RESET_HISTORY();
}
