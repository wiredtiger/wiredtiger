#include "os_alloc.h"

#include <fff.h>

extern "C" {
DEFINE_FAKE_VALUE_FUNC(int, __wt_calloc, WT_SESSION_IMPL *, size_t, size_t, void *);
}

void
reset_os_alloc_fakes()
{
    RESET_FAKE(__wt_calloc);
    FFF_RESET_HISTORY();
}
