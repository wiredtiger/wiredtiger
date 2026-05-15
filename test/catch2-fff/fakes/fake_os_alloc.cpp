#include "fake_os_alloc.h"

#include <cerrno>
#include <cstdlib>

extern "C" {
DEFINE_FAKE_VALUE_FUNC(int, __wt_calloc, WT_SESSION_IMPL *, size_t, size_t, void *);
}

static int
calloc_passthrough(WT_SESSION_IMPL *, size_t count, size_t size, void *retp)
{
    auto *p = std::calloc(count, size);
    *reinterpret_cast<void **>(retp) = p;
    return (p != nullptr || count == 0 || size == 0) ? 0 : ENOMEM;
}

void
reset_os_alloc_fakes()
{
    RESET_FAKE(__wt_calloc);
    __wt_calloc_fake.custom_fake = calloc_passthrough;

    FFF_RESET_HISTORY();
}
