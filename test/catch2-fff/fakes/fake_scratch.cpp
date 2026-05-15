#include "fake_scratch.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>

extern "C" {
DEFINE_FAKE_VALUE_FUNC(int, __wt_buf_set, WT_SESSION_IMPL *, WT_ITEM *, const void *, size_t);
DEFINE_FAKE_VOID_FUNC(__wt_buf_free, WT_SESSION_IMPL *, WT_ITEM *);
}

static int
buf_set_passthrough(WT_SESSION_IMPL *, WT_ITEM *buf, const void *data, size_t size)
{
    if (buf->memsize < size) {
        void *p = std::realloc(buf->mem, size);

        if (p == nullptr && size > 0)
            return ENOMEM;

        buf->mem = p;
        buf->memsize = size;
    }

    if (size > 0)
        std::memcpy(buf->mem, data, size);

    buf->data = buf->mem;
    buf->size = size;
    return 0;
}

static void
buf_free_passthrough(WT_SESSION_IMPL *, WT_ITEM *buf)
{
    std::free(buf->mem);
    std::memset(buf, 0, sizeof(*buf));
}

void
reset_scratch_fakes()
{
    RESET_FAKE(__wt_buf_set);
    RESET_FAKE(__wt_buf_free);
    __wt_buf_set_fake.custom_fake = buf_set_passthrough;
    __wt_buf_free_fake.custom_fake = buf_free_passthrough;

    FFF_RESET_HISTORY();
}
