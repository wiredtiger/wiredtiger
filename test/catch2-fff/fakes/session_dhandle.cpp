#include "session_dhandle.h"

extern "C" {
DEFINE_FAKE_VALUE_FUNC(int, __wt_session_get_dhandle, WT_SESSION_IMPL *, const char *, const char *,
  const char **, uint32_t);

DEFINE_FAKE_VALUE_FUNC(int, __wt_session_release_dhandle, WT_SESSION_IMPL *);
}

void
reset_session_dhandle_fakes()
{
    RESET_FAKE(__wt_session_get_dhandle);
    RESET_FAKE(__wt_session_release_dhandle);
    FFF_RESET_HISTORY();
}
