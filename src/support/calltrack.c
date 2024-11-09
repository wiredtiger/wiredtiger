#include "wt_internal.h"

__thread WT_CALLTRACK wt_calltrack = {
    .nest_level = 0,
};
