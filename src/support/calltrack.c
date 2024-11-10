#include "wt_internal.h"

__thread WT_CALLTRACK wt_calltrack = {
    .nest_level = 0,
    .tid = 0,
    .pid = 0,
};

WT_CALLTRACK_GLOBAL wt_calltrack_global;

void __attribute__((constructor)) __wt_calltrack_init_once(void);
void __attribute__((constructor))
__wt_calltrack_init_once(void)
{
    __wt_epoch_raw(NULL, &wt_calltrack_global.start);
}


void __attribute__((constructor)) __wt_calltrack_init_tracefile(void);
void __attribute__((constructor))
__wt_calltrack_init_tracefile(void)
{
    printf("{\"displayTimeUnit\": \"us\", \"traceEvents\": [\n");
}

void __attribute__((destructor)) __wt_calltrack_deinit_tracefile(void);
void __attribute__((destructor))
__wt_calltrack_deinit_tracefile(void)
{
    /* printf("]\n}\n"); */
    printf("{}]}\n");
}
