## WT_LOGSLOT

Cannot move `WT_LOGSLOT` into `src/log` as it is visible thru `WT_LOG`. Possible if `WT_LOG`
is made an opaque handle.

## WT_LOG_RECORD

Internals are visible through macros `WT_LOG_SKIP_HEADER` and `WT_LOG_REC_SIZE` used in 
`txn_log.c` look to convert to functions to allow migration into 