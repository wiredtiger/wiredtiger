#include "wt_internal.h"

#define PRIVATE 1
#include <ulock.h>
#undef PRIVATE

/*
 * __wt_futex_wait --
 *     Wait on the futex. The timeout is in microseconds and MUST be greater than zero.
 */
int
__wt_futex_wait(
  WT_FUTEX_WORD *futexp, WT_FUTEX_WORD expected, time_t usec, WT_FUTEX_WORD *wake_valp)
{
    WT_DECL_RET;

    ret = __ulock_wait(
      UL_COMPARE_AND_WAIT | ULF_WAIT_WORKQ_DATA_CONTENTION, futexp, expected, (uint32_t)usec);
    if (ret >= 0)
        *wake_valp = __atomic_load_n(futexp, __ATOMIC_ACQUIRE);
    return ((ret > 0) ? 0 : ret);
}

/*
 * __wt_futex_wake --
 *     Wake the futex.
 */
int
__wt_futex_wake(WT_FUTEX_WORD *futexp, WT_FUTEX_WAKE wake, WT_FUTEX_WORD wake_val)
{
    WT_DECL_RET;
    uint32_t op;

    WT_ASSERT(NULL, wake == WT_FUTEX_WAKE_ONE || wake == WT_FUTEX_WAKE_ALL);
    op = UL_COMPARE_AND_WAIT | ((wake == WT_FUTEX_WAKE_ALL) ? ULF_WAKE_ALL : 0);
    ret = __ulock_wake(op, futexp, wake_val);

    return ((ret > 0) ? 0 : ret);
}
