#pragma once
/*
 * Minimal portable direct Futex operations.
 */

/*
 * Linux limits Futex words to 32 bits.
 */
typedef uint32_t WT_FUTEX_WORD;

/*
 * Quantity of waiting threads to wake.
 */
typedef enum __wt_futex_wake {
    WT_WAKE_ONE, /* Wake a single waiting thread. */
    WT_WAKE_ALL  /* Wake all waiting threads. */
} WT_WAKE;

/*
 * -- __wt_futex_op_wake
 *
 */
int __wt_futex_op_wake(WT_FUTEX_WORD *futex_word, enum __wt_futex_wake whom);

/*
 * -- __wt_futex_op_wait
 *
 * The timeout is in microseconds and MUST be greater than zero.
 */
int __wt_futex_op_wait(WT_FUTEX_WORD *futex_word, uint32_t expected, int64_t timeout_us);
