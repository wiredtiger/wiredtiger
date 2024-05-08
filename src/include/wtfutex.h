#pragma once


/*
 * Wrap the futex (combination of a 32-bit value and its address) in a
 * structure to signify the value should not be modified directly.
 */
struct __wt_futex {
  _Alignas(4) uint32_t 
};


/*
 * Restricted for portability.
 */
enum __wt_futex_wake {
  WT_WAKE_ONE,			/* Wake a single waiting thread. */
  WT_WAKE_ALL			/* Wake all waiting threads. */
};


/*
 * Get the value associated with the futex.
 *
 * \retval -EINVAL Not a futex, or pointer to [val] is null.
 */
int __wt_futex_fetch(struct __wt_futex *ftx, uint32_t *val);


/*
 * Store to the value associated with the futex, WITHOUT intentionally
 * waking any threads waiting on the futex.
 *
 * NOTE: If there ARE any threads waiting on the corresponding futex,
 * the use of this function may result in an inadvertent wakeup.
 *
 * Example:
 * ---------------------------
 *  T1, T2 are threads.
 *  X != Y
 *
 *  Interleaved progression.
 *
 *  T1 : __wt_wait(F, X, ALONGTIME);
 *  T2 : __wt_store(F, Y);
 *  T1 : __wt_wait(F, X, ALONGTIME) == 0 "spurious wakeup"
 *
 * Thread T1 will see this as an intentional wakeup.
 *
 * \retval -EINVAL Not a futex.
 */
int __wt_futex_store(struct __wt_futex *ftx, uint32_t val);


/*
 * Wait on a futex.
 *
 * \param expected Expected current value of futex.
 *
 * \retval -EINVAL Not a futex, or [timeout_us] <= 0.
 * \retval -ETIMEDOUT Timeout expired before being awoken.
 */
int __wt_futex_wait(struct __wt_futex *ftx, uint32_t expected, int64_t timeout_us);


/*
 * Set the value associated with the futex and wake the specified
 * number of waiting threads.
 *
 * \retval -EINVAL Not a futex, or invalid [whom].
 */
int __wt_futex_wake(struct __wt_futex *ftx, uint32_t value, enum __wt_futex_wake whom);
