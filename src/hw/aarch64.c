/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#include <stdint.h>
#include <math.h>

/*
 * __aarch64_nsec_per_tick --
 *     Return the nsec/tick calculated using the cntfrq_el0 register.
 */
static double
__aarch64_nsec_per_tick(void)
{
    static const double NSEC_PER_SEC = 1.e9;
    uint64_t freq;

    __asm__ volatile("\tmrs\t%0, cntfrq_el0\n" : "=r"(freq));

    /*
     * The Armv8-A documentation warns that on a WARM reset the register is set to "an
     * architecturally UNKNOWN value". We assume the OS will take care of that scenario, but protect
     * against div 0 out of an abundance of caution.
     */
    if (freq == 0)
        return (0.0);

    /* The reported frequency is in Hz. */
    return (freq / NSEC_PER_SEC);
}

void __wti_hw_nsec_per_tick(void) __attribute__((weak, alias("__aarch64_nsec_per_tick")));
