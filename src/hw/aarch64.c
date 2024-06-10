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
static uint64_t
__aarch64_proc_freq_hz(void)
{
    uint64_t freq;
    /*
     * The Armv8-A documentation warns that on a WARM reset the register is set to "an
     * architecturally UNKNOWN value". We assume the OS will take care of that scenario, but protect
     * against div 0 out of an abundance of caution.
     */
    __asm__ volatile("\tmrs\t%0, cntfrq_el0\n" : "=r"(freq));
    return (freq);
}

uint64_t __wti_hw_proc_freq_hz(void) __attribute__((weak, alias("__aarch64_proc_freq_hz")));
