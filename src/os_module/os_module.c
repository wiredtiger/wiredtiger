/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "os_module.h"
#include <stdio.h>

/*
 * test_mod --
 *     A module test function
 */
int
test_mod(void)
{
    printf("Testing.. \n");
    return (0);
}
